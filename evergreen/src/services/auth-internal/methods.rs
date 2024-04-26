use eg::constants as C;
use eg::date;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::common::auth;
use eg::util;
use eg::Editor;
use eg::EgEvent;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use md5;

// Default time for extending a persistent session: ten minutes
const DEFAULT_RESET_INTERVAL: i32 = 10 * 60;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "session.create",
        desc: "Create an Authentication Session",
        param_count: ParamCount::Exactly(1),
        handler: create_auth_session,
        params: &[StaticParam {
            name: "Options",
            datatype: ParamDataType::Object,
            desc: "Hash of Login Options and Values",
        }],
    },
    StaticMethodDef {
        name: "user.validate",
        desc: "Validate a User for Login",
        param_count: ParamCount::Exactly(1),
        handler: validate_user,
        params: &[StaticParam {
            name: "Options",
            datatype: ParamDataType::Object,
            desc: "Hash of Login Options and Values",
        }],
    },
];

pub fn create_auth_session(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsAuthInternalWorker::downcast(worker)?;
    let options = method.param(0);

    let user_id = options["user_id"].int()?;
    let login_type = auth::AuthLoginType::try_from(options["login_type"].str()?)?;

    let mut editor = Editor::new(worker.client());

    let mut user = editor
        .retrieve("au", user_id)?
        .ok_or_else(|| editor.die_event())?;

    // No long really an issue, but good to clear.
    user["passwd"].take();

    if let Some(workstation) = options["workstation"].as_str() {
        let mut ws = editor
            .search("aws", eg::hash! {"name": workstation})?
            .pop()
            .ok_or_else(|| editor.die_event())?;

        user["wsid"] = ws["id"].take();
        user["ws_ou"] = ws["owning_lib"].take();
    } else {
        user["ws_ou"] = user["home_ou"].clone();
    }

    let org_id = match options["org_id"].as_int() {
        Some(id) => id,
        None => user["ws_ou"].int()?,
    };

    let duration = auth::get_auth_duration(
        &mut editor,
        org_id,
        user["home_ou"].int()?,
        worker.host_settings(),
        &login_type,
    )?;

    let authtoken = format!("{:x}", md5::compute(util::random_number(64)));
    let cache_key = format!("{}{}", C::OILS_AUTH_CACHE_PRFX, authtoken);

    let mut cache_val = eg::hash! {
        "authtime": duration,
        "userobj": user,
    };

    if login_type == auth::AuthLoginType::Persist {
        // Add entries for endtime and reset_interval, so that we can
        // gracefully extend the session a bit if the user is active
        // toward the end of the duration originally specified.
        cache_val["endtime"] = EgValue::from(date::epoch_secs().floor() as i64 + duration);

        // Reset interval is hard-coded for now, but if we ever want to make it
        // configurable, this is the place to do it:
        cache_val["reset_interval"] = DEFAULT_RESET_INTERVAL.into();
    }

    worker
        .cache()
        .set_for(&cache_key, cache_val, duration as usize)?;

    session.respond(eg::hash! {"authtime": duration, "authtoken": authtoken})
}

pub fn validate_user(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsAuthInternalWorker::downcast(worker)?;
    let options = method.param(0);

    let user_id = options["user_id"].int()?;
    let login_type = auth::AuthLoginType::try_from(options["login_type"].str()?)?;

    let mut editor = Editor::new(worker.client());

    let user = match editor.retrieve("au", user_id)? {
        Some(u) => u,
        None => return session.respond(EgEvent::value("LOGIN_FAILED")),
    };

    if user["deleted"].boolish() || user["barred"].boolish() {
        return session.respond(EgEvent::value("LOGIN_FAILED"));
    }

    if !user["active"].boolish() {
        return session.respond(EgEvent::value("PATRON_INACTIVE"));
    }

    let exp_date = date::parse_datetime(user["expire_date"].str()?)?;

    // Set the patron as the requestor so we can leverage its
    // perm checking abilities.
    editor.give_requestor(user);

    if exp_date < date::now() && block_expired_staff(&mut editor)? {
        log::warn!(
            "Blocking login for expired staff acount: {}",
            editor.requestor().unwrap().dump()
        );
        return session.respond(EgEvent::value("LOGIN_FAILED"));
    }

    if let Some(barcode) = options["barcode"].as_str() {
        let card_op = editor.search("ac", eg::hash! {"barcode": barcode})?.pop();
        if let Some(card) = card_op {
            if !card["active"].boolish() {
                return session.respond(EgEvent::value("PATRON_CARD_INACTIVE"));
            }
        }
    }

    let permission = match login_type {
        auth::AuthLoginType::Opac => "OPAC_LOGIN",
        auth::AuthLoginType::Staff | auth::AuthLoginType::Temp => "STAFF_LOGIN",
        auth::AuthLoginType::Persist => "PERSISTENT_LOGIN",
    };

    // For backwards compat, login permission checks are always global.
    if !editor.allowed(&permission)? {
        return session.respond(EgValue::from(editor.event()));
    }

    session.respond(EgEvent::success_value())
}

/// Returns true if we block expired STAFF_LOGIN accounts and the
/// user in question -- the editor's requestor -- has STAFF_LOGIN
/// permissions.
fn block_expired_staff(editor: &mut Editor) -> EgResult<bool> {
    // If configured, we block logins by expired staff accounts, so
    // let's see if the account is one. We'll do so by seeing if the
    // account has the STAFF_LOGIN permission anywhere. We are _not_
    // checking the login_type, as blocking 'staff' and 'temp' logins
    // still leaves open the possibility of constructing an 'opac'-type
    // login that _also_ sets a workstation, which in turn could
    // be used to set an authtoken cookie that works in the staff
    // interface. This means, that unlike ordinary patrons, a staff
    // account that expires will not be able to log into the public
    // catalog... but then, staff members really ought to be using a
    // separate account when acting as a library patron anyway.

    let query = eg::hash! {"enabled": "t", "name": "auth.block_expired_staff_login"};

    match editor.search("cgf", query)?.first() {
        Some(_) => editor.allowed("STAFF_LOGIN"),
        None => Ok(false),
    }
}
