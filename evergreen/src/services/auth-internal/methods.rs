use eg::common::auth;
use eg::date;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgEvent;
use eg::EgResult;
use evergreen as eg;

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
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsAuthInternalWorker::downcast(worker)?;
    let options = method.param(0);

    let user_id = options["user_id"].int()?;
    let login_type = auth::LoginType::try_from(options["login_type"].str()?)?;

    let mut editor = Editor::new(worker.client());

    let args = auth::InternalLoginArgs {
        user_id,
        login_type,
        org_unit: options["org_id"].as_int(),
        workstation: options["workstation"].as_str().map(|v| v.to_string()),
    };

    let auth_ses = auth::Session::internal_session(&mut editor, &args)?;

    session.respond(eg::hash! {"authtime": auth_ses.authtime(), "authtoken": auth_ses.token()})
}

pub fn validate_user(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsAuthInternalWorker::downcast(worker)?;
    let options = method.param(0);

    let user_id = options["user_id"].int()?;
    let login_type = auth::LoginType::try_from(options["login_type"].str()?)?;

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
        auth::LoginType::Opac => "OPAC_LOGIN",
        auth::LoginType::Staff | auth::LoginType::Temp => "STAFF_LOGIN",
        auth::LoginType::Persist => "PERSISTENT_LOGIN",
    };

    // For backwards compat, login permission checks are always global.
    if !editor.allowed(permission)? {
        return session.respond(editor.event());
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
