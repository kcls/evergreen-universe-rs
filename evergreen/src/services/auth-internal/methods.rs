use eg::EgValue;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::osrf::sclient::HostSettings;
use eg::Editor;
use eg::EgResult;
use eg::auth;
use eg::util;
use evergreen as eg;

// Default time for extending a persistent session: ten minutes
const DEFAULT_RESET_INTERVAL: i32 = 10 * 60;
const OILS_AUTH_CACHE_PRFX: &str = "oils_auth_";

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "session.create",
        desc: "Create an Authentication Session",
        param_count: ParamCount::Exactly(1),
        handler: create_auth_session,
        params: &[
            StaticParam {
                name: "Options",
                datatype: ParamDataType::Object,
                desc: "Hash of Login Options and Values",
            },
        ],
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

    let mut user = editor.retrieve("au", user_id)?
        .ok_or_else(|| editor.die_event())?;

    // No long really an issue, but good to clear.
    user["passwd"].take();

    if let Some(workstation) = options["workstation"].as_str() {
        let mut ws = editor.search("aws", eg::hash! {"name": workstation})?
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

    let duration = eg::common::auth::get_auth_duration(
        &mut editor,
        org_id,
        user["home_ou"].int()?,
        worker.host_settings(),
        &login_type,
    )?;

    let authtoken = util::random_number(32); // TODO use something better
    let cache_prefix = format!("{}{}", OILS_AUTH_CACHE_PRFX, authtoken);

    let cache_val = eg::hash! {
        "authtime": duration,
        "userobj": user,
    };

    if login_type == auth::AuthLoginType::Persist {
        todo!();
    }

    session.respond(eg::hash! {"authtime": duration, "authtoken": authtoken})
}


