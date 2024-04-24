use eg::EgValue;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::osrf::sclient::HostSettings;
use eg::Editor;
use eg::EgResult;
use eg::auth;
use evergreen as eg;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "session.create",
        desc: "Create an Authentication Session",
        param_count: ParamCount::Range(2, 4),
        handler: create_auth_session,
        params: &[
            StaticParam {
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                name: "Login Type",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                name: "Workstation",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "Context Org Unit",
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

    let user_id = method.param(0).int()?;
    let login_type = auth::AuthLoginType::try_from(method.param(1).str()?)?;

    let mut editor = Editor::new(worker.client());

    let mut user = editor.retrieve("au", user_id)?
        .ok_or_else(|| editor.die_event())?;

    if let Some(workstation) = method.params().get(2).map(|v| v.as_str()) {
        let mut ws = editor.search("aws", eg::hash! {"name": workstation})?
            .pop()
            .ok_or_else(|| editor.die_event())?;

        user["wsid"] = ws["id"].take();
        user["ws_ou"] = ws["owning_lib"].take();

    } else {
        user["ws_ou"] = user["home_ou"].clone();
    }

    let org_id = match method.params().get(3) {
        Some(id) => id.int()?,
        None => user["ws_ou"].int()?,
    };

    Ok(())
}

fn get_login_timeout(
    host_settings: &HostSettings,
    user: &EgValue,
    login_type: &auth::AuthLoginType,
    org_id: i64
) -> EgResult<i64> {

    // TODO

    Ok(0)
}



