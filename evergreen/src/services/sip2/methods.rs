use eg::common::sip2::session::Session;
use eg::common::user;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use sip2::Message;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[StaticMethodDef {
    name: "request",
    desc: "Dispatch a SIP Request",
    param_count: ParamCount::Exactly(2),
    handler: dispatch_sip_request,
    params: &[
        StaticParam {
            name: "Session Key",
            datatype: ParamDataType::String,
            desc: "SIP2 Client Session Key",
        },
        StaticParam {
            name: "Message",
            datatype: ParamDataType::Object,
            desc: "SIP2 Message JSON Value",
        },
    ],
}];

pub fn dispatch_sip_request(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    message::set_thread_ingress("sip2");

    let worker = app::Sip2Worker::downcast(worker)?;

    let seskey = method.param(0).str()?;

    let sip_msg = Message::from_json_value(&method.param(1).clone().into_json_value())
        .map_err(|e| format!("Error parsing SIP message: {e}"))?;

    let mut editor = Editor::new(worker.client());

    let response = if sip_msg.spec().code == "93" {
        handle_login(&mut editor, seskey, sip_msg)?
    } else {
        todo!()
    };

    let value = EgValue::from_json_value(response.to_json_value())?;

    session.respond_complete(value)
}

fn handle_login(editor: &mut Editor, seskey: &str, sip_msg: Message) -> EgResult<Message> {
    // Start with a login-failed response.
    let mut response = Message::from_ff_values("94", &["0"]).unwrap();

    let sip_username = sip_msg
        .get_field_value("CN")
        .ok_or_else(|| format!("'CN' field required"))?;

    let sip_password = sip_msg
        .get_field_value("CO")
        .ok_or_else(|| format!("'CO' field required"))?;

    let flesh = eg::hash! {
        "flesh": 1,
        "flesh_fields": {
            "sipacc": ["workstation"]
        }
    };

    let query = eg::hash! {
        "sip_username": sip_username,
        "enabled": "t",
    };

    let sip_account = match editor.search_with_ops("sipacc", query, flesh)?.pop() {
        Some(a) => a,
        None => {
            log::warn!("No SIP account for {sip_username}");
            return Ok(response);
        }
    };

    if user::verify_password(editor, sip_account["usr"].int()?, sip_password, "sip2")? {
        let mut session = Session::new(editor, seskey, sip_account);
        session.refresh_auth_token()?;
        session.to_cache()?;

        // Set the login succeeded value.
        response.fixed_fields_mut()[0].set_value("1").unwrap();
    } else {
        log::info!("SIP2 login failed for user={sip_username}");
    }

    Ok(response)
}
