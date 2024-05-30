use sip2::Message;
use sip2::spec;
use eg::EgValue;
use eg::common::sip2::session::Session;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use evergreen as eg;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
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
    },
];

pub fn dispatch_sip_request(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    message::set_thread_ingress("sip2");

    let worker = app::Sip2Worker::downcast(worker)?;

    let seskey = method.param(0).str()?;

    let mut sip_msg = Message::from_json_value(&method.param(1).clone().into_json_value())
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
    let mut response = Message::from_ff_values("94", &["0"]).unwrap();

    // TODO

    Ok(response)
}
