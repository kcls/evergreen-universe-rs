use eg::common::sip2;
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
    let worker = app::Sip2Worker::downcast(worker)?;

    let seskey = method.param(0).str()?;
    let message = method.param(1); // object
    let msg_code = message["code"].str()?;

    let mut editor = Editor::new(worker.client());


    Ok(())
}
