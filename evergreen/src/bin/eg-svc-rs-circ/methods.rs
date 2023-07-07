use eg::common::circulator::Circulator;
use eg::editor::Editor;
use eg::util;
use evergreen as eg;
use json;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::session::ServerSession;
use std::collections::HashMap;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[StaticMethod {
    name: "checkin",
    desc: "Checkin a copy",
    param_count: ParamCount::Exactly(2),
    handler: checkin,
    params: &[
        StaticParam {
            required: true,
            name: "authtoken",
            datatype: ParamDataType::String,
            desc: "Authentication Toaken",
        },
        StaticParam {
            required: true,
            name: "options",
            datatype: ParamDataType::Object,
            desc: "Optoins including copy_barcode, etc.", // TODO expand
        },
    ],
}];

pub fn checkin(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsCircWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;

    // Translate the JSON object into a hashmap our circulator can use.
    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    let op_params = method.param(1);
    for (k, v) in op_params.entries() {
        options.insert(k.to_string(), v.clone());
    }

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    // Auth check
    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    // Initial perm check
    if !editor.allowed("COPY_CHECKIN", None)? {
        return session.respond(editor.event());
    }

    // Circulator requires us to
    let mut circulator = Circulator::new(editor, options)?;
    circulator.begin()?;

    if let Err(e) = circulator.init() {
        circulator.rollback()?;
        return Err(format!("Checkin init failed: {e}"));
    }

    let result = circulator.checkin();

    if let Err(e) = result {
        circulator.rollback()?;
        return Err(e);
    }

    // TODO Ask the circulator to collect a pile of return
    // data, then commit, then return the collected data.

    circulator.commit()?;

    Ok(())
}
