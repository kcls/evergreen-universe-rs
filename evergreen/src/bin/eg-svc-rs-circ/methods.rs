use eg::idldb::{IdlClassSearch, Translator};
use eg::editor::Editor;
use eg::common::circulator::Circulator;
use eg::util;
use evergreen as eg;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::session::ServerSession;
use std::sync::Arc;
use std::collections::HashMap;
use json;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[
    StaticMethod {
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
            }
        ],
    },
];


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

    editor.xact_begin()?;
    let mut circulator = Circulator::new(editor, options)?;

    let result = circulator.checkin();

    // We're done checking in, recover our Editor.
    let mut editor = circulator.take_editor();

    if let Err(e) = result {
        log::error!("Checkin failed: {e}");
        editor.xact_rollback()?;
    } else {

        // TODO Ask the circulator to collect a pile of return
        // data and return it here.

        editor.xact_commit()?;
    }

    Ok(())
}
