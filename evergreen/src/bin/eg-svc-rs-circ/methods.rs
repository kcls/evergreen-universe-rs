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
    if !editor.allowed("COPY_CHECKIN")? {
        return session.respond(editor.event());
    }

    let mut circulator = Circulator::new(editor, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    let result = circulator.init().and_then(|()| circulator.checkin());

    if let Err(err) = result {
        circulator.rollback()?;
        // Return the error event to the caller.
        session.respond(&err.event_or_default())?;
        return Ok(());
    }

    // Collect the response data.
    // Consistent with Perl, collect all of the response data before
    // committing the transaction.
    let events: Vec<json::JsonValue> = circulator.events().iter().map(|e| e.into()).collect();

    circulator.commit()?;

    // Send the compiled events to the caller and let them know we're done.
    session.respond_complete(events)?;

    // Work that the caller does not care about.
    circulator.post_commit_tasks()?;

    Ok(())
}
