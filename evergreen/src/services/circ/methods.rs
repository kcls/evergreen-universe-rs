use eg::common::circ;
use eg::common::circulator::Circulator;
use eg::editor::Editor;
use eg::util;
use evergreen as eg;
use json;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use opensrf::session::ServerSession;
use std::collections::HashMap;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "checkin",
        desc: "Checkin a copy",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.", // TODO expand
            },
        ],
    },
    StaticMethodDef {
        name: "checkin.override",
        desc: "Checkin a copy / Override edition. See checkin",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.", // TODO expand
            },
        ],
    },
    StaticMethodDef {
        name: "checkout",
        desc: "Checkout a copy",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.",
            },
        ],
    },
    StaticMethodDef {
        name: "checkout.override",
        desc: "Checkout a copy / Override edition",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.",
            },
        ],
    },
    StaticMethodDef {
        name: "checkout.inspect",
        desc: "Inspect checkout policy",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.",
            },
        ],
    },
    StaticMethodDef {
        name: "renew",
        desc: "Renew a copy",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.",
            },
        ],
    },
    StaticMethodDef {
        name: "renew.override",
        desc: "Renew a copy / Override edition",
        param_count: ParamCount::Exactly(2),
        handler: checkout_renew_checkin,
        params: &[
            StaticParam {
                name: "authtoken",
                datatype: ParamDataType::String,
                desc: "Authentication Token",
            },
            StaticParam {
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Options including copy_barcode, etc.",
            },
        ],
    },
    StaticMethodDef {
        name: "renewal_chain.retrieve_by_circ.summary",
        desc: "Circulation Renewal Chain Summary",
        param_count: ParamCount::Exactly(2),
        handler: renewal_chain_summary,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Circ ID",
                datatype: ParamDataType::Number,
                desc: "Circulation ID to lookup",
            },
        ],
    },
    StaticMethodDef {
        name: "prev_renewal_chain.retrieve_by_circ.summary",
        desc: "Previous Circulation Renewal Chain Summary",
        param_count: ParamCount::Exactly(2),
        handler: prev_renewal_chain_summary,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Circ ID",
                datatype: ParamDataType::Number,
                desc: "Circulation ID to lookup",
            },
        ],
    },
];

pub fn checkout_renew_checkin(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
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

    let mut circulator = Circulator::new(editor, options)?;
    circulator.is_inspect = method.method().contains(".inspect");
    circulator.is_override = method.method().contains(".override");
    circulator.begin()?;

    let result = if method.method().contains("checkout") {
        circulator.checkout()
    } else if method.method().contains("checkin") {
        circulator.checkin()
    } else if method.method().contains("renew") {
        circulator.renew()
    } else {
        return Err(format!("Unhandled method {}", method.method()));
    };

    if let Err(err) = result {
        circulator.rollback()?;
        // Return the error event to the caller.
        session.respond(&err.event_or_default())?;
        return Ok(());
    }

    if circulator.is_inspect() {
        session.respond(circulator.policy_to_json_value())?;
        circulator.rollback()?;
        return Ok(());
    }

    // Checkin call completed
    circulator.commit()?;

    let events: Vec<json::JsonValue> = circulator.events().iter().map(|e| e.into()).collect();

    // Send the compiled events to the caller and let them know we're done.
    session.respond_complete(events)?;

    // Work that the caller does not care about.
    circulator.post_commit_tasks()?;

    Ok(())
}

pub fn renewal_chain_summary(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> Result<(), String> {
    let worker = app::RsCircWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;
    let circ_id = util::json_int(method.param(1))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    if !editor.allowed("VIEW_CIRCULATIONS")? {
        return session.respond(editor.event());
    }

    let chain = circ::summarize_circ_chain(&mut editor, circ_id)?;

    session.respond(chain)
}

pub fn prev_renewal_chain_summary(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> Result<(), String> {
    let worker = app::RsCircWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;
    let circ_id = util::json_int(method.param(1))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    if !editor.allowed("VIEW_CIRCULATIONS")? {
        return session.respond(editor.event());
    }

    let chain = circ::circ_chain(&mut editor, circ_id)?;
    let first_circ = &chain[0]; // circ_chain errors on not-found

    // The previous circ chain contains the circ that occurred most recently
    // before the first circ in the latest circ chain.

    let query = json::object! {
        target_copy: util::json_int(&first_circ["target_copy"])?,
        xact_start: {"<": first_circ["xact_start"].as_str().unwrap()}, // xact_tart required
    };

    let flesh = json::object! {
        flesh: 1,
        flesh_fields: {
            aacs: [
                "active_circ",
                "aged_circ"
            ]
        },
        order_by: {aacs: "xact_start desc"},
        limit: 1
    };

    let prev_circ = editor.search_with_ops("aacs", query, flesh)?;

    if prev_circ.len() == 0 {
        // No previous circ chain
        return Ok(());
    }

    session.respond(circ::summarize_circ_chain(
        &mut editor,
        util::json_int(&prev_circ[0]["id"])?,
    )?)
}
