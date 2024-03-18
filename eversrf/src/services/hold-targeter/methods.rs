use eg::common::targeter;
use eg::editor::Editor;
use eg::util;
use eversrf as eg;
use eg::app::ApplicationWorker;
use eg::message;
use eg::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::session::ServerSession;

// Import our local app module
use eg::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[StaticMethodDef {
    name: "target",
    desc: "Target one or more holds",
    param_count: ParamCount::Range(0, 1),
    handler: target,
    params: &[StaticParam {
        name: "options",
        datatype: ParamDataType::Object,
        desc: "Targeting Options",
    }],
}];

pub fn target(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> Result<(), String> {
    let worker = app::HoldTargeterWorker::downcast(worker)?;

    let mut editor = Editor::new(worker.client(), worker.env().idl());
    let mut tgtr = targeter::HoldTargeter::new(&mut editor);

    let mut return_throttle = 1;
    let mut return_count = false;
    let mut find_copy = None;

    // Apply user-supplied options if we have any.
    if let Some(options) = method.params().get(0) {
        return_count = util::json_bool(&options["return_count"]);

        if let Ok(t) = util::json_int(&options["return_throttle"]) {
            return_throttle = t;
        }
        if let Ok(c) = util::json_int(&options["find_copy"]) {
            find_copy = Some(c);
        }
        if let Ok(c) = util::json_int(&options["parallel_count"]) {
            tgtr.set_parallel_count(c as u8);
        }
        if let Ok(c) = util::json_int(&options["parallel_slot"]) {
            tgtr.set_parallel_slot(c as u8);
        }
        if let Some(s) = options["retarget_interval"].as_str() {
            tgtr.set_retarget_interval(s);
        }
        if let Some(s) = options["soft_retarget_interval"].as_str() {
            tgtr.set_soft_retarget_interval(s);
        }
        if let Some(s) = options["next_check_interval"].as_str() {
            tgtr.set_next_check_interval(s);
        }
    }

    tgtr.init()?;

    let list = tgtr.find_holds_to_target()?;

    let total = list.len();
    for (idx, id) in list.into_iter().enumerate() {
        let ctx = match tgtr.target_hold(id, find_copy) {
            Ok(c) => c,
            Err(e) => {
                session.respond(format!("Error targeting hold {id}: {e}"))?;
                continue;
            }
        };

        if idx as i64 % return_throttle == 0 {
            if return_count {
                session.respond(idx)?;
            } else {
                session.respond(ctx.to_json())?;
            }
        }

        log::info!("Targeted {idx} of {total} holds");
    }

    Ok(())
}
