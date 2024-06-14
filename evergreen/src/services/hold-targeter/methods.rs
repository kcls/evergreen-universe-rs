use eg::common::targeter;
use eg::editor::Editor;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::EgResult;
use evergreen as eg;

// Import our local app module
use crate::app;

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
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::HoldTargeterWorker::downcast(worker)?;

    let mut editor = Editor::new(worker.client());
    let mut tgtr = targeter::HoldTargeter::new(&mut editor);

    let mut return_throttle = 1;
    let mut return_count = false;
    let mut find_copy = None;

    // Apply user-supplied options if we have any.
    if let Some(options) = method.params().first() {
        return_count = options["return_count"].boolish();

        if let Ok(t) = options["return_throttle"].int() {
            return_throttle = t;
        }
        if let Ok(c) = options["find_copy"].int() {
            find_copy = Some(c);
        }
        if let Ok(c) = options["parallel_count"].int() {
            tgtr.set_parallel_count(c as u8);
        }
        if let Ok(c) = options["parallel_slot"].int() {
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
