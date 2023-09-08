use eg::editor::Editor;
use eg::common::targeter;
use eg::util;
use evergreen as eg;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::session::ServerSession;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[
    StaticMethod {
        name: "target",
        desc: "Target one or more holds",
        param_count: ParamCount::Range(0, 1),
        handler: target,
        params: &[
            StaticParam {
                required: false,
                name: "options",
                datatype: ParamDataType::Object,
                desc: "Targeting Options",
            },
        ],
    },
];

pub fn target(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsHoldTargeterWorker::downcast(worker)?;

    let editor = Editor::new(worker.client(), worker.env().idl());
    let mut tgtr = targeter::HoldTargeter::new(editor);
    let mut return_throttle = 1;
    let mut return_count = false;
    let mut find_copy = None;

    if let Some(options) = method.params().get(0) {

        return_count = util::json_bool(&options["return_count"]);

        if let Ok(t) = util::json_int(&options["return_throttle"]) {
            return_throttle = t;
        }

        if let Ok(c) = util::json_int(&options["find_copy"]) {
            find_copy = Some(c);
        }


        // TODO
    }

    tgtr.init()?;

    let mut list = tgtr.find_holds_to_target()?;

    let total = list.len();
    let mut counter = 0;

    for id in list.drain(..) {
        counter += 1;

        let ctx = match tgtr.target_hold(id, find_copy) {
            Ok(c) => c,
            Err(e) => {
                session.respond(format!("Error targeting hold {id}: {e}"))?;
                continue;
            }
        };

        if counter % return_throttle == 0 {
            if return_count {
                session.respond(counter)?;
            } else {
                // TODO reply with result object of some sort
                // session.respond(counter);
                session.respond(counter)?;
            }
        }

        log::info!("Targeted {counter} of {total} holds");
    }

    Ok(())
}
