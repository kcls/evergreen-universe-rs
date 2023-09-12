use eg::init::InitOptions;
use eg::util;
use eg::idl;
use eg::result::EgResult;
use evergreen as eg;
use opensrf as osrf;
use getopts;
use json::JsonValue;
use std::thread;
use std::sync::Arc;

const HELP_TEXT: &str = r#"
Batch hold targeter.

./eg-hold-targeter --parallel 2 --lockfile /tmp/hold_targeter-LOCK

General Options
    --lockfile [/tmp/hold_targeter-LOCK]
        Full path to lock file

    Standard OpenSRF environment variables (e.g. OSRF_CONFIG) are
    also supported.

Targeting Options

    --parallel <parallel-process-count>
        Number of parallel hold processors to run.  This overrides any
        value found in opensrf.xml

    --parallel-init-sleep <seconds=0>
        Number of seconds to wait before starting each subsequent
        parallel targeter instance.  This gives each targeter backend
        time to run the large targetable holds query before the next
        kicks off, so they don't all hit the database at once.

        Defaults to no sleep.

    --soft-retarget-interval
        Holds whose previous check time sits between the
        --soft-retarget-interval and the --retarget-interval are
        treated like this:

        1. The list of potential copies is updated for all matching holds.
        2. Holds that have a viable target are otherwise left untouched,
           including their prev_check_time.
        3. Holds with no viable target are fully retargeted.

    --next-check-interval
        Specify how long after the current run time the targeter will
        retarget the currently affected holds.  Applying a specific
        interval is useful when the retarget_interval is shorter than
        the time between targeter runs.

        This value is used to determine if an org unit will be closed
        during the next iteration of the targeter.  It overrides the
        default behavior of calculating the next retarget time from the
        retarget-interval.

    --retarget-interval
        Retarget holds whose previous check time occured before the
        requested interval.
        Overrides the 'circ.holds.retarget_interval' global_flag value.


"#;

fn process_batch(
    config: Arc<osrf::conf::Config>,
    idl: Arc<idl::Parser>,
    mut params: JsonValue,
    slot: i64
) -> EgResult<()> {
    log::info!("Targeter starting slot {slot}");

    let context = eg::init::init_from_parts(config, idl, None)?;

    params["parallel_slot"] = json::from(slot);

    let mut ses = context.client().session("open-ils.rs-hold-targeter");
    let mut req = ses.request("open-ils.rs-hold-targeter.target", params)?;

    while let Some(resp) = req.recv()? {
        log::debug!("Targeter responded with: {resp}");
    }

    log::info!("Targeter finished slot {slot}");

    Ok(())
}

fn main() -> EgResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut options = getopts::Options::new();

    options.optflag("", "help", "Show this message");
    options.optopt("", "lockfile", "", "");
    options.optopt("", "parallel", "", "");
    options.optopt("", "parallel-init-sleep", "", "");
    options.optopt("", "soft-retarget-interval", "", "");
    options.optopt("", "next-check-interval", "", "");
    options.optopt("", "retarget-interval", "", "");

    let params = match options.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => return Err(format!("Cannot parse command line params: {e}").into()),
    };

    if params.opt_present("help") {
        println!("{HELP_TEXT}");
        return Ok(());
    }

    if let Some(path) = params.opt_str("lockfile") {
        if util::lockfile(&path, "check")? {
            // This is a non-starter.
            return Err(format!("Remove lockfile first: {}", path).into());
        }
        util::lockfile(&path, "create")?;
    }

    let mut target_options = json::object! {
        "return_throttle": 20,  // TODO command line
        "return_count": true,   // instead of per-hold details
    };

    for key in &[
        "parallel",
        "retarget-interval",
        "soft-retarget-interval",
        "next-check-interval",
    ] {
        if let Some(val) = params.opt_str(key) {
            target_options[key.replace("-", "_")] = json::from(val);
        }
    }

    let parallel = util::json_int(&target_options["parallel"]).unwrap_or(1);
    let sleep = match params.opt_str("parallel-init-sleep") {
        Some(s) => match s.parse::<i64>() {
            Ok(v) => v,
            Err(e) => return Err(format!("Invalid init-sleep value: {} {}", s, e).into()),
        }
        None => 0,
    };

    let mut init_ops = InitOptions::new();
    init_ops.skip_host_settings = true; // we don't need it.

    let context = eg::init::init_with_options(&init_ops)?;

    let mut children = Vec::new();

    // 'slot' is 1-based at the API level.
    for slot in 1..parallel + 1 {

        // Clone some data we can pass to the thread.
        let ops = target_options.clone();
        let idl = context.idl().clone();
        let config = context.config().clone();

        let handle = thread::spawn(move || {
            if let Err(e) = process_batch(config, idl, ops, slot) {
                log::error!("Targeter thread exited with error: {e}");
            }
        });

        children.push(handle);

        if sleep > 0 {
            thread::sleep(std::time::Duration::from_secs(sleep as u64));
        }
    }

    for child in children {
        let _ = child.join();
    }

    if let Some(path) = params.opt_str("lockfile") {
        util::lockfile(&path, "delete")?;
    }

    Ok(())
}
