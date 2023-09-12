use eg::init::InitOptions;
use eg::util;
use eg::result::EgResult;
use evergreen as eg;
use getopts;
use std::thread;

const HELP_TEXT: &str = r#"
Batch hold targeter.

./eg-hold-targeter --parallel-count 2 --lockfile /tmp/hold_targeter-LOCK

General Options
    --lockfile [/tmp/hold_targeter-LOCK]
        Full path to lock file

    Standard OpenSRF environment variables (e.g. OSRF_CONFIG) are
    also supported.

Targeting Options

    --parallel-count <parallel-process-count>
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

fn main() -> EgResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut options = getopts::Options::new();

    options.optflag("", "help", "Show this message");
    options.optopt("", "lockfile", "", "");
    options.optopt("", "parallel-count", "", "");
    options.optopt("", "parallel-init-sleep", "", "");
    options.optopt("", "soft-retarget-interval", "", "");
    options.optopt("", "next-check-interval", "", "");
    options.optopt("", "retarget-interval", "", "");

    let params = match options.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => return Err(
            format!("Cannot parse command line params: {e}").into()),
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
        "parallel-count",
        "retarget-interval",
        "soft-retarget-interval",
        "next-check-interval",
    ] {
        if let Some(val) = params.opt_str(key) {
            target_options[key.replace("-", "_")] = json::from(val);
        }
    }

    let parallel = util::json_int(&target_options["parallel_count"]).unwrap_or(1);
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

    let mut requests = Vec::new();

    // 'slot' is 1-based at the API level.
    for slot in 1..(parallel + 1) {

        //println!("parallel {parallel} slot {slot}");

        let mut target_options = target_options.clone();
        target_options["parallel_slot"] = json::from(slot);

        let mut ses = context.client().session("open-ils.rs-hold-targeter");
        let req = ses.request("open-ils.rs-hold-targeter.target", target_options)?;

        requests.push(req);

        if sleep > 0 {
            thread::sleep(std::time::Duration::from_secs(sleep as u64));
        }
    }

    loop {
        //thread::sleep(std::time::Duration::from_secs(1)); // XXX
        //println!("looping with {} requests", requests.len()); // XXX

        if context.client().wait(60)? {
            for req in requests.iter_mut() {
                if let Some(resp) = req.recv_with_timeout(0)? {
                    println!("ses {} has a value {}", req.thread(), resp); // XXX
                    log::info!("Targeter responded with {resp}");
                }
            }
        }

        loop {
            // Clean up completed requests
            let mut rem_thread_trace = None;

            for req in requests.iter() {
                if req.complete() {
                    rem_thread_trace = Some(req.thread_trace());
                    break;
                }
            }

            let tt = match rem_thread_trace {
                Some(t) => t,
                None => break,
            };

            let pos = match requests.iter().position(|r| r.thread_trace() == tt) {
                Some(p) => p,
                None => continue,
            };

            requests.remove(pos);
        }

        if requests.len() == 0 {
            break;
        }
    }

    if let Some(path) = params.opt_str("lockfile") {
        util::lockfile(&path, "delete")?;
    }

    Ok(())
}
