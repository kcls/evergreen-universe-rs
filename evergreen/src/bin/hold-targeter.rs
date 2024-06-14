use eg::init::InitOptions;
use eg::osrf::session::MultiSession;
use eg::result::EgResult;
use eg::util;
use eg::EgValue;
use evergreen as eg;
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

    --return-throttle
        Report ongoing status after processing this many holds.
"#;

fn main() -> EgResult<()> {
    let mut options = getopts::Options::new();

    options.optflag("", "help", "Show this message");
    options.optopt("", "lockfile", "", "");
    options.optopt("", "parallel-count", "", "");
    options.optopt("", "parallel-init-sleep", "", "");
    options.optopt("", "soft-retarget-interval", "", "");
    options.optopt("", "next-check-interval", "", "");
    options.optopt("", "retarget-interval", "", "");
    options.optopt("", "return-throttle", "", "");

    let args: Vec<String> = std::env::args().collect();

    let params = options
        .parse(&args[1..]).map_err(|e| format!("Error parsing params: {e}"))?;

    if params.opt_present("help") {
        println!("{HELP_TEXT}");
        return Ok(());
    }

    if let Some(path) = params.opt_str("lockfile") {
        if util::lockfile(&path, "check")? {
            return Err(format!("Remove lockfile first: {}", path).into());
        }
        util::lockfile(&path, "create")?;
    }

    let mut target_options = eg::hash! {
        "return_count": true, // summary counts only
    };

    for key in &[
        "parallel-count",
        "retarget-interval",
        "soft-retarget-interval",
        "next-check-interval",
        "return-throttle", // is number, but OK for json
    ] {
        if let Some(val) = params.opt_str(key) {
            target_options[&key.replace('-', "_")] = EgValue::from(val);
        }
    }

    let parallel = target_options["parallel_count"].as_int().unwrap_or(1);

    let mut sleep = 0;
    if let Some(v) = params.opt_str("parallel-init-sleep") {
        sleep = v.parse::<u64>().unwrap_or(0);
    }

    let mut init_ops = InitOptions::new();
    init_ops.skip_host_settings = true; // we don't need it.

    let client = eg::init::with_options(&init_ops)?;

    let mut multi_ses = MultiSession::new(client.clone(), "open-ils.rs-hold-targeter");

    // 'slot' is 1-based at the API level.
    for slot in 1..(parallel + 1) {
        let mut target_options = target_options.clone();
        target_options["parallel_slot"] = EgValue::from(slot);

        multi_ses.request("open-ils.rs-hold-targeter.target", target_options)?;

        if sleep > 0 {
            thread::sleep(std::time::Duration::from_secs(sleep));
        }
    }

    loop {
        if multi_ses.complete() {
            break;
        }

        if let Some((thread, value)) = multi_ses.recv(60)? {
            println!("Thread {} has a value {}", thread, value);
        }
    }

    if let Some(path) = params.opt_str("lockfile") {
        util::lockfile(&path, "delete")?;
    }

    Ok(())
}
