const HELP_TEXT: &str = r#"
Utility for logging in to a SIP server to check its availability.
The script exits with code 0 on success; non-zero code on failure.

The original use case is as a check script executed by a proxy (e.g. HAProxy)
to determine if the service is up and running.

    --sip-host <hostname:port>
    --sip-user <sip username>
    --sip-pass <sip password>
    --timeout <send/recv timeout in seconds>
    --verbose
        Log errors/failures to stderr.  Otherwise, the script silently
        exits with a non-success exit code.
"#;

fn main() {
    let mut ops = getopts::Options::new();

    ops.optflag("h", "help", "");
    ops.optflag("", "verbose", "");

    ops.optopt("", "sip-host", "", "");
    ops.optopt("", "sip-user", "", "");
    ops.optopt("", "sip-pass", "", "");
    ops.optopt("", "timeout", "", "");

    let args: Vec<String> = std::env::args().collect();

    let params = match ops.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => panic!("Error parsing options: {}", e),
    };

    if params.opt_present("help") {
        println!("{}", HELP_TEXT);
        std::process::exit(exitcode::TEMPFAIL);
    }

    let verbose = params.opt_present("verbose");

    // Called for any non-success outcomes.
    let on_err = |err: String, force: bool| {
        if verbose || force {
            eprintln!("SIP check failed: {err}");
        }
        std::process::exit(exitcode::UNAVAILABLE);
    };

    let sip_host = params
        .opt_get_default("sip-host", "127.0.0.1:6001".to_string())
        .expect("--host should have a usable value");

    let sip_user = match params.opt_str("sip-user") {
        Some(v) => v,
        None => return on_err("--sip-user is required".to_string(), true),
    };

    let sip_pass = match params.opt_str("sip-pass") {
        Some(v) => v,
        None => return on_err("--sip-pass is required".to_string(), true),
    };

    let timeout = params
        .opt_get_default("timeout", 2u64)
        .expect("Timeout value should be sane");

    // Login message
    let req =
        sip2::Message::from_values("93", &["0", "0"], &[("CN", &sip_user), ("CO", &sip_pass)])
            .expect("Login message format should be known-good");

    // Connect
    let mut sipcon = match sip2::Connection::new(&sip_host) {
        Ok(c) => c,
        Err(e) => return on_err(format!("Connection faield; {e}"), false),
    };

    // Send the login
    if let Err(e) = sipcon.send_with_timeout(&req, timeout) {
        return on_err(format!("Send failed: {e}"), false);
    }

    // Wait for a response
    let resp_op = match sipcon.recv_with_timeout(timeout) {
        Ok(r) => r,
        Err(e) => return on_err(format!("Recv failed: {e}"), false),
    };

    // Make sure we got a response within the timeout.
    let resp = match resp_op {
        Some(r) => r,
        None => return on_err("SIP request returned no response".to_string(), false),
    };

    // This will happen regardless, but we may as well.
    sipcon.disconnect().ok();

    if resp.fixed_fields().len() == 1 && resp.fixed_fields()[0].value() == "1" {
        // Login OK.
        std::process::exit(exitcode::OK);
    } else {
        on_err("Heartbeat login failed".to_string(), false);
    }
}
