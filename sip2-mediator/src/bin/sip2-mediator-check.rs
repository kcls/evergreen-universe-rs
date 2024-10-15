use std::time::Duration;

const HELP_TEXT: &str = r#"
    --sip-host <hostname>
    --sip-port <port>
    --sip-user <sip username>
    --sip-pass <sip password>
    --timeout <receive timeout in seconds>
"#;

fn main() {
    let mut ops = getopts::Options::new();

    ops.optflag("h", "help", "");
    ops.optopt("", "sip-host", "", "");
    ops.optopt("", "sip-port", "", "");
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
        // Not really an error, but also not a success.
        std::process::exit(exitcode::TEMPFAIL);
    }

    let host = params
        .opt_get_default("sip-host", "127.0.0.1".to_string())
        .unwrap();

    let port = params
        .opt_get_default("sip-port", "6001".to_string())
        .unwrap();

    let sip_user = params
        .opt_get_default("sip-user", "sip-user".to_string())
        .unwrap();

    let sip_pass = params
        .opt_get_default("sip-pass", "sip-pass".to_string())
        .unwrap();

    let sip_host = format!("{host}:{port}");

    let mut sipcon = match sip2::Connection::new(&sip_host) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    };

    let timeout = Some(Duration::from_secs(
        params
            .opt_get_default("timeout", 2u64)
            .expect("Timeout value should be sane"),
    ));

    if let Err(e) = sipcon.set_recv_timeout(timeout) {
        eprintln!("{e}");
        std::process::exit(exitcode::USAGE);
    }

    if let Err(e) = sipcon.set_send_timeout(timeout) {
        eprintln!("{e}");
        std::process::exit(exitcode::USAGE);
    }

    let req = sip2::Message::from_values(
        sip2::spec::M_LOGIN.code,
        &[
            "0", // UID algo
            "0", // PW algo
        ],
        &[
            ("CN", &sip_user), // SIP login username
            ("CO", &sip_pass), // SIP login password
        ],
    )
    .unwrap();

    let resp_op = match sipcon.sendrecv(&req) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    };

    let resp = match resp_op {
        Some(r) => r,
        None => {
            eprintln!("SIP request returned no response");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    };

    sipcon.disconnect().ok();

    if resp.fixed_fields().len() == 1 && resp.fixed_fields()[0].value() == "1" {
        std::process::exit(exitcode::OK);
    } else {
        std::process::exit(exitcode::NOUSER);
    }
}
