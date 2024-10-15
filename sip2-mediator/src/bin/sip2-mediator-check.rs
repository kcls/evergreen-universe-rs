use getopts;
use eg::script;
use evergreen as eg;

const HELP_TEXT: &str = r#"
    --sip-host <hostname>
    --sip-port <port>
    --sip-user <sip username>
    --sip-pass <sip password>
    --timeout <receive timeout in seconds>
"#;

fn main() {
    let mut ops = getops::Options::new();

    ops.optflag("h", "help", "");
    ops.optopt("", "sip-host", "", "");
    ops.optopt("", "sip-port", "", "");
    ops.optopt("", "sip-user", "", "");
    ops.optopt("", "sip-pass", "", "");
    ops.optopt("", "timeout", "", "");

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

    let sip_pas = params
        .opt_get_default("sip-pass", "sip-pass".to_string())
        .unwrap();

    let timeout = params.opt_get_default("timeout", 2u8).unwrap();

    let sip_host = format!("{host}:{port}");

    let sipcon = match sip2::Connection::new(&sip_host) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    };

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

    let resp = match sipcon.sendrecv_with_timeout(&req, timeout) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    };

    tester.sipcon.disconnect().ok();

    if resp.fixed_fields().len() == 1 && resp.fixed_fields()[0].value() == "1" {
        std::process::exit(exitcode::OK);
    } else {
        std::process::exit(exitcode::NOUSER);
    }
}

