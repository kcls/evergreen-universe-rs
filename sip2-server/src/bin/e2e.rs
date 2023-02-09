use evergreen as eg;
use sip2;
use getopts;
use std::time::SystemTime;

const HELP_TEXT: &str = r#"
    --sip-host
    --sip-port
    --sip-user
    --sip-pass
    --help
"#;

fn main() -> Result<(), String> {

    let mut opts = getopts::Options::new();

    opts.optflag("h", "help", "");
    opts.optopt("", "sip-host", "", "");
    opts.optopt("", "sip-port", "", "");
    opts.optopt("", "sip-user", "", "");
    opts.optopt("", "sip-pass", "", "");

    let ctx = eg::init::init_with_options(&mut opts).expect("Evergreen Init");
    let options = ctx.params();

    if options.opt_present("help") {
        println!("{}", HELP_TEXT);
        return Ok(());
    }

    let host = options.opt_get_default("sip-host", "127.0.0.1".to_string()).unwrap();
    let port = options.opt_get_default("sip-port", "6001".to_string()).unwrap();
    let user = options.opt_get_default("sip-user", "sip-user".to_string()).unwrap();
    let pass = options.opt_get_default("sip-pass", "sip-pass".to_string()).unwrap();

    let sip_host = format!("{host}:{port}");

    println!("Connecting to SIP host: {sip_host}");

    let mut sipcon = sip2::Connection::new(&sip_host)
        .expect("Error creating SIP connection");

    let mut editor = eg::Editor::new(ctx.client(), ctx.idl());

    let t = test_invalid_login(&mut sipcon, &user, &pass);
    println!("OK [{t}] test_invalid_login");

    let t = test_valid_login(&mut sipcon, &user, &pass);
    println!("OK [{t}] test_valid_login");

    let t = test_sc_status(&mut sipcon);
    println!("OK [{t}] test_sc_status");

    sipcon.disconnect().ok();

    Ok(())
}

fn duration(micros: u128) -> String {
    // We'll never need a full u128.
    let millis = (micros as f64) / 1000.0;
    format!("{:.3}ms", millis)
}

fn test_invalid_login(sipcon: &mut sip2::Connection, user: &str, pass: &str) -> String {

    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN,
        &[
            "0",    // UID algo
            "0",    // PW algo
        ],
        &[
            ("CN", &format!("+{user}+")),   // SIP login username
            ("CO", &format!("+{pass}+")),   // SIP login password
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = sipcon.sendrecv(&req).unwrap();
    let duration = duration(now.elapsed().unwrap().as_micros());

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "0");

    duration
}

fn test_valid_login(sipcon: &mut sip2::Connection, user: &str, pass: &str) -> String {

    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN,
        &[
            "0",    // UID algo
            "0",    // PW algo
        ],
        &[
            ("CN", user),   // SIP login username
            ("CO", pass),   // SIP login password
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = sipcon.sendrecv(&req).unwrap();
    let duration = duration(now.elapsed().unwrap().as_micros());

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "1");

    duration
}

fn test_sc_status(sipcon: &mut sip2::Connection) -> String {
    let req = sip2::Message::from_ff_values(
        &sip2::spec::M_SC_STATUS,
        &[
            "0",    // status code
            "999",  // max print width
            &sip2::spec::SIP_PROTOCOL_VERSION,
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = sipcon.sendrecv(&req).unwrap();
    let duration = duration(now.elapsed().unwrap().as_micros());

    assert!(resp.fixed_fields().len() > 0);
    assert_eq!(resp.fixed_fields()[0].value(), "Y");

    duration
}
