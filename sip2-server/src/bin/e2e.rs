use evergreen as eg;
use sip2;
use getopts;

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

    test_invalid_login(&mut sipcon, &user, &pass);
    println!("OK test_invalid_login");

    test_valid_login(&mut sipcon, &user, &pass);
    println!("OK test_valid_login");

    sipcon.disconnect().ok();

    Ok(())
}

fn test_invalid_login(sipcon: &mut sip2::Connection, user: &str, pass: &str) -> Result<(), String> {

    let mut req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN,
        &[
            "0",    // UID algo
            "0",    // PW algo
        ],
        &[
            ("CN", "%%%"),   // SIP login username
            ("CO", "%%%"),   // SIP login password
        ],
    ).unwrap();

    let resp = sipcon.sendrecv(&req).unwrap();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "0");

    Ok(())
}

fn test_valid_login(sipcon: &mut sip2::Connection, user: &str, pass: &str) -> Result<(), String> {

    let mut req = sip2::Message::from_values(
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

    let resp = sipcon.sendrecv(&req).unwrap();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "1");

    Ok(())
}

