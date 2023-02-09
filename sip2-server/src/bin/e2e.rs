use evergreen as eg;
use sip2;
use getopts;
use std::time::SystemTime;

struct Tester {
    sip_user: String,
    sip_pass: String,
    institution: String,
    sipcon: sip2::Connection,
    editor: eg::Editor,
}

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
    opts.optopt("", "institution", "", "");

    // OpenSRF connect, get host settings, parse IDL, etc.
    let now = SystemTime::now();
    let ctx = eg::init::init_with_options(&mut opts).expect("Evergreen Init");
    let t = now.elapsed().unwrap().as_micros();
    log(t, "EG Init");

    let options = ctx.params();

    if options.opt_present("help") {
        println!("{}", HELP_TEXT);
        return Ok(());
    }

    let host = options.opt_get_default("sip-host", "127.0.0.1".to_string()).unwrap();
    let port = options.opt_get_default("sip-port", "6001".to_string()).unwrap();
    let sip_host = format!("{host}:{port}");

    let editor = eg::Editor::new(ctx.client(), ctx.idl());

    let now = SystemTime::now();
    let sipcon = sip2::Connection::new(&sip_host).expect("Error creating SIP connection");
    let t = now.elapsed().unwrap().as_micros();
    log(t, "SIP Connect");

    let mut tester = Tester {
        sipcon,
        editor,
        sip_user: options.opt_get_default("sip-user", "sip-user".to_string()).unwrap(),
        sip_pass: options.opt_get_default("sip-pass", "sip-pass".to_string()).unwrap(),
        institution: options.opt_get_default("institution", "example".to_string()).unwrap(),
    };

    log(test_invalid_login(&mut tester), "test_invalid_login");
    log(test_valid_login(&mut tester), "test_valid_login");
    log(test_sc_status(&mut tester), "test_sc_status");
    log(test_sc_status(&mut tester), "test_sc_status (2nd time)");
    log(test_invalid_item_info(&mut tester), "test_invalid_item_info");

    tester.sipcon.disconnect().ok();

    Ok(())
}

fn log(duration: u128, test: &str) {
    // We'll never need a full u128.
    let millis = (duration as f64) / 1000.0; // micros -> millis
    println!("OK [{:.3} ms]\t{test}", millis);
}

fn test_invalid_login(tester: &mut Tester) -> u128 {

    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN,
        &[
            "0",    // UID algo
            "0",    // PW algo
        ],
        &[
            ("CN", &format!("+23423+")),   // SIP login username
            ("CO", &format!("+29872+")),   // SIP login password
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req).unwrap();
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "0");

    duration
}

fn test_valid_login(tester: &mut Tester) -> u128 {

    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN,
        &[
            "0",    // UID algo
            "0",    // PW algo
        ],
        &[
            ("CN", &tester.sip_user),   // SIP login username
            ("CO", &tester.sip_pass),   // SIP login password
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req).unwrap();
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "1");

    duration
}

fn test_sc_status(tester: &mut Tester) -> u128 {
    let req = sip2::Message::from_ff_values(
        &sip2::spec::M_SC_STATUS,
        &[
            "0",    // status code
            "999",  // max print width
            &sip2::spec::SIP_PROTOCOL_VERSION,
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req).unwrap();
    let duration = now.elapsed().unwrap().as_micros();

    assert!(resp.fixed_fields().len() > 0);
    assert_eq!(resp.fixed_fields()[0].value(), "Y");

    duration
}

fn test_invalid_item_info(tester: &mut Tester) -> u128 {

    let dummy = "I-AM-BAD-BARCODE";

    let req = sip2::Message::from_values(
        &sip2::spec::M_ITEM_INFO,
        &[&sip2::util::sip_date_now()],
        &[
            ("AB", dummy),
            ("AO", &tester.institution),
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req).unwrap();
    let duration = now.elapsed().unwrap().as_micros();

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    assert!(barcode.is_some());
    assert!(title.is_some());
    assert_eq!(barcode.unwrap(), dummy);
    assert_eq!(title.unwrap(), "");
    assert_eq!(circ_status, "01");

    duration
}
