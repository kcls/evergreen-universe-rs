use evergreen as eg;
use eg::samples::SampleData;
use sip2;
use getopts;
use std::time::SystemTime;
use std::sync::Arc;

struct Tester {
    sip_user: String,
    sip_pass: String,
    institution: String,
    sipcon: sip2::Connection,
    editor: eg::Editor,
    idl: Arc<eg::idl::Parser>,
    samples: SampleData,
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
        idl: ctx.idl().clone(),
        samples: SampleData::new(),
        sip_user: options.opt_get_default("sip-user", "sip-user".to_string()).unwrap(),
        sip_pass: options.opt_get_default("sip-pass", "sip-pass".to_string()).unwrap(),
        institution: options.opt_get_default("institution", "example".to_string()).unwrap(),
    };

    let now = SystemTime::now();
    delete_test_assets(&mut tester)?;
    let t = now.elapsed().unwrap().as_micros();
    log(t, "Pre-Delete Test Assets");

    let now = SystemTime::now();
    create_test_assets(&mut tester)?;
    let t = now.elapsed().unwrap().as_micros();
    log(t, "Create Test Assets");

    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    println!("--------------------------------------");

    // Run them twice to get a sense of the speed difference
    // for collecting some of the same data (e.g. org units) within
    // an existing back-end sip server thread.
    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    let now = SystemTime::now();
    delete_test_assets(&mut tester)?;
    let t = now.elapsed().unwrap().as_micros();
    log(t, "Delete Test Assets");

    tester.sipcon.disconnect().ok();

    Ok(())
}

fn run_tests(tester: &mut Tester) -> Result<(), String> {

    log(test_invalid_login(tester)?, "test_invalid_login");
    log(test_valid_login(tester)?, "test_valid_login");
    log(test_sc_status(tester)?, "test_sc_status");
    log(test_invalid_item_info(tester)?, "test_invalid_item_info");
    log(test_item_info(tester)?, "test_item_info");
    log(test_patron_status(tester)?, "test_patron_status");

    Ok(())
}

fn create_test_assets(tester: &mut Tester) -> Result<(), String> {
    let e = &mut tester.editor;

    e.xact_begin()?;

    let acn = tester.samples.create_default_acn(e)?;
    tester.samples.create_default_acp(e, eg::util::json_int(&acn["id"])?)?;
    tester.samples.create_default_au(e)?;

    e.commit()
}

fn delete_test_assets(tester: &mut Tester) -> Result<(), String> {
    let e = &mut tester.editor;

    e.xact_begin()?;

    tester.samples.delete_default_acp(e)?;
    tester.samples.delete_default_acn(e)?;
    tester.samples.delete_default_au(e)?;

    e.commit()?;

    Ok(())
}

fn log(duration: u128, test: &str) {
    // We'll never need a full u128.
    let millis = (duration as f64) / 1000.0; // micros -> millis
    println!("OK [{:.3} ms]\t{test}", millis);
}

fn test_invalid_login(tester: &mut Tester) -> Result<u128, String> {

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
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "0");

    Ok(duration)
}

fn test_valid_login(tester: &mut Tester) -> Result<u128, String> {

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
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "1");

    Ok(duration)
}

fn test_sc_status(tester: &mut Tester) -> Result<u128, String> {
    let req = sip2::Message::from_ff_values(
        &sip2::spec::M_SC_STATUS,
        &[
            "0",    // status code
            "999",  // max print width
            &sip2::spec::SIP_PROTOCOL_VERSION,
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    assert!(resp.fixed_fields().len() > 0);
    assert_eq!(resp.fixed_fields()[0].value(), "Y");

    Ok(duration)
}

fn test_invalid_item_info(tester: &mut Tester) -> Result<u128, String> {

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
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), dummy);
    assert_eq!(title.unwrap(), "");
    assert_eq!(circ_status, "01");

    Ok(duration)
}

fn test_item_info(tester: &mut Tester) -> Result<u128, String> {

    let req = sip2::Message::from_values(
        &sip2::spec::M_ITEM_INFO,
        &[&sip2::util::sip_date_now()],
        &[
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), tester.samples.acp_barcode);
    assert_ne!(title.unwrap(), "");
    assert_eq!(circ_status, "03");

    assert_eq!(resp.get_field_value("CT").unwrap(), tester.samples.org_shortname);
    assert_eq!(resp.get_field_value("BG").unwrap(), tester.samples.org_shortname);
    assert_eq!(resp.get_field_value("AP").unwrap(), tester.samples.org_shortname);
    assert_eq!(&resp.get_field_value("BV").unwrap(), "0.00"); // fee amount
    assert_eq!(&resp.get_field_value("CF").unwrap(), "0"); // hold queue len
    assert_eq!(&resp.get_field_value("CK").unwrap(), "001"); // media type

    Ok(duration)
}

fn test_patron_status(tester: &mut Tester) -> Result<u128, String> {

    let req = sip2::Message::from_values(
        &sip2::spec::M_PATRON_STATUS,
        &["000", &sip2::util::sip_date_now()],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AD", &tester.samples.au_barcode),
            ("AO", &tester.institution),
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.get_field_value("AA").unwrap(), tester.samples.au_barcode);
    assert_eq!(resp.get_field_value("BL").unwrap(), "Y"); // valid patron
    assert_eq!(resp.get_field_value("CQ").unwrap(), "Y"); // valid password
    assert_eq!(&resp.get_field_value("BV").unwrap(), "0.00"); // fee amount

    let status = resp.fixed_fields()[0].value();
    assert_eq!(status.len(), 14);
    assert!(!status.contains("Y")); // no blocks

    Ok(duration)
}


