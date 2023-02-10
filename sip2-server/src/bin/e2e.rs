use evergreen as eg;
use sip2;
use getopts;
use std::time::SystemTime;
use std::sync::Arc;

// Default values for assets
const ACN_CREATOR: i64 = 1;
const ACN_RECORD: i64 = 1;
const ACN_OWNING_LIB: i64 = 4;
const ACN_LABEL: &str = "_SIP_TEST_";
const ACN_LABEL_CLASS: i64 = 1; // Generic
const ACP_STATUS: i64 = 0; // Available
const ACP_BARCODE: &str = "_SIP_TEST_";
const ACP_LOAN_DURATION: i64 = 1;
const ACP_FINE_LEVEL: i64 = 2; // Medium?

struct Tester {
    sip_user: String,
    sip_pass: String,
    institution: String,
    sipcon: sip2::Connection,
    editor: eg::Editor,
    idl: Arc<eg::idl::Parser>,
    acn_creator: i64,
    acn_record: i64,
    acn_owning_lib: i64,
    acn_label: String,
    acn_label_class: i64,
    acp_barcode: String,
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
        // TODO command line ops
        acn_creator: ACN_CREATOR,
        acn_record: ACN_RECORD,
        acn_owning_lib: ACN_OWNING_LIB,
        acn_label: ACN_LABEL.to_string(),
        acn_label_class: ACN_LABEL_CLASS,
        acp_barcode: ACP_BARCODE.to_string(),
        sip_user: options.opt_get_default("sip-user", "sip-user".to_string()).unwrap(),
        sip_pass: options.opt_get_default("sip-pass", "sip-pass".to_string()).unwrap(),
        institution: options.opt_get_default("institution", "example".to_string()).unwrap(),
    };

    let (acp, acn) = create_test_assets(&mut tester)?;

    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    delete_test_assets(&mut tester, &acp, &acn)?;

    tester.sipcon.disconnect().ok();

    Ok(())
}

fn run_tests(tester: &mut Tester) -> Result<(), String> {

    log(test_invalid_login(tester)?, "test_invalid_login");
    log(test_valid_login(tester)?, "test_valid_login");
    log(test_sc_status(tester)?, "test_sc_status");
    log(test_sc_status(tester)?, "test_sc_status (2nd time)");
    log(test_invalid_item_info(tester)?, "test_invalid_item_info");
    log(test_item_info(tester)?, "test_item_info");

    Ok(())
}

fn create_test_assets(tester: &mut Tester) -> Result<(json::JsonValue, json::JsonValue), String> {

    let obj = json::object! {
        creator: tester.acn_creator,
        editor: tester.acn_creator,
        record: tester.acn_record,
        owning_lib: tester.acn_owning_lib,
        label: tester.acn_label.to_string(),
        label_class: tester.acn_label_class,
    };

    let acn = tester.idl.create_from("acn", obj)?;

    let e = &mut tester.editor;

    e.xact_begin()?;

    // Grab the from-database version of the acn.
    let acn = e.create(&acn)?;

    let obj = json::object! {
        call_number: acn["id"].clone(),
        creator: tester.acn_creator,
        editor: tester.acn_creator,
        status: ACP_STATUS,
        circ_lib: tester.acn_owning_lib,
        loan_duration: ACP_LOAN_DURATION,
        fine_level: ACP_FINE_LEVEL,
        barcode: tester.acp_barcode.to_string(),
    };

    let acp = tester.idl.create_from("acp", obj)?;

    let acp = e.create(&acp)?;

    e.commit()?;

    Ok((acp, acn))
}

fn delete_test_assets(
    tester: &mut Tester,
    acp: &json::JsonValue,
    acn: &json::JsonValue,
) -> Result<(), String> {
    let e = &mut tester.editor;

    e.xact_begin()?;
    e.delete(acp)?;
    e.delete(acn)?;
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
            ("AB", &tester.acp_barcode),
            ("AO", &tester.institution),
        ]
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    println!("ITEM INFO: {resp:?}");

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), tester.acp_barcode);
    assert_ne!(title.unwrap(), "");
    assert_eq!(circ_status, "03");

    Ok(duration)
}
