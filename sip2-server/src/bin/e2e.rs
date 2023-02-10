use evergreen as eg;
use sip2;
use getopts;
use std::time::SystemTime;
use std::sync::Arc;

// Default values for assets
const ACN_CREATOR: i64 = 1;
const ACN_RECORD: i64 = 1;
const ACN_LABEL: &str = "_SIP_TEST_";
const ACN_LABEL_CLASS: i64 = 1; // Generic
const ACP_STATUS: i64 = 0; // Available
const ACP_BARCODE: &str = "_SIP_TEST_";
const ACP_LOAN_DURATION: i64 = 1;
const ACP_FINE_LEVEL: i64 = 2; // Medium?
const ORG_ID: i64 = 4;
const ORG_SHORTNAME: &str = "BR1";
const AU_BARCODE: &str = "_SIP_TEST_";
const AU_PROFILE: i64 = 2; // Patrons
const AU_IDENT_TYPE: i64 = 3; // Other

struct Tester {
    sip_user: String,
    sip_pass: String,
    institution: String,
    sipcon: sip2::Connection,
    editor: eg::Editor,
    idl: Arc<eg::idl::Parser>,
    acn_creator: i64,
    acn_record: i64,
    org_id: i64,
    org_shortname: String,
    acn_label: String,
    acn_label_class: i64,
    acp_barcode: String,
    au_barcode: String,
    au_profile: i64,
    au_ident_type: i64,
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
        org_id: ORG_ID,
        acn_label: ACN_LABEL.to_string(),
        acn_label_class: ACN_LABEL_CLASS,
        acp_barcode: ACP_BARCODE.to_string(),
        org_shortname: ORG_SHORTNAME.to_string(),
        au_barcode: AU_BARCODE.to_string(),
        au_profile: AU_PROFILE,
        au_ident_type: AU_IDENT_TYPE,
        sip_user: options.opt_get_default("sip-user", "sip-user".to_string()).unwrap(),
        sip_pass: options.opt_get_default("sip-pass", "sip-pass".to_string()).unwrap(),
        institution: options.opt_get_default("institution", "example".to_string()).unwrap(),
    };

    let now = SystemTime::now();
    let (acp, acn, au) = create_test_assets(&mut tester)?;
    let t = now.elapsed().unwrap().as_micros();
    log(t, "Create Test Assets");

    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    // Run them twice to get a sense of the speed difference
    // for collecting some of the same data (e.g. org units) within
    // an existing back-end sip server thread.
    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    let now = SystemTime::now();
    delete_test_assets(&mut tester, &acp, &acn, &au)?;
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

fn create_test_assets(
    tester: &mut Tester
) -> Result<(
    json::JsonValue,
    json::JsonValue,
    json::JsonValue,
), String> {

    let seed = json::object! {
        creator: tester.acn_creator,
        editor: tester.acn_creator,
        record: tester.acn_record,
        owning_lib: tester.org_id,
        label: tester.acn_label.to_string(),
        label_class: tester.acn_label_class,
    };

    let acn = tester.idl.create_from("acn", seed)?;

    let e = &mut tester.editor;

    e.xact_begin()?;

    // Grab the from-database version of the acn.
    let acn = e.create(&acn)?;

    let seed = json::object! {
        call_number: acn["id"].clone(),
        creator: tester.acn_creator,
        editor: tester.acn_creator,
        status: ACP_STATUS,
        circ_lib: tester.org_id,
        loan_duration: ACP_LOAN_DURATION,
        fine_level: ACP_FINE_LEVEL,
        barcode: tester.acp_barcode.to_string(),
    };

    let acp = tester.idl.create_from("acp", seed)?;

    let acp = e.create(&acp)?;

    let seed = json::object! {
        profile: tester.au_profile,
        usrname: tester.au_barcode.to_string(),
        passwd: tester.au_barcode.to_string(),
        ident_type: tester.au_ident_type,
        first_given_name: "SIP TEST",
        family_name: "SIP TEST",
        home_ou: ORG_ID,
    };

    let au = tester.idl.create_from("au", seed)?;

    let au = e.create(&au)?;

    let seed = json::object! {
        barcode: tester.au_barcode.to_string(),
        usr: au["id"].clone(),
    };

    let ac = tester.idl.create_from("ac", seed)?;
    e.create(&ac)?;

    e.commit()?;

    Ok((acp, acn, au))
}

fn delete_test_assets(
    tester: &mut Tester,
    acp: &json::JsonValue,
    acn: &json::JsonValue,
    au: &json::JsonValue,
) -> Result<(), String> {
    let e = &mut tester.editor;

    e.xact_begin()?;

    e.delete(acp)?;
    e.delete(acn)?;

    // Purge the user
    // This deletes the ac (card) we created as well.
    let query = json::object! {
        from: ["actor.usr_delete", au["id"].clone(), json::JsonValue::Null]
    };

    e.json_query(query)?;

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

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), tester.acp_barcode);
    assert_ne!(title.unwrap(), "");
    assert_eq!(circ_status, "03");

    assert_eq!(resp.get_field_value("CT").unwrap(), tester.org_shortname);
    assert_eq!(resp.get_field_value("BG").unwrap(), tester.org_shortname);
    assert_eq!(resp.get_field_value("AP").unwrap(), tester.org_shortname);
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
            ("AA", &tester.au_barcode),
            ("AD", &tester.au_barcode),
            ("AO", &tester.institution),
        ],
    ).unwrap();

    let now = SystemTime::now();
    let resp = tester.sipcon.sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    let duration = now.elapsed().unwrap().as_micros();

    assert_eq!(resp.get_field_value("AA").unwrap(), tester.au_barcode);
    assert_eq!(resp.get_field_value("BL").unwrap(), "Y"); // valid patron
    assert_eq!(resp.get_field_value("CQ").unwrap(), "Y"); // valid password
    assert_eq!(&resp.get_field_value("BV").unwrap(), "0.00"); // fee amount

    Ok(duration)
}


