use eg::samples::SampleData;
use eg::EgValue;
use evergreen as eg;
use getopts;
use sip2;
use std::time::SystemTime;

fn is_zero(n: &str) -> bool {
    if let Ok(f) = n.parse::<f64>() {
        f == 0.0
    } else {
        false
    }
}

struct Timer {
    start: SystemTime,
}

impl Timer {
    fn new() -> Timer {
        Timer {
            start: SystemTime::now(),
        }
    }

    fn done(&self, msg: &str) {
        let duration = self.start.elapsed().unwrap().as_micros();
        // translate micros to millis retaining 3 decimal places.
        let millis = (duration as f64) / 1000.0;
        println!("OK [{:.3} ms]\t{msg}", millis);
    }
}

struct Tester {
    sip_user: String,
    sip_pass: String,
    institution: String,
    sipcon: sip2::Connection,
    editor: eg::Editor,
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
    let args: Vec<String> = std::env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optflag("h", "help", "");
    opts.optopt("", "sip-host", "", "");
    opts.optopt("", "sip-port", "", "");
    opts.optopt("", "sip-user", "", "");
    opts.optopt("", "sip-pass", "", "");
    opts.optopt("", "institution", "", "");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => panic!("Error parsing options: {}", e),
    };

    // OpenSRF connect, get host settings, parse IDL, etc.
    let t = Timer::new();
    let client = eg::init().expect("Evergreen Init");
    t.done("EG Init");

    if params.opt_present("help") {
        println!("{}", HELP_TEXT);
        return Ok(());
    }

    let host = params
        .opt_get_default("sip-host", "127.0.0.1".to_string())
        .unwrap();
    let port = params
        .opt_get_default("sip-port", "6001".to_string())
        .unwrap();
    let sip_host = format!("{host}:{port}");

    let editor = eg::Editor::new(&client);

    let t = Timer::new();
    let sipcon = sip2::Connection::new(&sip_host).expect("Error creating SIP connection");
    t.done("SIP Connect");

    //std::thread::sleep(std::time::Duration::from_secs(15));

    let mut tester = Tester {
        sipcon,
        editor,
        samples: SampleData::new(),
        sip_user: params
            .opt_get_default("sip-user", "sip-user".to_string())
            .unwrap(),
        sip_pass: params
            .opt_get_default("sip-pass", "sip-pass".to_string())
            .unwrap(),
        institution: params
            .opt_get_default("institution", "example".to_string())
            .unwrap(),
    };

    let t = Timer::new();
    delete_test_assets(&mut tester)?;
    t.done("Pre-Delete Test Assets");

    let t = Timer::new();
    create_test_assets(&mut tester)?;
    t.done("Create Test Assets");

    println!("--------------------------------------");

    if let Err(e) = run_tests(&mut tester) {
        eprintln!("Tester exited with error: {e}");
    };

    println!("--------------------------------------");

    let t = Timer::new();
    delete_test_assets(&mut tester)?;
    t.done("Delete Test Assets");

    tester.sipcon.disconnect().ok();

    Ok(())
}

fn run_tests(tester: &mut Tester) -> Result<(), String> {
    test_invalid_login(tester)?;
    test_valid_login(tester)?;

    // Run whatever tests we can multiple times to get a sense of
    // timing for multiple scenarios.

    test_sc_status(tester)?;
    test_invalid_item_info(tester)?;
    test_item_info(tester, false)?;
    test_patron_status(tester)?;
    test_patron_info(tester, false)?;

    test_checkout(tester)?;
    test_item_info(tester, true)?;
    test_patron_status(tester)?;
    test_patron_info(tester, true)?;

    // Checkout a second time to force a renewal.
    test_checkout(tester)?;
    test_item_info(tester, true)?;
    test_checkin(tester)?;

    test_item_info(tester, false)?;
    test_patron_status(tester)?;
    test_patron_info(tester, false)?;

    test_checkout(tester)?;
    test_item_info(tester, true)?;
    test_patron_status(tester)?;
    test_patron_info(tester, true)?;
    test_checkin(tester)?;

    test_checkin_with_transit(tester)?;

    Ok(())
}

fn create_test_assets(tester: &mut Tester) -> Result<(), String> {
    let e = &mut tester.editor;

    e.xact_begin()?;

    let acn = tester.samples.create_default_acn(e)?;
    tester.samples.create_default_acp(e, acn.id()?)?;
    tester.samples.create_default_au(e)?;

    e.commit().map_err(|e| e.to_string())
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

fn test_invalid_login(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN.code,
        &[
            "0", // UID algo
            "0", // PW algo
        ],
        &[
            ("CN", &format!("+23423+")), // SIP login username
            ("CO", &format!("+29872+")), // SIP login password
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_invalid_login");

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "0");

    Ok(())
}

fn test_valid_login(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_LOGIN.code,
        &[
            "0", // UID algo
            "0", // PW algo
        ],
        &[
            ("CN", &tester.sip_user), // SIP login username
            ("CO", &tester.sip_pass), // SIP login password
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_valid_login");

    assert_eq!(resp.spec().code, sip2::spec::M_LOGIN_RESP.code);
    assert_eq!(resp.fixed_fields().len(), 1);
    assert_eq!(resp.fixed_fields()[0].value(), "1");

    Ok(())
}

fn test_sc_status(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_ff_values(
        &sip2::spec::M_SC_STATUS.code,
        &[
            "0",   // status code
            "999", // max print width
            &sip2::spec::SIP_PROTOCOL_VERSION,
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_sc_status");

    assert!(resp.fixed_fields().len() > 0);
    assert_eq!(resp.fixed_fields()[0].value(), "Y");

    Ok(())
}

fn test_invalid_item_info(tester: &mut Tester) -> Result<(), String> {
    let dummy = "I-AM-BAD-BARCODE";

    let req = sip2::Message::from_values(
        &sip2::spec::M_ITEM_INFO.code,
        &[&sip2::util::sip_date_now()],
        &[("AB", dummy), ("AO", &tester.institution)],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_invalid_item_info");

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), dummy);
    assert_eq!(title.unwrap(), "");
    assert_eq!(circ_status, "01");

    Ok(())
}

fn test_item_info(tester: &mut Tester, charged: bool) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_ITEM_INFO.code,
        &[&sip2::util::sip_date_now()],
        &[
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_item_info");

    let circ_status = resp.fixed_fields()[0].value();
    let barcode = resp.get_field_value("AB");
    let title = resp.get_field_value("AJ");

    // We should get title/barcode fields in the response.
    assert!(barcode.is_some());
    assert!(title.is_some());

    assert_eq!(barcode.unwrap(), tester.samples.acp_barcode);
    assert_ne!(title.unwrap(), "");
    if charged {
        assert_eq!(circ_status, "04");
    } else {
        // May be available or reshelving
        assert!(circ_status.eq("03") || circ_status.eq("09"));
    }

    if let Some(dest) = resp.get_field_value("CT") {
        assert_eq!(dest, tester.samples.aou_shortname);
    }

    assert_eq!(
        resp.get_field_value("BG").unwrap(),
        tester.samples.aou_shortname
    );
    assert_eq!(
        resp.get_field_value("AP").unwrap(),
        tester.samples.aou_shortname
    );
    assert!(is_zero(resp.get_field_value("BV").unwrap())); // fee amount

    if let Some(ql) = resp.get_field_value("CF") {
        assert_eq!(ql, "0"); // hold queue len
    }

    assert_eq!(resp.get_field_value("CK").unwrap(), "001"); // media type

    Ok(())
}

fn test_patron_status(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_PATRON_STATUS.code,
        &["000", &sip2::util::sip_date_now()],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AD", &tester.samples.au_barcode),
            ("AO", &tester.institution),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_patron_status");

    assert_eq!(
        resp.get_field_value("AA").unwrap(),
        tester.samples.au_barcode
    );
    assert_eq!(resp.get_field_value("BL").unwrap(), "Y"); // valid patron
    assert_eq!(resp.get_field_value("CQ").unwrap(), "Y"); // valid password

    if let Some(fee) = resp.get_field_value("BV") {
        assert!(is_zero(fee));
    }

    let status = resp.fixed_fields()[0].value();
    assert_eq!(status.len(), 14);

    // Legacy EG sip server will set a Y on the 'recall denied' field,
    // regardless of patron, because it does not support recalls.
    if status.contains("Y") {
        assert_eq!(&status[2..3], "Y");
    }

    Ok(())
}

fn test_patron_info(tester: &mut Tester, charged: bool) -> Result<(), String> {
    let summary = "          ";

    let req = sip2::Message::from_values(
        &sip2::spec::M_PATRON_INFO.code,
        &["000", &sip2::util::sip_date_now(), summary],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AD", &tester.samples.au_barcode),
            ("AO", &tester.institution),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_patron_info");

    assert_eq!(
        resp.get_field_value("AA").unwrap(),
        tester.samples.au_barcode
    );
    assert_eq!(resp.get_field_value("BL").unwrap(), "Y"); // valid patron
    assert_eq!(resp.get_field_value("CQ").unwrap(), "Y"); // valid password

    if let Some(fee) = resp.get_field_value("BV") {
        assert!(is_zero(fee)); // fee amount
    }

    assert_eq!(
        &resp.get_field_value("AQ").unwrap(),
        &tester.samples.aou_shortname
    );

    let status = resp.fixed_fields()[0].value();
    assert_eq!(status.len(), 14);

    // Legacy EG sip server will set a Y on the 'recall denied' field,
    // regardless of patron, because it does not support recalls.
    if status.contains("Y") {
        assert_eq!(&status[2..3], "Y");
    }

    // Summary counts.  Should all be zero since this is a new patron.
    assert_eq!(resp.fixed_fields()[3].value(), "0000"); // holds
    assert_eq!(resp.fixed_fields()[4].value(), "0000"); // overdue
    if charged {
        assert_eq!(resp.fixed_fields()[5].value(), "0001"); // charged
    } else {
        assert_eq!(resp.fixed_fields()[5].value(), "0000"); // charged
    }
    assert_eq!(resp.fixed_fields()[6].value(), "0000"); // fine count
    assert_eq!(resp.fixed_fields()[7].value(), "0000"); // recall count
    assert_eq!(resp.fixed_fields()[8].value(), "0000"); // unavail hold count

    Ok(())
}

fn test_checkout(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_CHECKOUT.code,
        &[
            "Y", // renewal allowed if needed
            "N", // previously checked out offline / no block
            &sip2::util::sip_date_now(),
            "                  ", // no-block due date
        ],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_checkout");

    assert_eq!(resp.fixed_fields()[0].value(), "1"); // checkout ok.

    // This depends on which call to test_checkout() we're in the midst of.
    // 'renewal ok' means it was renewed, not that it can be renewed later.
    // assert_eq!(resp.fixed_fields()[1].value(), "Y"); // renewal ok.

    assert_eq!(
        resp.get_field_value("AA").unwrap(),
        tester.samples.au_barcode
    );
    assert_eq!(
        resp.get_field_value("AB").unwrap(),
        tester.samples.acp_barcode
    );
    assert_ne!(resp.get_field_value("AJ").unwrap(), ""); // assume we have some kind of title

    if let Some(da) = resp.get_field_value("BV") {
        assert!(is_zero(da));
    }

    Ok(())
}

fn test_checkin(tester: &mut Tester) -> Result<(), String> {
    let req = sip2::Message::from_values(
        &sip2::spec::M_CHECKIN.code,
        &[
            "N", // renewal policy
            &sip2::util::sip_date_now(),
            &sip2::util::sip_date_now(),
        ],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
            ("AP", &tester.samples.aou_shortname),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_checkin");

    assert_eq!(resp.fixed_fields()[0].value(), "1"); // checkin ok.
    assert_eq!(resp.fixed_fields()[1].value(), "Y"); // resensitize, i.e. not magnetic

    assert_eq!(
        resp.get_field_value("AB").unwrap(),
        tester.samples.acp_barcode
    );
    assert_ne!(resp.get_field_value("AJ").unwrap(), ""); // assume we have some kind of title
    assert_eq!(
        resp.get_field_value("AQ").unwrap(),
        tester.samples.aou_shortname
    );

    if let Some(da) = resp.get_field_value("BV") {
        assert!(is_zero(da));
    }

    Ok(())
}

/// Same as test_checkin except the item needs to transit back home.
fn test_checkin_with_transit(tester: &mut Tester) -> Result<(), String> {
    // Change the circ lib for the copy to an alternate org unit
    // and check it in "here".
    tester.editor.xact_begin()?;

    tester.samples.modify_default_acp(
        &mut tester.editor,
        eg::hash! {"circ_lib":  eg::samples::AOU_BR2_ID},
    )?;

    tester.editor.commit()?;

    let req = sip2::Message::from_values(
        &sip2::spec::M_CHECKIN.code,
        &[
            "N", // renewal policy
            &sip2::util::sip_date_now(),
            &sip2::util::sip_date_now(),
        ],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
            ("AP", &tester.samples.aou_shortname),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_checkin_with_transit");

    assert_eq!(resp.fixed_fields()[0].value(), "1"); // checkin ok.

    assert_eq!(
        resp.get_field_value("AQ").unwrap(),
        eg::samples::AOU_BR2_SHORTNAME
    );

    let copy = tester.samples.get_default_acp(&mut tester.editor)?;

    // Verify the transit was created.
    let query = eg::hash! {
        "target_copy": copy["id"].clone(),
        "dest_recv_time": EgValue::Null,
        "cancel_time": EgValue::Null,
    };

    let mut results = tester.editor.search("atc", query)?;

    assert_eq!(results.len(), 1);

    let transit = results.pop().unwrap();

    // Check it in again at the destination branch to
    // complete the transit.

    let req = sip2::Message::from_values(
        &sip2::spec::M_CHECKIN.code,
        &[
            "N", // renewal policy
            &sip2::util::sip_date_now(),
            &sip2::util::sip_date_now(),
        ],
        &[
            ("AA", &tester.samples.au_barcode),
            ("AB", &tester.samples.acp_barcode),
            ("AO", &tester.institution),
            ("AP", eg::samples::AOU_BR2_SHORTNAME),
        ],
    )
    .unwrap();

    let t = Timer::new();
    let resp = tester
        .sipcon
        .sendrecv(&req)
        .or_else(|e| Err(format!("SIP sendrecv error: {e}")))?;
    t.done("test_checkin_with_transit");

    assert_eq!(resp.fixed_fields()[0].value(), "1"); // checkin ok.

    assert_eq!(
        resp.get_field_value("AQ").unwrap(),
        eg::samples::AOU_BR2_SHORTNAME
    );

    // Verify the transit was completed
    let result = tester.editor.retrieve("atc", transit["id"].clone())?;
    assert!(result.is_some());

    let transit = result.unwrap();
    assert!(!transit["dest_recv_time"].is_null());

    Ok(())
}
