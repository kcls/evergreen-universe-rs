use crate::util;
use eg::common::circulator::Circulator;
use eg::constants as C;
use eg::result::EgResult;
use evergreen as eg;
use json;
use std::collections::HashMap;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    util::login(tester)?;

    tester.timer.start();
    delete_test_assets(tester)?;
    tester.timer.stop("Deleted circ assets");

    tester.timer.start();
    create_test_assets(tester)?;
    tester.timer.stop("Created circ assets");

    tester.timer.start();
    checkout(tester)?;
    tester.timer.stop("checkout()");

    tester.timer.start();
    checkin_item_at_home(tester)?;
    tester.timer.stop("checkin_item_at_home()");

    tester.timer.start();
    checkin_item_remote(tester)?;
    tester.timer.stop("checkin_item_remote()");

    tester.timer.start();
    delete_test_assets(tester)?;
    tester.timer.stop("Deleted circ assets");

    Ok(())
}

fn create_test_assets(tester: &mut util::Tester) -> EgResult<()> {
    let e = &mut tester.editor;
    e.xact_begin()?;

    let acn = tester.samples.create_default_acn(e)?;
    tester
        .samples
        .create_default_acp(e, eg::util::json_int(&acn["id"])?)?;
    tester.samples.create_default_au(e)?;

    e.commit()
}

fn delete_test_assets(tester: &mut util::Tester) -> EgResult<()> {
    let e = &mut tester.editor;
    e.xact_begin()?;

    tester.samples.delete_default_acp(e)?;
    tester.samples.delete_default_acn(e)?;
    tester.samples.delete_default_au(e)?;

    e.commit()
}

fn checkout(tester: &mut util::Tester) -> EgResult<()> {
    let e = tester.editor.clone(); // circulator wants its own editor

    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    options.insert(
        "copy_barcode".to_string(),
        json::from(tester.samples.acp_barcode.as_str()),
    );

    options.insert(
        "patron_barcode".to_string(),
        json::from(tester.samples.au_barcode.as_str()),
    );

    let mut circulator = Circulator::new(e, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    circulator.checkout()?;
    circulator.commit()?;

    let evt = circulator
        .events()
        .get(0)
        .ok_or(format!("Checkin returned no result!"))?;

    assert!(evt.is_success());

    let copy = &evt.payload()["copy"];
    let patron = &evt.payload()["patron"];
    let circ = &evt.payload()["circ"];

    assert_eq!(
        copy["barcode"].as_str(),
        Some(tester.samples.acp_barcode.as_str())
    );

    assert_eq!(
        eg::util::json_int(&copy["status"])?,
        C::COPY_STATUS_CHECKED_OUT
    );

    assert_eq!(
        patron["card"]["barcode"].as_str(),
        Some(tester.samples.au_barcode.as_str())
    );

    // make sure the circ actually exists
    let circ_id = circ["id"].clone();
    let circ = tester.editor.retrieve("circ", circ_id)?.unwrap();

    // Some basic checks
    assert_eq!(circ["duration_rule"].as_str(), Some("default"));
    assert!(circ["stop_fines"].is_null());

    Ok(())
}

fn checkin_item_at_home(tester: &mut util::Tester) -> EgResult<()> {
    let e = tester.editor.clone(); // circulator wants its own editor

    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    options.insert(
        "copy_barcode".to_string(),
        json::from(tester.samples.acp_barcode.as_str()),
    );

    let mut circulator = Circulator::new(e, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    circulator.checkin()?;
    circulator.commit()?;

    let evt = circulator
        .events()
        .get(0)
        .ok_or(format!("Checkin returned no result!"))?;

    assert!(evt.is_success());

    let copy = &evt.payload()["copy"];
    let patron = &evt.payload()["patron"];

    assert_eq!(
        copy["barcode"].as_str(),
        Some(tester.samples.acp_barcode.as_str())
    );

    assert_eq!(
        eg::util::json_int(&copy["status"])?,
        C::COPY_STATUS_RESHELVING
    );

    assert_eq!(
        patron["card"]["barcode"].as_str(),
        Some(tester.samples.au_barcode.as_str())
    );

    Ok(())
}

fn checkin_item_remote(tester: &mut util::Tester) -> EgResult<()> {
    let e = tester.editor.clone(); // circulator wants its own editor

    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    options.insert(
        "copy_barcode".to_string(),
        json::from(tester.samples.acp_barcode.as_str()),
    );

    // Tell the circulator we're operating from a different org unit
    // so our item goes into transit on checkin.
    options.insert("circ_lib".to_string(), json::from(eg::samples::AOU_BR2_ID));

    let mut circulator = Circulator::new(e, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    circulator.checkin()?;

    circulator.commit()?;

    let evt = circulator
        .events()
        .get(0)
        .ok_or(format!("Checkin returned no result!"))?;

    assert_eq!(evt.textcode(), "ROUTE_ITEM");

    let copy = &evt.payload()["copy"];

    assert_eq!(
        copy["barcode"].as_str(),
        Some(tester.samples.acp_barcode.as_str())
    );

    assert_eq!(
        eg::util::json_int(&copy["status"])?,
        C::COPY_STATUS_IN_TRANSIT
    );

    Ok(())
}
