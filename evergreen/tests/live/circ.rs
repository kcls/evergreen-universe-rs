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

fn checkin_item_at_home(tester: &mut util::Tester) -> EgResult<()> {
    let e = tester.editor.clone(); // circulator wants its own editor

    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    options.insert(
        "copy_barcode".to_string(),
        json::from(tester.samples.acp_barcode.to_string()),
    );

    let mut circulator = Circulator::new(e, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    circulator.init().and_then(|()| circulator.checkin())?;

    circulator.commit()?;

    let evt = circulator
        .events()
        .get(0)
        .ok_or(format!("Checkin returned no result!"))?;

    assert!(evt.is_success());

    let copy = &evt.payload()["copy"];

    assert_eq!(
        copy["barcode"].as_str(),
        Some(tester.samples.acp_barcode.as_str())
    );

    assert_eq!(
        eg::util::json_int(&copy["status"])?,
        C::COPY_STATUS_RESHELVING
    );

    Ok(())
}

fn checkin_item_remote(tester: &mut util::Tester) -> EgResult<()> {
    let e = tester.editor.clone(); // circulator wants its own editor

    let mut options: HashMap<String, json::JsonValue> = HashMap::new();
    options.insert(
        "copy_barcode".to_string(),
        json::from(tester.samples.acp_barcode.to_string()),
    );

    // Tell the circulator we're operating from a different org unit
    // so our item goes into transit on checkin.
    options.insert("circ_lib".to_string(), json::from(eg::samples::AOU_BR2_ID));

    let mut circulator = Circulator::new(e, options)?;
    circulator.begin()?;

    // Collect needed data then kickoff the checkin process.
    circulator.init().and_then(|()| circulator.checkin())?;

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
