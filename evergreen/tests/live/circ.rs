use crate::util;
use eg::common::circulator::Circulator;
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
    checkin_one(tester)?;
    tester.timer.stop("checkin_one()");

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

fn checkin_one(tester: &mut util::Tester) -> EgResult<()> {
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

    assert_eq!(
        evt.payload()["copy"]["barcode"].as_str(),
        Some(tester.samples.acp_barcode.as_str())
    );

    Ok(())
}
