use crate::util;
use eg::EgResult;
use evergreen::{self as eg, EgError};
use std::process::Command;

// This live test assumes default Evergreen concerto data set
pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    setup(tester)?;

    test_auth_to_auth_linker_script_creates_links(tester)?;

    Ok(())
}

fn setup(tester: &mut util::Tester) -> EgResult<()> {
    tester.timer.start();

    tester.editor.xact_begin()?;
    let preexisting_links = tester.editor.search("aalink", eg::hash! {"id": {">": 0}})?;
    for link in preexisting_links {
        tester.editor.delete(link)?;
    }
    tester.editor.xact_commit()?;

    Ok(())
}

fn test_auth_to_auth_linker_script_creates_links(tester: &mut util::Tester) -> EgResult<()> {
    let links_before_script = tester.editor.search("aalink", eg::hash! {"id": {">": 0}})?;
    assert_eq!(links_before_script.len(), 0);
    tester
        .timer
        .log("before running the script, we have 0 authority-authority links");

    Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("eg-auth-to-auth-linker")
        .arg("--")
        .arg("-a")
        .output()
        .or(EgResult::Err(EgError::Debug(
            "could not run authority_authority linker".to_string(),
        )))?;
    // You can also run this test against the original Perl implementation
    // if desired:
    // Command::new("authority_authority_linker.pl")
    //   .arg("-a")
    //   .output()
    //   .or(EgResult::Err(EgError::Debug(
    //       "could not run authority_authority linker".to_string(),
    //   )))?;

    let links_after_script = tester.editor.search("aalink", eg::hash! {"id": {">": 0}})?;
    assert_eq!(links_after_script.len(), 5);
    assert!(links_after_script
        .iter()
        .any(|link| link["source"] == 84.into()
            && link["target"] == 82.into()
            && link["field"] == 25.into()));
    assert!(links_after_script
        .iter()
        .any(|link| link["source"] == 81.into()
            && link["target"] == 83.into()
            && link["field"] == 25.into()));
    assert!(links_after_script
        .iter()
        .any(|link| link["source"] == 74.into()
            && link["target"] == 73.into()
            && link["field"] == 27.into()));
    assert!(links_after_script
        .iter()
        .any(|link| link["source"] == 93.into()
            && link["target"] == 87.into()
            && link["field"] == 25.into()));
    assert!(links_after_script
        .iter()
        .any(|link| link["source"] == 75.into()
            && link["target"] == 74.into()
            && link["field"] == 27.into()));
    tester
        .timer
        .log("after running the script, we have the correct 5 authority-authority links");

    Ok(())
}
