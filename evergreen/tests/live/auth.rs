use crate::util;
use eg::common::auth;
use eg::osrf::cache::Cache;
use eg::EgResult;
use evergreen as eg;

/// Values from EG's opensrf.xml.example
const DEFAULT_OPAC_LOGIN_DURATION: u32 = 420;
const DEFAULT_STAFF_LOGIN_DURATION: u32 = 7200;
const DEFAULT_TEMP_LOGIN_DURATION: u32 = 300;
const DEFAULT_PERSIST_LOGIN_DURATION: u32 = 2 * 604800; // "2 weeks"
const NO_ORG_UNIT: i64 = 0;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    tester.timer.start();

    let opac = auth::get_auth_duration(
        &mut tester.editor,
        // Org unit setting lookups will occur, but this guarantees
        // we don't find any.
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Opac,
    )?;
    assert_eq!(DEFAULT_OPAC_LOGIN_DURATION, opac);
    tester.timer.log("Check Default OPAC Login Duration");

    let staff = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Staff,
    )?;
    assert_eq!(DEFAULT_STAFF_LOGIN_DURATION, staff);
    tester.timer.log("Check Default Staff Login Duration");

    let temp = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Temp,
    )?;
    assert_eq!(DEFAULT_TEMP_LOGIN_DURATION, temp);
    tester.timer.log("Check Default Temp Login Duration");

    let persist = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Persist,
    )?;
    assert_eq!(DEFAULT_PERSIST_LOGIN_DURATION, persist);
    tester.timer.log("Check Default Persist Login Duration");

    let mut args = auth::InternalLoginArgs::new(eg::samples::AU_STAFF_ID, auth::LoginType::Staff);
    args.org_unit = Some(tester.samples.aou_id);

    Cache::init_cache("global")?;

    let ses = auth::Session::internal_session(&mut tester.editor, &args)?;
    assert_eq!(ses.authtime(), staff);
    tester.timer.log("Created Internal Session");

    let ses2 = auth::Session::from_cache(ses.token())?.expect("Session Exists");
    assert_eq!(ses2.token(), ses.token());
    assert_eq!(ses2.user().id(), eg::samples::AU_STAFF_ID);
    assert_eq!(ses2.authtime(), staff);
    tester.timer.log("Retrieved valid Session from cache");

    ses2.remove()?;
    assert!(auth::Session::from_cache(ses2.token())?.is_none());
    tester.timer.log("Removed session from cache");

    Ok(())
}
