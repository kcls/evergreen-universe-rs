use crate::util;
use eg::common::auth;
use eg::osrf::cache::Cache;
use eg::EgResult;
use evergreen as eg;

/// Values from EG's opensrf.xml.example
const DEFAULT_OPAC_LOGIN_DURATION: i64 = 420;
const DEFAULT_STAFF_LOGIN_DURATION: i64 = 7200;
const DEFAULT_TEMP_LOGIN_DURATION: i64 = 300;
const DEFAULT_PERSIST_LOGIN_DURATION: i64 = 2 * 604800; // "2 weeks"
const NO_ORG_UNIT: i64 = 0;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    // Check default auth durations

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
    tester.timer.stop("Check Default OPAC Login Duration");

    tester.timer.start();
    let staff = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Staff,
    )?;
    assert_eq!(DEFAULT_STAFF_LOGIN_DURATION, staff);
    tester.timer.stop("Check Default Staff Login Duration");

    tester.timer.start();
    let temp = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Temp,
    )?;
    assert_eq!(DEFAULT_TEMP_LOGIN_DURATION, temp);
    tester.timer.stop("Check Default Temp Login Duration");

    tester.timer.start();
    let persist = auth::get_auth_duration(
        &mut tester.editor,
        NO_ORG_UNIT,
        NO_ORG_UNIT,
        &auth::LoginType::Persist,
    )?;
    assert_eq!(DEFAULT_PERSIST_LOGIN_DURATION, persist);
    tester.timer.stop("Check Default Persist Login Duration");

    tester.timer.start();
    let mut args = auth::InternalLoginArgs::new(eg::samples::AU_STAFF_ID, auth::LoginType::Staff);
    args.org_unit = Some(tester.samples.aou_id);

    let mut cache = Cache::init()?;

    let ses = auth::Session::internal_session(
        &mut tester.editor,
        &mut cache,
        &args
    )?;
    assert_eq!(ses.authtime(), staff);
    tester.timer.stop("Created Internal Session");

    Ok(())
}
