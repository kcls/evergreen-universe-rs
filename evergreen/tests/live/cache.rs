use crate::util;
use eg::osrf::cache::Cache;
use eg::EgResult;
use evergreen as eg;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    tester.timer.start();

    Cache::init_cache("global").expect("Cache Connected");

    let blob = eg::hash! {
        "key1": [1, 2, 3],
        "key2": "blargle",
    };

    Cache::set_global("funstuff", blob).expect("Set OK");

    tester.timer.log("Cached Something");

    let blargle = Cache::get_global("funstuff")?;

    assert!(blargle.is_some());

    assert_eq!(blargle.unwrap()["key2"].as_str(), Some("blargle"));

    tester.timer.log("Fetched Something");

    Cache::del_global("funstuff").expect("Del OK");

    assert_eq!(Cache::get_global("funstuff").expect("Get OK"), None);

    tester.timer.log("Deleted Something");

    // We have not initialized the anon cache, so this should produce an error.
    assert!(Cache::get_anon("foo").is_err());

    tester.timer.log("Confirmed Not Initialized");

    Ok(())
}
