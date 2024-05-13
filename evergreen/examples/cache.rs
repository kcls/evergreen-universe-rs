use eg::osrf::cache::Cache;
use evergreen as eg;

fn main() {
    let _ = eg::init().expect("Init OK");
    Cache::init_cache("global").expect("Cache Connected");

    let blob = eg::hash! {
        "key1": [1, 2, 3],
        "key2": "blargle",
    };

    Cache::set_global("funstuff", blob).expect("Set OK");

    println!(
        "{}",
        Cache::get_global("funstuff")
            .expect("Get OK")
            .expect("Has Value")
            .dump()
    );

    Cache::del_global("funstuff").expect("Del OK");

    assert_eq!(Cache::get_global("funstuff").expect("Get OK"), None);

    // We have not initialized the anon cache, so error.
    assert!(Cache::get_anon("foo").is_err());
}
