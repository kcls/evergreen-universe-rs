use eg::cache;
use eversrf as eg;

fn main() {
    // Standard setup + connect routines.
    let ctx = eg::init::init().expect("Init OK");
    let hsettings = ctx.host_settings().unwrap();
    let mut cache = cache::Cache::init(hsettings.clone()).expect("Cache Connected");

    let blob = eg::hash! {
        "key1": [1, 2, 3],
        "key2": "blargle",
    };

    cache.set("funstuff", blob, None).expect("Set OK");

    println!(
        "{}",
        cache
            .get("funstuff")
            .expect("Get OK")
            .expect("Has Value")
            .dump()
    );

    cache.del("funstuff").expect("Del OK");

    assert_eq!(cache.get("funstuff").expect("Get OK"), None);

    assert!(cache.set_active_type("anon").is_ok());

    assert!(cache.set_active_type("asdjasfjklsadkj").is_err());
}
