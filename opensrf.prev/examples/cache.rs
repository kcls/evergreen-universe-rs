use json;
use opensrf::cache;
use opensrf::sclient;
use opensrf::Client;

fn main() {
    // Standard setup + connect routines.
    let conf = opensrf::init::init().expect("Init OK");
    let client = Client::connect(conf.into_shared()).expect("Client connect OK");

    let host_settings = sclient::SettingsClient::get_host_settings(&client, false)
        .expect("Fetched Host Settings")
        .into_shared();

    let mut cache = cache::Cache::init(host_settings).expect("Cache Connected");

    let blob = json::object! {
        "key1": [1, 2, 3],
        "key2": "blargle",
    };

    cache.set("funstuff", &blob, None).expect("Set OK");

    println!(
        "{}",
        cache.get("funstuff").expect("Get OK").expect("Has Value")
    );

    cache.del("funstuff").expect("Del OK");

    assert_eq!(cache.get("funstuff").expect("Get OK"), None);

    assert!(cache.set_active_type("anon").is_ok());

    assert!(cache.set_active_type("asdjasfjklsadkj").is_err());
}
