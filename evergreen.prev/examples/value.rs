use evergreen as eg;
use eg::{idl, EgValue, EgResult};
use json;

fn main() {
    let ctx = eg::init::init().expect("init");

    let v = json::object! {
        "_classname": "aou",
        "id":123,
        "shortname": "AB",
    };

    let mut y: EgValue = v.into();

    println!("{y}");

    for key in y.keys() {
        println!("KEY IS {key}");
    }

    for (k, v) in y.entries() {
        println!("KEY IS {k} and value = {v}");
    }

    for (k, v) in y.entries_mut() {
        if k == "shortname" {
            *v = EgValue::from(json::from("asdfasffds"));
        }
    }

    for (k, v) in y.entries() {
        println!("KEY IS {k} and value = {v}");
    }
}
