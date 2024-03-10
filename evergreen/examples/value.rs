use evergreen as eg;
use eg::{idl, EgValue, EgResult};
use json;

fn main() {
    let ctx = eg::init::init().expect("init");

    let v = json::object! {
        "_classname": "aou",
        "hello": ["yes"]
    };


    let y: EgValue = v.into();

    println!("y={y:?}");
}
