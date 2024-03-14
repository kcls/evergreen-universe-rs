use eg::EgResult;
use eg::EgValue;
use eversrf as eg;

pub fn main() -> EgResult<()> {
    // Connect and load the IDLj
    let ctx = eg::init::init()?;

    let v = ctx
        .client()
        .send_recv_one(
            "opensrf.settings",
            "opensrf.system.echo",
            eg::hash! {"water": "baloon"},
        )?
        .expect("Has Response");

    println!("value is {v:?}");

    let mut v = eg::hash! {
        "shortname": "BR1",
        "name": "Branch 1",
        "id": 4,
        "foo": eg::NULL,
    };

    println!("v = {v:?}");

    // Fails on invalid field "foo"
    assert!(v.bless("aou").is_err());

    // remove "foo"
    v.scrub_hash_nulls();

    println!("v = {v:?}");

    v.bless("aou")?;

    println!("value is {v}");

    let v = ctx
        .client()
        .send_recv_one("opensrf.settings", "opensrf.system.echo", v)?
        .expect("Has Response");

    println!("value is {v}");

    let mut list = eg::array!["1", 78, true, eg::NULL, eg::hash! {"water":"cannon"}];

    assert!(list.contains("1"));
    assert!(list.contains(78));
    assert!(!list.contains(79));
    assert!(list.contains(true));
    assert!(list.contains(eg::NULL));

    // This expands the array to accomodate the value.
    list[20] = eg::hash! {"foo":"baz"};

    println!("LIST is {list:?}");

    let mut v = EgValue::create(
        "aou",
        eg::hash! {"id": 1, "shortname":"AAA", "name": "HOWDYDFD"}
    )?;

    v["shortname"] = EgValue::from("HELLLO");

    println!("v = {v}");

    v.unbless()?;

    println!("v = {}", v.dump());

    Ok(())
}


