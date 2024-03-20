use eg::EgResult;
use eg::EgValue;
use evergreen as eg;

pub fn main() -> EgResult<()> {
    // Connect and load the IDL
    let ctx = eg::init::init()?;

    let mut v = eg::hash! {
        "shortname": "BR1",
        "name": "Branch 1",
        "id": 4,
        "foo": eg::NULL,
    };

    v["floogle"] = "fanagle".into();
    v["floogle"] = eg::NULL;

    println!("v = {v:?}");

    // Fails on invalid field "foo"
    assert!(v.bless("aou").is_err());

    // remove "foo"
    v.scrub_hash_nulls();

    v.bless("aou")?;

    println!("value is {v}");

    let v = ctx
        .client()
        .send_recv_one("opensrf.settings", "opensrf.system.echo", v)?
        .expect("Has Response");

    println!("shortname is {v}");
    println!("shortname is {}", v["shortname"]);

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
        eg::hash! {"id": 1, "shortname":"AAA", "name": "HOWDYDFD"},
    )?;

    //v["shortname"] = EgValue::from("HELLLO");
    v["shortname"] = "HELLLO".into();

    println!("v = {v}");

    v.unbless()?;

    println!("v = {}", v.dump());

    let v = ctx
        .client()
        .send_recv_one(
            "opensrf.settings",
            "opensrf.system.echo",
            eg::hash! {"water": "baloon"},
        )?
        .expect("Has Response");

    println!("value is {v:?}");

    Ok(())
}
