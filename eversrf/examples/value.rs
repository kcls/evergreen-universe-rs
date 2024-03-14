use eg::EgResult;
use eg::EgValue;
use eversrf as eg;

pub fn main() -> EgResult<()> {
    let ctx = eg::init::init()?;

    let v = ctx
        .client()
        .send_recv_one(
            "opensrf.settings",
            "opensrf.system.echo",
            eg::hash! {"water":"baloon"},
        )?
        .expect("Has Response");

    println!("value is {v:?}");

    let mut v = eg::hash! {
        "shortname": "BR1",
        "name": "Branch 1",
        "id": 4,
        "foo": EgValue::Null,
    };

    println!("v = {v:?}");
    v.scrub_hash_nulls();
    println!("v = {v:?}");

    let v = EgValue::bless(v, "aou").expect("Sane Object");

    println!("value is {v}");

    let v = ctx
        .client()
        .send_recv_one("opensrf.settings", "opensrf.system.echo", v)?
        .expect("Has Response");

    println!("value is {v}");

    let mut list = eg::array!["1", 78, true, EgValue::Null, eg::hash! {"water":"cannon"}];

    println!("contains 1 = {}", list.contains("1"));
    println!("contains 78 = {}", list.contains(78));
    println!("contains 79 = {}", list.contains(79));
    println!("contains true = {}", list.contains(true));
    println!("contains EgValue::Null = {}", list.contains(EgValue::Null));

    list[20] = eg::hash! {"foo":"baz"};

    println!("LIST is {list:?}");

    Ok(())
}
