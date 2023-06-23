use evergreen as eg;

const CBT_NAME: &str = "open-ils.rs-store-test";

fn main() -> Result<(), String> {
    let ctx = eg::init::init().or_else(|e| Err(format!("Cannot init: {e}")))?;

    let mut ses = ctx.client().session("open-ils.rs-store");

    ses.connect()?;

    let mut req = ses.request("open-ils.rs-store.transaction.begin", None)?;
    req.recv(10)?
        .expect("transaction.begin should return a value");

    let mut cbt = ctx.idl().create("cbt").expect("'cbt' is an IDL object");
    cbt["name"] = json::from(CBT_NAME);
    cbt["owner"] = json::from(1);

    req = ses.request("open-ils.rs-store.direct.config.billing_type.create", cbt)?;
    cbt = req
        .recv(10)?
        .expect(".create should return the created object");

    println!("Created: {}", cbt.dump());

    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.retrieve",
        cbt["id"].clone(),
    )?;
    cbt = req.recv(10)?.expect("retrieve should return a value");

    println!("Retrieve found: {}", cbt.dump());

    let query = json::object! {name: CBT_NAME};
    req = ses.request("open-ils.rs-store.direct.config.billing_type.search", query)?;
    cbt = req.recv(10)?.expect("search should return a value");

    println!("Search found: {}", cbt.dump());

    cbt["default_price"] = json::from(2.25);
    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.update",
        cbt.clone(),
    )?;
    let resp = req.recv(10)?.expect("update should return a value");

    // 1 row should be affected
    if eg::util::json_int(&resp)? != 1 {
        panic!("Update failed: resp={:?}", resp);
    }

    println!("Update succeeded");

    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.delete",
        cbt["id"].clone(),
    )?;
    let resp = req.recv(10)?.expect("delete should return a value");

    // 1 row should be affected
    if eg::util::json_int(&resp)? != 1 {
        panic!("Delete failed: resp={:?}", resp);
    }

    println!("Delete succeeded");

    // Roll it back
    let mut req = ses.request("open-ils.rs-store.transaction.rollback", None)?;
    req.recv(10)?
        .expect("transaction.rollback should return a value");

    ses.disconnect()?; // this will also cause a rollback

    Ok(())
}
