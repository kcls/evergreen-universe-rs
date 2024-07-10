use crate::util;
use eg::result::EgResult;
use eg::EgValue;
use evergreen as eg;

const CBT_NAME: &str = "open-ils.rs-store-test";

#[allow(dead_code)]
pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    tester.timer.start();

    let mut ses = tester.client.session("open-ils.rs-store");

    // Required for executing a batch of commands in a transaction.
    ses.connect()?;
    tester.timer.log("Connected");

    // Start a transaction
    let mut req = ses.request("open-ils.rs-store.transaction.begin", None)?;
    req.recv()?
        .expect("transaction.begin should return a value");
    tester.timer.log("Transaction Started");

    // Create a new billing type row.

    let mut cbt = EgValue::stub("cbt")?;
    cbt["name"] = EgValue::from(CBT_NAME);
    cbt["owner"] = EgValue::from(1);

    req = ses.request("open-ils.rs-store.direct.config.billing_type.create", cbt)?;
    cbt = req
        .recv()?
        .expect(".create should return the created object");

    //println!("Created: {}", cbt.dump());

    assert!(cbt["id"].int().is_ok());
    tester.timer.log("Billing Type Created");

    // Fetch the new billing type
    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.retrieve",
        cbt["id"].clone(),
    )?;
    cbt = req.recv()?.expect("retrieve should return a value");

    //println!("Retrieve found: {}", cbt.dump());

    assert!(cbt["id"].int().is_ok());
    tester.timer.log("Retrieved Billing Type");

    // Search for the new billing type by name
    let query = eg::hash! {"name": CBT_NAME};
    req = ses.request("open-ils.rs-store.direct.config.billing_type.search", query)?;
    cbt = req.recv()?.expect("search should return a value");

    //println!("Search found: {}", cbt.dump());

    assert!(cbt["id"].int().is_ok());
    assert!(cbt["name"].str().is_ok());
    assert_eq!(cbt["name"].str()?, CBT_NAME);
    tester.timer.log("Search Found Billing Type");

    // Update the billing type
    cbt["default_price"] = EgValue::from(2.25);
    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.update",
        cbt.clone(),
    )?;
    let resp = req.recv()?.expect("update should return a value");

    // 1 row should be affected
    assert!(resp.int().is_ok());
    assert_eq!(resp.int()?, 1);

    tester.timer.log("Update Succeeded");

    // Delete the new billing type
    req = ses.request(
        "open-ils.rs-store.direct.config.billing_type.delete",
        cbt["id"].clone(),
    )?;
    let resp = req.recv()?.expect("delete should return a value");

    // 1 row should be affected
    assert!(resp.int().is_ok());
    assert_eq!(resp.int()?, 1);

    tester.timer.log("Delete Succeeded");

    // Roll it back
    let mut req = ses.request("open-ils.rs-store.transaction.rollback", None)?;
    req.recv()?
        .expect("transaction.rollback should return a value");
    tester.timer.log("Transaction Rolled Back");

    ses.disconnect()?; // this would also cause a rollback
    tester.timer.log("Disconnected");

    Ok(())
}
