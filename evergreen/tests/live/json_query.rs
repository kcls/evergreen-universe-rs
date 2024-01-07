use crate::util;
use eg::idldb;
use eg::constants as C;
use eg::result::EgResult;
use eg::db::DatabaseConnection;
use eg::idldb::{IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
use evergreen as eg;
use json;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {

    // Not connecting the DB here.  Not needed.  At least, not yet.
    let db = DatabaseConnection::builder().build().into_shared();

    let mut translator = Translator::new(tester.ctx.idl().clone(), db.clone());

    let query = json::object! {
        "select": {"aou": ["id"], "aout": ["depth"]},
        "from": {
            "aou": "aout"
        }
    };

    let jq = translator.compile_json_query(&query);

    println!("JQ = {jq:?}");

    Ok(())
}

