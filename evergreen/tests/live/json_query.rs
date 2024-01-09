use crate::util;
use eg::constants as C;
use eg::db::DatabaseConnection;
use eg::idldb;
use eg::idldb::{IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
use eg::result::EgResult;
use evergreen as eg;
use json::JsonValue;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    // Not connecting the DB here.  Not needed.  At least, not yet.
    let db = DatabaseConnection::builder().build().into_shared();

    let mut translator = Translator::new(tester.ctx.idl().clone(), db.clone());

    let query = json::object! {
        "select": {"acp": ["id", "circ_lib"]},
        "from": {
            "acp": {
                "acn": {
                    "field": "id",
                    "fkey": "call_number",
                    "filter": {"record": 12345}
                },
                "acpl": {
                    "field": "id",
                    "filter": {"holdable": "t", "deleted": "f"},
                    "fkey": "location"
                },
                "ccs": {
                    "field": "id",
                    "filter": {"holdable": "t"},
                    "fkey": "status"
                },
                "acpm": {
                    "field": "target_copy",
                    "type": "left"
                }
            }
        },
        "where": {
            "+acp": {
                "circulate": "t",
                "deleted": "f",
                "holdable": "t"
            },
            "+acpm": {
                "target_copy": JsonValue::Null
            }
        },
        "order_by": [{
            "class": "acp",
            "field": "circ_lib",
            "compare": {"!=" : {"+acn": "owning_lib"}}
        }, {
            "class": "acpl",
            "field": "name"
        }]
    };

    let jq = translator.compile_json_query(&query)?;

    println!("JQ = {jq:?}");

    Ok(())
}
