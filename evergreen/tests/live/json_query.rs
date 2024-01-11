use crate::util;
use eg::common::jq::JsonQueryCompiler;
use eg::result::EgResult;
use evergreen as eg;
use json::JsonValue;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    let mut jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());

    let query = json::object! {
        "select": {
            "acp": "*",
            "acn": ["label", "owning_lib"],
            "bre": "editor"
        },
        "from": {
            "acp": {
                "acn": {
                    "field": "id",
                    "fkey": "call_number",
                    "filter": {"record": 12345},
                    "join": {
                        "bre": {
                            "fkey": "record"
                        }
                    }
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

    println!("\n{}\n", query.dump());

    jq_compiler.compile(&query)?;

    //println!("JQ = {jq_compiler:?}");

    println!(
        "\n{}\n",
        jq_compiler.query_string().expect("SHOULD HAVE SQL")
    );

    Ok(())
}
