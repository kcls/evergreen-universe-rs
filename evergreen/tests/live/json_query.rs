use crate::util;
use eg::common::jq::JsonQueryCompiler;
use eg::result::EgResult;
use evergreen as eg;
use json::JsonValue;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    let mut jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.set_locale("en-US").expect("set locale");

    let query = json::object! {
        "select": {
            "acp": "*",
            "acn": ["label", "owning_lib"],
            "bre": "editor",
            "acpl": "name",
            "ccs": [{
                "column": "name",
                "alias": "status_label",
                "transform": "uppercase",
                "params": [1, 2, 3], // TESTING
            }]
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
                "holdable": "t",
                "circ_lib": {"not in": [1, 2, 3]},
                "-or": [
                    {"mint_condition": true},
                    {"deposit": false}
                ]
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

    println!("\nPARRAMS: {}\n", jq_compiler.param_values().dump());

    Ok(())
}
