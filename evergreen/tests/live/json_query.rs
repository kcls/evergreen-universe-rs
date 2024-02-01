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
                //"params": [1, 2, 3], // TESTING
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
                ],
                "id": {
                    "not in": {
                        "select": {"acp": ["id"]},
                        "from": "acp",
                        "where": {
                            "editor": {"<>": 1}
                        }
                    }
                }
            },
            "+acpm": {
                "target_copy": JsonValue::Null
            },
            "+acn": {
                "-or": [
                   {"label": {"between": ["Hello", "Goodbye"]}},
                   {"label": {"<>": "SUP"}}
                ]
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

    println!("\n{}\n", jq_compiler.query_string().expect("CREATE SQL"));
    println!("\n{}\n", jq_compiler.debug_params());
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    let query = json::object! {"from": ["asset.record_has_holdable_copy", 32, "something"]};

    jq_compiler.compile(&query)?;

    println!("\n{}\n", jq_compiler.query_string().expect("CREATE SQL"));
    println!("\n{}\n", jq_compiler.debug_params());
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    let query = json::object! {
        "select": {
            "acp": [
                "circ_lib",
                "call_number",
                {"column": "holdable", "aggregate": true, "transform": "count"}
            ]
        },
        "from": {"acp": "acn"},
        "where": {
            "+acn": {
                "label": {
                    ">=": {
                        "transform": "oils_text_as_bytea",
                        "value": ["oils_text_as_bytea", "ABC"]
                    }
                }
            }
        }
    };

    jq_compiler.compile(&query)?;

    println!("\n{}\n", jq_compiler.query_string().expect("CREATE SQL"));
    println!("\n{}\n", jq_compiler.debug_params());
    println!("\n{:?}\n", jq_compiler.query_params());
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    let query = json::object! {"union": [{"select":{"au":["id"]},"from": "au"}, {"select":{"aou":["id"]},"from":"aou"}], "all": true, "alias":"fooooo"};

    jq_compiler.compile(&query)?;

    println!("\n{}\n", jq_compiler.query_string().expect("CREATE SQL"));
    println!("\n{}\n", jq_compiler.debug_params());
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    let query = json::object! {
        "select": {"au": ["id"], "aurs": ["usr"]},
        "from": {"au": "aurs"}
    };

    jq_compiler.compile(&query)?;

    //println!("\n{}\n", jq_compiler.query_string().expect("CREATE SQL"));
    //println!("\n{}\n", jq_compiler.debug_params());
    //println!("\n{}\n", jq_compiler.debug_query_kludge());

    Ok(())
}
