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

    jq_compiler.compile(&query)?;
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    let query = json::object! {"order_by":[{"field":"id","class":"mfr"}],"select":{"mfr":["tag","value"]},"from":"mfr","where":{"record":"71","-or":[{"-and":[{"tag":"020"},{"subfield":"a"}]},{"-and":[{"tag":"022"},{"subfield":"a"}]},{"-and":[{"tag":"024"},{"subfield":"a"},{"ind1":1}]}]}};

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.compile(&query)?;
    println!("\n{}\n", jq_compiler.debug_query_kludge());

    let query = json::object! {"where":{"+ahr":{"-or":[{"-and":{"target":{"in":{"select":{"acp":["id"]},"from":{"acp":{"acn":{"field":"id","fkey":"call_number","join":{"bre":{"filter":{"id":32},"fkey":"record","field":"id"}}}}}}},"hold_type":["C","F","R"]}},{"-and":{"target":{"in":{"select":{"acn":["id"]},"from":{"acn":{"bre":{"field":"id","filter":{"id":32},"fkey":"record"}}}}},"hold_type":"V"}},{"-and":{"target":{"in":{"select":{"bmp":["id"]},"from":{"bmp":{"bre":{"fkey":"record","filter":{"id":32},"field":"id"}}}}},"hold_type":"P"}},{"-and":{"target":32,"hold_type":"T"}}],"cancel_time":null,"fulfillment_time":null}},"select":{"ahr":[{"column":"id","alias":"count","transform":"count"}]},"from":"ahr"};

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.compile(&query)?;

    assert_eq!(
        jq_compiler.debug_query_kludge(),
        r#"SELECT count("ahr".id) AS "count" FROM action.hold_request AS "ahr" WHERE (((("ahr".target IN (SELECT "acp".id FROM asset.copy AS "acp" INNER JOIN asset.call_number AS "acn" ON ("acn".id = "acp".call_number) INNER JOIN biblio.record_entry AS "bre" ON ("bre".id = "acn".record AND "bre".id = 32) WHERE TRUE) AND "ahr".hold_type IN ('C', 'F', 'R'))) OR (("ahr".target IN (SELECT "acn".id FROM asset.call_number AS "acn" INNER JOIN biblio.record_entry AS "bre" ON ("bre".id = "acn".record AND "bre".id = 32) WHERE TRUE) AND "ahr".hold_type = 'V')) OR (("ahr".target IN (SELECT "bmp".id FROM biblio.monograph_part AS "bmp" INNER JOIN biblio.record_entry AS "bre" ON ("bre".id = "bmp".record AND "bre".id = 32) WHERE TRUE) AND "ahr".hold_type = 'P')) OR (("ahr".target = '32' AND "ahr".hold_type = 'T'))) AND "ahr".cancel_time IS NULL AND "ahr".fulfillment_time IS NULL)"#
    );

    let query = json::object! {
        "select": {"bre": {"exclude": ["marc", "vis_attr_vector"]}},
        "from": "bre",
        "where": {"+bre":{"id": {"between": [1, 10]}}}
    };

    jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.compile(&query)?;

    println!("EXCLUDE\n{}", jq_compiler.debug_query_kludge());

    Ok(())
}
