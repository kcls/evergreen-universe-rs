//! Json Query Unit Tests
//! NOTE Field names may change / be added that could affect these tests.
//! Probably the test should be based more on the data returned from
//! the queries than the than the compiled SQL.  This is a start.
use crate::util;
use eg::common::jq::JsonQueryCompiler;
use eg::result::EgResult;
use evergreen as eg;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {
    let query = json::object! {"order_by":[{"field":"id","class":"mfr"}],"select":{"mfr":["tag","value"]},"from":"mfr","where":{"record":"71","-or":[{"-and":[{"tag":"020"},{"subfield":"a"}]},{"-and":[{"tag":"022"},{"subfield":"a"}]},{"-and":[{"tag":"024"},{"subfield":"a"},{"ind1":1}]}]}};

    let mut jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.compile(&query)?;

    assert_eq!(
        jq_compiler.debug_query_kludge(),
        r#"SELECT "mfr".tag, "mfr".value FROM metabib.full_rec AS "mfr" WHERE "mfr".record = '71' AND (((("mfr".tag = '020') AND ("mfr".subfield = 'a'))) OR ((("mfr".tag = '022') AND ("mfr".subfield = 'a'))) OR ((("mfr".tag = '024') AND ("mfr".subfield = 'a') AND ("mfr".ind1 = '1')))) ORDER BY "mfr".id ASC"#
    );

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

    assert_eq!(
         jq_compiler.debug_query_kludge(),
        r#"SELECT "bre".active, "bre".create_date, "bre".creator, "bre".deleted, "bre".edit_date, "bre".editor, "bre".fingerprint, "bre".id, "bre".last_xact_id, "bre".merge_date, "bre".merged_to, "bre".owner, "bre".quality, "bre".share_depth, "bre".source, "bre".tcn_source, "bre".tcn_value FROM biblio.record_entry AS "bre" WHERE ("bre".id BETWEEN 1 AND 10)"#
    );

    Ok(())
}
