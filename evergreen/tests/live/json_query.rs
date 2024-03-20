//! Json Query Unit Tests
//! NOTE Field names may change or be added that could affect these tests.
//! Similarly, hash iteration order is not guaranteed.  Testing
//! generated SQL is likely to be fragile.
use crate::util;
use eg::common::jq::JsonQueryCompiler;
use eg::result::EgResult;
use evergreen as eg;

pub fn run_live_tests(tester: &mut util::Tester) -> EgResult<()> {

    let query = eg::hash! {"where":{"+ahr":{"-or":[{"-and":{"target":{"in":{"select":{"acp":["id"]},"from":{"acp":{"acn":{"field":"id","fkey":"call_number","join":{"bre":{"filter":{"id":32},"fkey":"record","field":"id"}}}}}}},"hold_type":["C","F","R"]}},{"-and":{"target":{"in":{"select":{"acn":["id"]},"from":{"acn":{"bre":{"field":"id","filter":{"id":32},"fkey":"record"}}}}},"hold_type":"V"}},{"-and":{"target":{"in":{"select":{"bmp":["id"]},"from":{"bmp":{"bre":{"fkey":"record","filter":{"id":32},"field":"id"}}}}},"hold_type":"P"}},{"-and":{"target":32,"hold_type":"T"}}],"cancel_time":null,"fulfillment_time":null}},"select":{"ahr":[{"column":"id","alias":"count","transform":"count"}]},"from":"ahr"};

    let mut jq_compiler = JsonQueryCompiler::new(tester.ctx.idl().clone());
    jq_compiler.compile(&query)?;

    // Only sources on the root "from" are represented in the sources list
    // for the root compiler.
    assert_eq!(jq_compiler.sources()[0].classname(), "ahr");

    Ok(())
}
