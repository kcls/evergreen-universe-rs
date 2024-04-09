use evergreen as eg;
mod circ;
mod json_query;
mod store;
mod util;

/// Set to 'ignored' by default since it requires a running system
/// and creates data.
///
/// To run:
/// cargo test --package evergreen -- --ignored
///
/// Or more specifically:
/// cargo test --package evergreen --test live -- --ignored
///
/// To also see timing:
/// cargo test --package evergreen --test live -- --ignored --nocapture
#[test]
#[ignore]
fn main() -> eg::EgResult<()> {
    let ctx = eg::init()?;
    let editor = eg::Editor::new(ctx.client());

    let mut tester = util::Tester {
        ctx,
        editor,
        samples: eg::samples::SampleData::new(),
        timer: util::Timer::new(),
    };

    circ::run_live_tests(&mut tester)?;

    // open-ils.rs-store tester
    //store::run_live_tests(&mut tester)?;

    json_query::run_live_tests(&mut tester)?;

    Ok(())
}
