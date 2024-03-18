use eg::common::targeter;
use eg::date;
use eg::editor::Editor;
use eg::result::EgResult;
use eversrf as eg;

/// Retarget all holds regardless of whether it's time.
const FULL_RETARGET: bool = true;

fn main() -> EgResult<()> {
    let ctx = eg::init::init()?;
    let client = ctx.client();
    let mut editor = Editor::new(client, ctx.idl());

    let start = date::now();
    let mut tgtr = targeter::HoldTargeter::new(&mut editor);
    tgtr.set_retarget_interval("0s"); // retarget everything
    tgtr.init()?;

    if FULL_RETARGET {
        let hold_ids = tgtr.find_holds_to_target()?;

        let mut success = 0;
        for (idx, id) in hold_ids.iter().enumerate() {
            let ctx = tgtr.target_hold(*id, None)?;
            if ctx.success() {
                success += 1;
            }

            if idx % 20 == 0 {
                println!("Targeted {idx} so far");
            }
        }

        let duration = date::now() - start;
        println!(
            "Target batch duration: {}.{}",
            duration.num_seconds(),
            duration.num_milliseconds()
        );

        println!(
            "Finished targeting {} holds; success count = {success}",
            hold_ids.len()
        );
    } else {
        // Retarget some holds.
        for hold_id in 1..10 {
            let ctx = tgtr.target_hold(hold_id, None)?;
            println!("{hold_id} target success={}", ctx.success());
        }
    }

    Ok(())
}
