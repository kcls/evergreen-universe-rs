use eg::common::targeter;
use eg::date;
use eg::editor::Editor;
use eg::result::EgResult;
use evergreen as eg;
use json::JsonValue;

/// Retarget all holds regardless of whether it's time.
const FULL_RETARGET: bool = false;

fn main() -> EgResult<()> {
    let ctx = eg::init::init()?;
    let client = ctx.client();
    let mut editor = Editor::new(client, ctx.idl());

    let hold_ids = editor.json_query(json::object! {
        "select": {"ahr": ["id"]},
        "from": "ahr",
        "where": {
            "fulfillment_time": JsonValue::Null,
            "cancel_time": JsonValue::Null,
            "frozen": "f"
        }
    })?;

    println!("We have {} active holds", hold_ids.len());

    let mut tgtr = targeter::HoldTargeter::new(editor.clone());

    if FULL_RETARGET {
        let mut counter = 0;
        let mut success = 0;
        let start = date::now();

        for hold_id in hold_ids.iter() {
            let id = eg::util::json_int(&hold_id["id"])?;
            let ctx = tgtr.target_hold(id, None)?;
            if ctx.success() {
                success += 1;
            }

            counter += 1;
            if counter % 20 == 0 {
                println!("Targeted {counter} so far");
            }
        }

        let duration = date::now() - start;
        println!(
            "Target batch duration: {}.{}",
            duration.num_seconds(),
            duration.num_milliseconds()
        );

        println!("Finished targeting {counter} holds; success count = {success}");
    } else {
        // Retarget some holds.
        for hold_id in 1..10 {
            let ctx = tgtr.target_hold(hold_id, None)?;
            println!("{hold_id} target success={}", ctx.success());
        }
    }

    Ok(())
}
