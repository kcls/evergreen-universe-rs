use eg::common::trigger;
use evergreen as eg;

fn main() -> eg::EgResult<()> {
    let client = eg::init()?;
    let mut editor = eg::Editor::new(&client);

    let filter = eg::hash! {
        "checkin_time": json::JsonValue::Null,
        "-or": [
            {"stop_fines" : ["MAXFINES", "LONGOVERDUE"]},
            {"stop_fines" : json::JsonValue::Null}
        ],
        "-not": {
            "-exists": {
                "select": {"atev" : ["target"]},
                "from": "atev",
                "where": {
                    "event_def": [236, 43],
                    "target": {"=": {"+circ": "id"}}
                }
            }
        }
    };

    editor.xact_begin()?;

    let event_ids = trigger::create_passive_events_for_def(
        &mut editor,
        1, // 7-day overdue email stock
        "circ_lib",
        Some(filter),
    )?;

    editor.commit()?;

    println!("Created events: {event_ids:?}");

    if let Some(list) = event_ids {
        if let Some(id) = list.first() {
            trigger::processor::Processor::process_event_once(&mut editor, *id)?;
        }
    }

    Ok(())
}
