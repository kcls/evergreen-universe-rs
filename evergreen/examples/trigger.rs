use evergreen as eg;
use eg::common::trigger;

fn main() -> eg::EgResult<()> {
    let ctx = eg::init::init()?;
    let mut editor = eg::Editor::new(ctx.client(), ctx.idl());

	let filter = json::object! {
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

    trigger::create_passive_events_for_def(
        &mut editor,
        1, // 7-day overdue email stock
        "circ_lib",
        Some(filter)
    )?;

    editor.rollback()
}
