//! Standing penalty utility functions
use crate::util;
use crate::common::trigger;
use crate::editor::Editor;
use json::JsonValue;

// Shortcut for unckecked int conversions for values that are known good.
// We coul compare JsonValue's directly, but there's a chance a number may be
// transferred as a JSON String, so turn them into numbers for conformity.
fn number(v: &JsonValue) -> i64 {
    util::json_int(v).unwrap()
}

pub fn calculate_penalties(
    editor: &mut Editor,
    user_id: i64,
    context_org: i64,
    only_penalties: &[&str]
) -> Result<(), String> {

    let query = json::object! {
        from: [
            "actor.calculate_system_penalties",
            user_id, context_org
        ]
    };

    // The DB func returns existing penalties and penalties the user
    // should have at the context org unit.
    let penalties = editor.json_query(query)?;

    let penalties = trim_to_wanted_penalties(editor, only_penalties, penalties)?;

    let mut existing_penalties: Vec<&JsonValue> =
        penalties.iter().filter(|p| !p["id"].is_null()).collect();

    let wanted_penalties: Vec<&JsonValue> =
        penalties.iter().filter(|p| p["id"].is_null()).collect();

    let mut trigger_events: Vec<(String, JsonValue, i64)> = Vec::new();

    for pen_hash in wanted_penalties {
        let org_unit = number(&pen_hash["org_unit"]);
        let penalty = number(&pen_hash["standing_penalty"]);

        // Do we have this penalty already?
        let existing = existing_penalties.iter()
        .filter(|p| {
            let e_org_unit = number(&p["org_unit"]);
            let e_penalty = number(&p["standing_penalty"]);
            org_unit == e_org_unit && penalty == e_penalty
        })
        .next();

        if let Some(epen) = existing {
            // We already have this penalty.  Remove it from the set of
            // existing penalties so it's not deleted in the subsequent loop.
            let id = number(&epen["id"]);

            existing_penalties = existing_penalties
                .iter()
                .filter(|p| number(&p["id"]) != id)
                .map(|p| *p) // &&JsonValue
                .collect();

        } else {
            // This is a new penalty.  Create it.
            let new_pen = editor.idl().create_from("ausp", pen_hash.clone())?;
            editor.create(&new_pen)?;

            // Track new penalties so we can fire related A/T events.
            let csp_id = pen_hash["standing_penalty"].clone();

            let csp = editor.retrieve("csp", csp_id)?
                .ok_or(format!("DB returned an invalid csp id??"))?;

            let evt_name = format!("penalty.{}", csp["name"]);
            trigger_events.push((evt_name, new_pen, context_org));
        }
    }

    // Delete applied penalties that are no longer wanted.
    for pen_hash in existing_penalties {
        let del_pen = editor.idl().create_from("ausp", pen_hash.clone())?;
        editor.delete(&del_pen)?;
    }

    for events in trigger_events {
        trigger::create_events_for_hook(
            editor.client_mut(),
            &events.0,  // hook name
            &events.1,  // penalty object
            events.2,   // org unit ID
            None,       // granularity
            None,       // user data
            false       // block until complete
        )?;
    }

    Ok(())
}

fn trim_to_wanted_penalties(
    editor: &mut Editor,
    only_penalties: &[&str],
    found_penalties: Vec<JsonValue>
) -> Result<Vec<JsonValue>, String> {

    if only_penalties.len() == 0 {
        return Ok(found_penalties);
    }

    // TODO

    Ok(found_penalties)
}

