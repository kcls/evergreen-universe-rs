//! Standing penalty utility functions
use crate as eg;
use eg::common::settings::Settings;
use eg::common::trigger;
use eg::editor::Editor;
use eg::result::EgResult;
use eg::EgValue;

// Shortcut for unckecked int conversions for values that are known good.
// We coul compare EgValue's directly, but there's a chance a number may be
// transferred as a JSON String, so turn them into numbers for conformity.
fn number(v: &EgValue) -> i64 {
    v.int().expect("Has Number")
}

pub fn calculate_penalties(
    editor: &mut Editor,
    user_id: i64,
    context_org: i64,
    only_penalties: Option<&Vec<EgValue>>,
) -> EgResult<()> {
    let query = eg::hash! {
        from: [
            "actor.calculate_system_penalties",
            user_id, context_org
        ]
    };

    // The DB func returns existing penalties and penalties the user
    // should have at the context org unit.
    let penalties = editor.json_query(query)?;

    let penalties = trim_to_wanted_penalties(editor, context_org, only_penalties, penalties)?;

    if penalties.is_empty() {
        // Nothing to change.
        return Ok(());
    }

    // Applied penalties have a DB ID.
    let mut existing_penalties: Vec<&EgValue> =
        penalties.iter().filter(|p| !p["id"].is_null()).collect();

    // Penalties that should be applied do not have a DB ID.
    let wanted_penalties: Vec<&EgValue> = penalties.iter().filter(|p| p["id"].is_null()).collect();

    let mut trigger_events: Vec<(String, EgValue, i64)> = Vec::new();

    for pen_hash in wanted_penalties {
        let org_unit = number(&pen_hash["org_unit"]);
        let penalty = number(&pen_hash["standing_penalty"]);

        // Do we have this penalty already?
        let existing = existing_penalties.iter().find(|p| {
            let e_org_unit = number(&p["org_unit"]);
            let e_penalty = number(&p["standing_penalty"]);
            org_unit == e_org_unit && penalty == e_penalty
        });

        if let Some(epen) = existing {
            // We already have this penalty.  Remove it from the set of
            // existing penalties so it's not deleted in the subsequent loop.
            let id = number(&epen["id"]);

            existing_penalties.retain(|p| number(&p["id"]) != id);
        } else {
            // This is a new penalty.  Create it.
            let new_pen = EgValue::create("ausp", pen_hash.clone())?;
            let new_pen = editor.create(new_pen)?;

            // Track new penalties so we can fire related A/T events.
            let csp_id = pen_hash["standing_penalty"].clone();

            let csp = editor
                .retrieve("csp", csp_id)?
                .ok_or_else(|| editor.die_event())?;

            let evt_name = format!("penalty.{}", csp["name"]);
            trigger_events.push((evt_name, new_pen, context_org));
        }
    }

    // Delete applied penalties that are no longer wanted.
    for pen_hash in existing_penalties {
        let del_pen = EgValue::create("ausp", pen_hash.clone())?;
        editor.delete(del_pen)?;
    }

    for events in trigger_events {
        trigger::create_events_for_object(
            editor, &events.0, // hook name
            &events.1, // penalty object
            events.2,  // org unit ID
            None,      // granularity
            None,      // user data
            false,     // ignore opt-in
        )?;
    }

    Ok(())
}

/// If the caller specifies a limited set of penalties to process,
/// trim the calculated penalty set to those whose penalty types
/// match the types specified in only_penalties.
fn trim_to_wanted_penalties(
    editor: &mut Editor,
    context_org: i64,
    only_penalties: Option<&Vec<EgValue>>,
    all_penalties: Vec<EgValue>,
) -> EgResult<Vec<EgValue>> {
    let only_penalties = match only_penalties {
        Some(op) => op,
        None => return Ok(all_penalties),
    };

    if only_penalties.is_empty() {
        return Ok(all_penalties);
    }

    // The set to limit may be specified as penalty type IDs or names.
    let mut penalty_id_list: Vec<EgValue> = Vec::new();
    let mut penalty_name_list: Vec<EgValue> = Vec::new();

    for pen in only_penalties {
        if pen.is_number() {
            penalty_id_list.push(pen.clone());
        } else if pen.is_string() {
            penalty_name_list.push(pen.clone());
        }
    }

    if penalty_name_list.is_empty() {
        // Get penalty type IDs from their names.
        let query = eg::hash! {"name": {"in": penalty_name_list.clone()}};
        let penalty_types = editor.search("csp", query)?;
        for ptype in penalty_types {
            penalty_id_list.push(ptype["id"].clone());
        }

        // See if any of the named penalties have local overrides.
        // If so, process them as well.
        let mut settings = Settings::new(editor);
        settings.set_org_id(context_org);

        let names: Vec<String> = penalty_name_list
            .iter()
            .map(|n| format!("circ.custom_penalty_override.{n}"))
            .collect();

        let names: Vec<&str> = names.iter().map(|n| n.as_str()).collect();

        settings.fetch_values(names.as_slice())?; // precache

        for name in names.iter() {
            let pen_id = settings.get_value(name)?;
            // Verify the org unit setting value is numerifiable.
            if let Some(n) = pen_id.as_int() {
                penalty_id_list.push(EgValue::from(n));
            }
        }
    }

    // Trim our list of penalties to those whose IDs we have identified
    // the caller is interested in.
    let mut final_penalties: Vec<EgValue> = Vec::new();
    for pen in all_penalties {
        if penalty_id_list
            .iter()
            .any(|id| id == &pen["standing_penalty"])
        {
            final_penalties.push(pen);
        }
    }

    Ok(final_penalties)
}
