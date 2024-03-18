//! Action/Trigger main entry point.
use crate as eg;
use eg::common::org;
use eg::date;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;

pub mod event;
pub use event::{Event, EventState};
pub mod processor;
pub use processor::Processor;
mod reactor;
mod validator;

/// Create A/T events for an object and A/T hook.
pub fn create_events_for_object(
    editor: &mut Editor,
    hook: &str,
    target: &EgValue,
    org_id: i64,
    granularity: Option<&str>,
    user_data: Option<&EgValue>,
    ignore_opt_in: bool,
) -> EgResult<()> {
    let hook_obj = match editor.retrieve("ath", hook)? {
        Some(h) => h,
        None => {
            log::warn!("No such A/T hook: {hook}");
            return Ok(());
        }
    };

    let class = target
        .classname()
        .ok_or_else(|| format!("Invalid target: {target}"))?;

    if hook_obj["core_type"].as_str().unwrap() != class {
        // "key" is required.
        log::warn!("A/T hook {hook} does not match object core type: {class}");
        return Ok(());
    }

    let query = eg::hash! {
        "hook": hook,
        "active": "t",
        "owner": org::ancestors(editor, org_id)?,
    };

    let event_defs = editor.search("atevdef", query)?;

    for def in event_defs.iter() {
        create_event_for_object_and_def(
            editor,
            def,
            target,
            granularity,
            user_data,
            ignore_opt_in,
        )?;
    }

    Ok(())
}

/// Take one target and one event def and create an event if we can.
///
/// Assumes that the target is appropriate for the event def.
pub fn create_event_for_object_and_def(
    editor: &mut Editor,
    event_def: &EgValue,
    target: &EgValue,
    granularity: Option<&str>,
    user_data: Option<&EgValue>,
    ignore_opt_in: bool,
) -> EgResult<Option<EgValue>> {
    if let Some(gran) = granularity {
        // If a granularity is provided by the caller, the def
        // must a) have one and b) have one that matches.
        if let Some(def_gran) = event_def["granularity"].as_str() {
            if def_gran != gran {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }

    if !ignore_opt_in && !user_is_opted_in(editor, event_def, target)? {
        return Ok(None);
    }

    let runtime = match calc_runtime(event_def, target)? {
        Some(t) => t,
        None => return Ok(None),
    };

    let pkey = target
        .pkey_value()
        .ok_or_else(|| format!("Pkey value required"))?;

    let mut event = eg::hash! {
        "event_def": event_def["id"].clone(),
        "run_time": runtime,
    };

    event["target"] = pkey.clone();

    if let Some(udata) = user_data {
        event["user_data"] = EgValue::from(udata.dump());
    }

    event.bless("atev")?;

    Ok(Some(editor.create(event)?))
}

// Non-doc test required since this is a private function.
#[test]
fn test_calc_runtime() {
    let event_def = eg::hash! {
      "passive": "t",
      "delay_field": "due_date",
      "delay": "1 day 1 hour 5 minutes 1 second",
    };

    let target = eg::hash! {
      "due_date": "2023-08-18T23:59:59-0400"
    };

    let runtime = calc_runtime(&event_def, &target).unwrap();
    assert_eq!(runtime, Some("2023-08-20T01:05:00-0400".to_string()));
}

/// Determine the run_time value for an event.
///
/// Returns the value as an ISO string.
fn calc_runtime(event_def: &EgValue, target: &EgValue) -> EgResult<Option<String>> {
    if !event_def["passive"].boolish() {
        // Active events always run now.
        return Ok(Some(date::to_iso(&date::now_local())));
    }

    let delay_field = match event_def["delay_field"].as_str() {
        Some(d) => d,
        None => return Ok(Some(date::to_iso(&date::now_local()))),
    };

    let delay_start = match target[delay_field].as_str() {
        Some(a) => a,
        None => return Ok(None),
    };

    let delay_intvl = match event_def["delay"].as_str() {
        Some(d) => d,
        None => return Ok(None), // required field / should not happen.
    };

    let runtime = date::parse_datetime(&delay_start)?;
    let runtime = date::add_interval(runtime, &delay_intvl)?;

    Ok(Some(date::to_iso(&runtime)))
}

/// Returns true if the event def does not require opt in (i.e. everyone
/// is opted in) or it does require an opt-in and the user linked to the
/// target has the needed opt-in user setting.
fn user_is_opted_in(editor: &mut Editor, event_def: &EgValue, target: &EgValue) -> EgResult<bool> {
    let opt_in = match event_def["opt_in_setting"].as_str() {
        Some(o) => o,
        None => return Ok(true),
    };

    // If the event def requires an opt-in but defines no user field,
    // then no one is opted in.
    let usr_field = match event_def["usr_field"].as_str() {
        Some(f) => f,
        None => return Ok(false),
    };

    let user_id = target[usr_field]
        .as_int()
        .unwrap_or(target[usr_field].id_required());

    let query = eg::hash! {
        "usr": user_id,
        "name": opt_in,
        "value": "true",
    };

    Ok(editor.search("aus", query)?.len() > 0)
}

/// Create events for a passive-hook event definition, returning the
/// IDs of the created events on success.
///
/// Caller is responsible for beginning / committing the transaction.
pub fn create_passive_events_for_def(
    editor: &mut Editor,
    event_def_id: i64,
    location_field: &str,
    mut filter_op: Option<EgValue>,
) -> EgResult<Option<Vec<i64>>> {
    let flesh = eg::hash! {
        "flesh": 1,
        "flesh_fields": {
            "atevdef": ["hook"]
        }
    };

    let event_def = editor
        .retrieve_with_ops("atevdef", event_def_id, flesh)?
        .ok_or_else(|| editor.die_event())?;

    let mut filters = match filter_op.take() {
        Some(f) => f,
        None => eg::hash! {},
    };

    // Limit to targets within range of our event def.
    filters[location_field] = eg::hash! {
        "in": {
            "select": {
                "aou": [{
                    "column": "id",
                    "transform": "actor.org_unit_descendants",
                    "result_field": "id"
                }],
            },
            "from": "aou",
            "where": {"id": event_def["owner"].clone()}
        }
    };

    // Determine the date range of the items we want to target.

    let def_delay = event_def["delay"].as_str().unwrap(); // required
    let delay_dt = date::add_interval(date::now(), def_delay)?;

    let delay_filter;
    if let Some(max_delay) = event_def["max_delay"].as_str() {
        let max_delay_dt = date::add_interval(date::now(), max_delay)?;

        if max_delay_dt < delay_dt {
            delay_filter = eg::hash! {
                "between": [
                    date::to_iso(&max_delay_dt),
                    date::to_iso(&delay_dt),
                ]
            };
        } else {
            delay_filter = eg::hash! {
                "between": [
                    date::to_iso(&delay_dt),
                    date::to_iso(&max_delay_dt),
                ]
            };
        }
    } else {
        delay_filter = eg::hash! {"<=": date::to_iso(&delay_dt)};
    }

    let delay_field = event_def["delay_field"]
        .as_str()
        .ok_or_else(|| format!("Passive event defs require a delay_field"))?;

    filters[delay_field] = delay_filter;

    // Make sure we don't create events that are already represented.

    let core_type = event_def["hook"]["core_type"].as_str().unwrap(); // required
    let idl_class = editor
        .idl()
        .classes()
        .get(core_type)
        .ok_or_else(|| format!("No such IDL class: {core_type}"))?
        .clone(); // Arc; mut's

    let pkey_field = idl_class
        .pkey()
        .ok_or_else(|| format!("IDL class {core_type} has no primary key"))?;

    let mut join = eg::hash! {
        "join": {
            "atev": {
                "field": "target",
                "fkey": pkey_field,
                "type": "left",
                "filter": {"event_def": event_def_id}
            }
        }
    };

    // Some event types are repeatable depending on a repeat delay.
    if let Some(rpt_delay) = event_def["repeat_delay"].as_str() {
        let delay_dt = date::add_interval(date::now(), rpt_delay)?;

        join["join"]["atev"]["filter"] = eg::hash! {
            "start_time": {">": date::to_iso(&delay_dt)}
        }
    }

    // Skip targets where the user is not opted in.
    if let Some(usr_field) = event_def["usr_field"].as_str() {
        if let Some(setting) = event_def["opt_in_setting"].as_str() {
            // {"+circ": "usr"}
            let mut user_matches = eg::hash! {};
            user_matches[&format!("+{core_type}")] = EgValue::from(usr_field);

            let opt_filter = eg::hash! {
                "-exists": {
                    "from": "aus",
                    "where": {
                        "name": setting,
                        "usr": {"=": user_matches},
                        "value": "true"
                    }
                }
            };

            if filters["-and"].is_array() {
                filters["-and"].push(opt_filter).expect("Is Array");
            } else {
                filters["-and"] = eg::array![opt_filter];
            }
        }
    }

    log::debug!("Event def {event_def_id} filter is: {}", filters.dump());

    editor.set_timeout(3600); // 1hr

    let targets = editor.search(core_type, filters)?;

    editor.reset_timeout();

    if targets.len() == 0 {
        log::info!("No targets found for event def {event_def_id}");
        return Ok(None);
    } else {
        log::info!(
            "Found {} targets for vent def {event_def_id}",
            targets.len()
        );
    }

    let mut result_ids = Vec::new();

    for target in targets {
        let id = target[pkey_field].to_string();

        let mut event = eg::hash! {
            "target": id,
            "event_def": event_def_id,
            "run_time": "now",
        };

        event.bless("atev")?;

        let event = editor.create(event)?;

        result_ids.push(event.id_required());
    }

    log::info!("Done creating events for event_def {event_def_id}");

    Ok(Some(result_ids))
}
