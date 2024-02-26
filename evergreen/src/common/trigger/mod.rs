//! Action/Trigger main entry point.
use crate::common::org;
use crate::date;
use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use chrono::Duration;
use json::JsonValue;

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
    target: &JsonValue,
    org_id: i64,
    granularity: Option<&str>,
    user_data: Option<&JsonValue>,
    ignore_opt_in: bool,
) -> EgResult<()> {
    let hook_obj = match editor.retrieve("ath", hook)? {
        Some(h) => h,
        None => {
            log::warn!("No such A/T hook: {hook}");
            return Ok(());
        }
    };

    let class = editor.idl().get_classname(target)?;

    if hook_obj["key"].as_str().unwrap() != class {
        // "key" is required.
        log::warn!("A/T hook {hook} does not match object core type: {class}");
        return Ok(());
    }

    let query = json::object! {
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
    event_def: &JsonValue,
    target: &JsonValue,
    granularity: Option<&str>,
    user_data: Option<&JsonValue>,
    ignore_opt_in: bool,
) -> EgResult<Option<JsonValue>> {
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

    let pkey = match editor.idl().get_pkey_value(target) {
        Some(k) => k,
        None => {
            log::warn!("Object has no pkey value: {}", target.dump());
            return Ok(None);
        }
    };

    let mut event = json::object! {
        "target": pkey,
        "event_def": event_def["id"].clone(),
        "run_time": runtime,
    };

    if let Some(udata) = user_data {
        event["user_data"] = json::from(udata.dump());
    }

    let event = editor.idl().create_from("atev", event)?;

    Ok(Some(editor.create(event)?))
}

// Non-doc test required since this is a private function.
#[test]
fn test_calc_runtime() {
    let event_def = json::object! {
      "passive": "t",
      "delay_field": "due_date",
      "delay": "1 day 1 hour 5 minutes 1 second",
    };

    let target = json::object! {
      "due_date": "2023-08-18T23:59:59-0400"
    };

    let runtime = calc_runtime(&event_def, &target).unwrap();
    assert_eq!(runtime, Some("2023-08-20T01:05:00-0400".to_string()));
}

/// Determine the run_time value for an event.
///
/// Returns the value as an ISO string.
fn calc_runtime(event_def: &JsonValue, target: &JsonValue) -> EgResult<Option<String>> {
    if !util::json_bool(&event_def["passive"]) {
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
    let seconds = date::interval_to_seconds(&delay_intvl)?;
    let runtime = runtime + Duration::seconds(seconds);

    Ok(Some(date::to_iso(&runtime)))
}

/// Returns true if the event def does not require opt in (i.e. everyone
/// is opted in) or it does require an opt-in and the user linked to the
/// target has the needed opt-in user setting.
fn user_is_opted_in(
    editor: &mut Editor,
    event_def: &JsonValue,
    target: &JsonValue,
) -> EgResult<bool> {
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

    let user_id = if target[usr_field].is_object() {
        // fleshed
        util::json_int(&target[usr_field]["id"])?
    } else {
        util::json_int(&target[usr_field])?
    };

    let query = json::object! {
        "usr": user_id,
        "name": opt_in,
        "value": "true",
    };

    Ok(editor.search("aus", query)?.len() > 0)
}

