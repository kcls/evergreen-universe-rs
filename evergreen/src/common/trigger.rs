//! action_trigger bits
use crate::common::org;
use crate::date;
use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use chrono::Duration;
use json::JsonValue;

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
    // Warn and exit w/ Ok if we can't find the requested hook or some
    // data is not shaped as expected.

    let (class, pkey_op) = match editor.idl().get_class_and_pkey(target) {
        Ok((a, b)) => (a, b),
        Err(e) => {
            log::error!("create_events_for_object(): {e}");
            return Ok(());
        }
    };

    let pkey = match pkey_op {
        Some(k) => k,
        None => {
            log::warn!("Skipping. Object has no primary key: {}", target.dump());
            return Ok(());
        }
    };

    let hook_obj = match editor.retrieve("ath", hook)? {
        Some(h) => h,
        None => {
            log::warn!("No such A/T hook: {hook}");
            return Ok(());
        }
    };

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
        if let Some(gran) = granularity {
            // If a granularity is provided by the caller, the def
            // must a) have one and b) have one that matches.
            if let Some(def_gran) = def["granularity"].as_str() {
                if def_gran != gran {
                    continue;
                }
            } else {
                continue;
            }
        }

        if !ignore_opt_in && !user_is_opted_in(editor, def, &target)? {
            continue;
        }

        let runtime = match calc_runtime(def, &target)? {
            Some(t) => t,
            None => continue,
        };

        let mut event = json::object! {
            "target": pkey.clone(),
            "event_def": def["id"].clone(),
            "run_time": runtime,
        };

        if let Some(udata) = user_data {
            event["user_data"] = json::from(udata.dump());
        }

        let event = editor.idl().create_from("atev", event)?;

        editor.create(&event)?;
    }

    Ok(())
}

/// Returns the event runtime as an ISO string if it can be calculated.
///
/// If an event is meant to have a delay, but the delay cannot be
/// calculated, due to lack of data, return None.
fn calc_runtime(event_def: &JsonValue, target: &JsonValue) -> EgResult<Option<String>> {
    let now = date::now_local();

    if !util::json_bool(&event_def["passive"]) {
        // Active events always run now.
        return Ok(Some(date::to_iso(&now)));
    }

    let delay_field = match event_def["delay_field"].as_str() {
        Some(d) => d,
        None => return Ok(Some(date::to_iso(&now))),
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
/// is opted in) or it does and the user in question has the necessary
/// opt-in user setting.
fn user_is_opted_in(
    editor: &mut Editor,
    event_def: &JsonValue,
    target: &JsonValue,
) -> EgResult<bool> {
    let opt_in = match event_def["opt_in_setting"].as_str() {
        Some(o) => o,
        None => return Ok(true),
    };

    let usr_field = match event_def["usr_field"].as_str() {
        Some(f) => f,
        None => return Ok(true),
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

    let opt_in_settings = editor.search("aus", query)?;

    Ok(opt_in_settings.len() > 0)
}
