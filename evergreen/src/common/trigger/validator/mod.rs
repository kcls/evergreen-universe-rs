use crate::common::trigger::Event;
use crate::editor::Editor;
use crate::result::EgResult;
use crate::date;
use json::JsonValue;
use chrono::Duration;

/// Validat an event.
///
/// TODO stacked validators.
pub fn validate(editor: &mut Editor, event: &Event) -> EgResult<bool> {
    // Loading modules dynamically is not as simple in Rust as in Perl.
    // Hard-code a module-mapping instead. (*shrug* They all requires
    // code changes).

    // required string field.
    let validator = event.event_def()["validator"].as_str().unwrap();

    match validator {
        "NOOP_True" => Ok(true),
        "NOOP_False" => Ok(false),
        "CircIsOpen" => circ_is_open(editor, event),
        _ => Err(format!("No such validator: {validator}").into()),
    }
}


/// Returns the parameter value with the provided name or None if no 
/// such parameter exists.
fn get_param_value<'a>(event: &'a Event, param_name: &str) -> Option<&'a JsonValue> {
    for param in event.event_def()["params"].members() {
        if param["param"].as_str() == Some(param_name) {
            return Some(&param["value"]);
        }
    }
    None
}

/// Returns the parameter value with the provided name as a &str or None 
/// if no such parameter exists OR the parameter is not a JSON string.
fn get_param_value_as_str<'a>(event: &'a Event, param_name: &str) -> Option<&'a str> {
    if let Some(pval) = get_param_value(event, param_name) {
        pval["value"].as_str()
    } else {
        None
    }
}

/// True if the target circulation is still open.
fn circ_is_open(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    if event.target()["checkin_time"].is_string() {
        return Ok(false);
    }

    if event.target()["xact_finish"].is_string() {
        return Ok(false);
    }

    let min_target_age = get_param_value(event, "min_target_age");

    if min_target_age.is_some() {
        if let Some(fname) = get_param_value_as_str(event, "target_age_field") {
            if fname == "xact_start" {
                return min_passive_target_age(_editor, event);
            }
        }
    }

    Ok(true)
}

fn min_passive_target_age(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    let min_target_age = get_param_value_as_str(event, "min_target_age")
        .ok_or_else(|| format!(
            "'min_target_age' parameter required for MinPassiveTargetAge"
        ))?;

    let age_field = get_param_value_as_str(event, "target_age_field")
        .ok_or_else(|| format!(
            "'target_age_field' parameter or delay_field required for MinPassiveTargetAge"
        ))?;

    let age_field_jval = &event.target()[age_field];
    let age_date_str = age_field_jval.as_str()
        .ok_or_else(|| format!(
            "MinPassiveTargetAge age field {age_field} has unexpected value: {}", 
            age_field_jval.dump()
        ))?;

    let age_field_ts = date::parse_datetime(age_date_str)?;
    let interval = date::interval_to_seconds(min_target_age)?;
    let age_field_ts = age_field_ts + Duration::seconds(interval);

    Ok(age_field_ts <= date::now())
}
