use crate::common::trigger::Event;
use crate::editor::Editor;
use crate::result::EgResult;
use crate::common::holdings;
use crate::date;
use crate::util;
use crate::constants as C;
use json::JsonValue;
use chrono::Duration;

/// Validate an event.
///
/// TODO stacked validators.
pub fn validate(editor: &mut Editor, event: &Event) -> EgResult<bool> {
    // Loading modules dynamically is not as simple in Rust as in Perl.
    // Hard-code a module-mapping instead. (*shrug* They all require
    // code changes).

    // required string field.
    let validator = event.event_def()["validator"].as_str().unwrap();

    match validator {
        "NOOP_True" => Ok(true),
        "NOOP_False" => Ok(false),
        "CircIsOpen" => circ_is_open(editor, event),
        "CircIsOverdue" => circ_is_overdue(editor, event),
        "HoldIsAvailable" => hold_is_available(editor, event),
        "HoldIsCancelled" => hold_is_canceled(editor, event),
        "HoldNotifyCheck" => hold_notify_check(editor, event),
        "MinPassiveTargetAge" => min_passive_target_age(editor, event),
        "PatronBarred" => patron_is_barred(editor, event),
        "PatronNotBarred" => patron_is_barred(editor, event).map(|val| !val),
        "ReservationIsAvailable" => reservation_is_available(editor, event),
        _ => Err(format!("No such validator: {validator}").into()),
    }
}


/// Returns the parameter value with the provided name or None if no 
/// such parameter exists.
fn param_value<'a>(event: &'a Event, param_name: &str) -> Option<&'a JsonValue> {
    for param in event.event_def()["params"].members() {
        if param["param"].as_str() == Some(param_name) {
            return Some(&param["value"]);
        }
    }
    None
}

/// Returns the parameter value with the provided name as a &str or None 
/// if no such parameter exists OR the parameter is not a JSON string.
fn param_value_as_str<'a>(event: &'a Event, param_name: &str) -> Option<&'a str> {
    if let Some(pval) = param_value(event, param_name) {
        pval["value"].as_str()
    } else {
        None
    }
}

fn param_value_as_bool<'a>(event: &'a Event, param_name: &str) -> bool {
    if let Some(pval) = param_value(event, param_name) {
        util::json_bool(&pval["value"])
    } else {
        false
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

    if param_value(event, "min_target_age").is_some() {
        if let Some(fname) = param_value_as_str(event, "target_age_field") {
            if fname == "xact_start" {
                return min_passive_target_age(_editor, event);
            }
        }
    }

    Ok(true)
}

fn min_passive_target_age(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    let min_target_age = param_value_as_str(event, "min_target_age")
        .ok_or_else(|| format!(
            "'min_target_age' parameter required for MinPassiveTargetAge"
        ))?;

    let age_field = param_value_as_str(event, "target_age_field")
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

fn circ_is_overdue(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    if event.target()["checkin_time"].is_string() {
        return Ok(false);
    }

    if let Some(stop_fines) = event.target()["stop_fines"].as_str() {
        if stop_fines == "MAXFINES" || stop_fines == "LONGOVERDUE" {
            return Ok(false);
        }
    }

    if param_value(event, "min_target_age").is_some() {
        if let Some(fname) = param_value_as_str(event, "target_age_field") {
            if fname == "xact_start" {
                return min_passive_target_age(_editor, event);
            }
        }
    }
    
    // due_date is a required string field.
    let due_date = event.target()["due_date"].as_str().unwrap();
    let due_date_ts = date::parse_datetime(due_date)?;

    Ok(due_date_ts < date::now())
}

/// True if the hold is ready for pickup.
fn hold_is_available(editor: &mut Editor, event: &Event) -> EgResult<bool> {
    if !hold_notify_check(editor, event)? {
        return Ok(false);
    }

    let hold = event.target();

    // Start with some simple tests.
    let canceled = hold["cancel_time"].is_string();
    let fulfilled = hold["fulfillment_time"].is_string();
    let captured = hold["capture_time"].is_string();
    let shelved = hold["shelf_time"].is_string();

    if canceled || fulfilled || !captured || !shelved {
        return Ok(false);
    }

    // Verify shelf lib matches pickup lib -- it's not sitting on
    // the wrong shelf somewhere.
    //
    // Accommodate fleshing
    let shelf_lib = match hold["current_shelf_lib"].as_i64() {
        Some(id) => id,
        None => match hold["current_shelf_lib"]["id"].as_i64() {
            Some(id) => id,
            None => return Ok(false),
        }
    };

    let pickup_lib = match hold["pickup_lib"].as_i64() {
        Some(id) => id,
        // pickup_lib is a required numeric value.
        None => util::json_int(&hold["pickup_lib"]["id"])?,
    };

    if shelf_lib != pickup_lib {
        return Ok(false);
    }

    // Verify we have a targted copy and it has the expected status.
    let copy_status = if let Some(copy_id) = hold["current_copy"].as_i64() {
        holdings::copy_status(editor, Some(copy_id), None)?
    } else if hold["current_copy"].is_object() {
        holdings::copy_status(editor, None, Some(&hold["current_copy"]))?
    } else {
        -1
    };

    Ok(copy_status == C::COPY_STATUS_ON_HOLDS_SHELF)
}

fn hold_is_canceled(editor: &mut Editor, event: &Event) -> EgResult<bool> {
    if hold_notify_check(editor, event)? {
        Ok(event.target()["cancel_time"].is_string())
    } else {
        Ok(false)
    }
}

/// Returns false if a notification parameter is present and the
/// hold in question is inconsistent with the parameter.
///
/// In general, if this test fails, the event should not proceed
/// to reacting.
///
/// Assumes the hold in question == the event.target().
fn hold_notify_check(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    let hold = event.target();

    if param_value_as_bool(event, "check_email_notify") {
        if !util::json_bool(&hold["email_notify"]) {
            return Ok(false);
        }
    }

    if param_value_as_bool(event, "check_sms_notify") {
        if !util::json_bool(&hold["sms_notify"]) {
            return Ok(false);
        }
    }

    if param_value_as_bool(event, "check_phone_notify") {
        if !util::json_bool(&hold["phone_notify"]) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn reservation_is_available(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    let res = event.target();
    Ok(
        res["cancel_time"].is_null() 
            && !res["capture_time"].is_null()
            && !res["current_resource"].is_null()
    )
}

fn patron_is_barred(_editor: &mut Editor, event: &Event) -> EgResult<bool> {
    Ok(util::json_bool(&event.target()["barred"]))
}


// Perl has CircIsAutoRenewable but it oddly creates the same
// events (hook 'autorenewal') that the autorenewal reactor creates,
// and it's not used in the default A/T definitions.  Guessing that 
// validator should be removed from the Perl.

// TODO PatronNotInCollections

