use crate::common::org;
use crate::common::settings::Settings;
use crate::date;
use crate::editor::Editor;
use crate::util::{json_bool, json_bool_op, json_int};
use chrono::Duration;
use json::JsonValue;
/*
use crate::common::org;
use crate::event::EgEvent;
use crate::util;
use std::collections::{HashMap, HashSet};
use std::fmt;
*/

/// Returns an ISO date string if a shelf time was calculated, None
/// if holds do not expire on the shelf.
pub fn calc_hold_shelf_expire_time(
    editor: &mut Editor,
    hold: &JsonValue,
    start_time: Option<&str>,
) -> Result<Option<String>, String> {
    let pickup_lib = json_int(&hold["pickup_lib"])?;

    let mut settings = Settings::new(&editor);
    let interval =
        settings.get_value_at_org("circ.holds.default_shelf_expire_interval", pickup_lib)?;

    let interval = match interval.as_str() {
        Some(i) => i,
        None => return Ok(None), // hold never expire on-shelf.
    };

    let interval = date::interval_to_seconds(interval)?;

    let start_time = if let Some(st) = start_time {
        date::parse_datetime(&st)?
    } else {
        date::now_local()
    };

    let mut start_time = start_time + Duration::seconds(interval);
    let org_info = org::next_open_date(editor, pickup_lib, &start_time)?;

    if let org::OrgOpenState::OpensOnDate(open_on) = org_info {
        // Org unit is closed on the calculated shelf expire date.
        // Extend the expire date to the end of the next open day.
        start_time = date::set_hms(&open_on, 23, 59, 59)?;
    }

    Ok(Some(date::to_iso(&start_time)))
}

/// Returns the captured, unfulfilled, uncanceled hold that
/// targets the provided copy.
pub fn captured_hold_for_copy(
    editor: &mut Editor,
    copy_id: i64
) -> Result<Option<JsonValue>, String> {

    let query = json::object! {
        current_copy: copy_id,
        capture_time: {"!=": JsonValue::Null},
        fulfillment_time: JsonValue::Null,
        cancel_time: JsonValue::Null,
    };

    Ok(editor.search("ahr", query)?.first().map(|h| h.to_owned()))
}

