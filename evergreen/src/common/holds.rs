use crate::editor::Editor;
use crate::settings::Settings;
use crate::util::{json_bool, json_bool_op, json_int};
use crate::date;
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
) -> Result<Option<String>, String>  {

    let mut settings = Settings::new(&editor);
    let interval = settings.get_value_at_org(
        "circ.holds.default_shelf_expire_interval",
        json_int(&hold["pickup_lib"])?
    )?;

    let interval = match interval.as_str() {
        Some(i) => i,
        None => return Ok(None), // hold never expire on-shelf.
    };

    let start_time = if let Some(st) = start_time {
        date::parse_datetime(&st)?
    } else {
        date::now_local()
    };

    // TODO
    Ok(None)
}
