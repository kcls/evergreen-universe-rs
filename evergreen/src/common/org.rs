use crate as eg;
use chrono::prelude::Datelike;
use chrono::Duration;
use eg::date;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;

/// Apply a variety of DB transforms to an org unit and return
/// the calculated org unit IDs.
fn org_relations_query(
    editor: &mut Editor,
    org_id: i64,
    transform: &str,
    depth: Option<i64>,
) -> EgResult<Vec<i64>> {
    let mut query = eg::hash! {
        "select": {
            "aou": [{
                "transform": transform,
                "column": "id",
                "result_field": "id",
                "params": []
            }]
        },
        "from": "aou",
        "where": {"id": org_id}
    };

    if let Some(d) = depth {
        query["select"][0]["params"] = EgValue::from(vec![d]);
    }

    let list = editor.json_query(query)?;

    let mut ids = Vec::new();
    for h in list {
        ids.push(h.id()?);
    }
    Ok(ids)
}

pub fn by_shortname(editor: &mut Editor, sn: &str) -> EgResult<EgValue> {
    if let Some(o) = editor.search("aou", eg::hash! {"shortname": sn})?.pop() {
        Ok(o)
    } else {
        Err(editor.die_event())
    }
}

pub fn ancestors(editor: &mut Editor, org_id: i64) -> EgResult<Vec<i64>> {
    org_relations_query(editor, org_id, "actor.org_unit_ancestors", None)
}

pub fn descendants(editor: &mut Editor, org_id: i64) -> EgResult<Vec<i64>> {
    org_relations_query(editor, org_id, "actor.org_unit_descendants", None)
}

pub fn full_path(editor: &mut Editor, org_id: i64, depth: Option<i64>) -> EgResult<Vec<i64>> {
    org_relations_query(editor, org_id, "actor.org_unit_full_path", depth)
}

/// Conveys the open state of an org unit on a specific day.
#[derive(Clone, PartialEq)]
pub enum OrgOpenState {
    /// Open on the requested date.
    Open,
    /// Org unit is never open.
    Never,
    /// Org unit is closed on the requested day and will be open
    /// again on the day representd by this date.
    OpensOnDate(date::EgDate),
}

/// Returns an OrgOpenState descibing the open state of the org unit
/// on the provided day in the timezone of the provided date.
///
/// If the result is OrgOpenState::OpensOnDate(date), the date value
/// will be a fully-qualified DateTime with fixed timezone (so the
/// original time zone can be retained).  However, only the date portion
/// of the datetime is meaningful.  To get the final unadorned Date,
/// in the timezone of the returned DateTime, without time or timzone:
/// date.date_naive()
pub fn next_open_date(
    editor: &mut Editor,
    org_id: i64,
    date: &date::EgDate,
) -> EgResult<OrgOpenState> {
    let start_date = date.clone();
    let mut date = date.clone();

    let mut closed_days: Vec<i64> = Vec::new();
    if let Some(h) = editor.retrieve("aouhoo", org_id)? {
        for day in 0..7 {
            let open = h[&format!("dow_{day}_open")].as_str().unwrap();
            let close = h[&format!("dow_{day}_close")].as_str().unwrap();
            if open == "00:00:00" && close == open {
                closed_days.push(day);
            }
        }

        // Always closed.
        if closed_days.len() == 7 {
            return Ok(OrgOpenState::Never);
        }
    }

    let mut counter = 0;
    while counter < 366 {
        // inspect at most 1 year of data
        counter += 1;

        // Zero-based day of week
        let weekday = date.date_naive().weekday().num_days_from_sunday();

        if closed_days.contains(&(weekday as i64)) {
            // Closed for the current day based on hours of operation.
            // Jump ahead one day and start over.
            date = date + Duration::try_days(1).expect("In Bounds");
            continue;
        }

        // Open this day based on hours of operation.
        // See if any overlapping closings are configured instead.

        let timestamp = date::to_iso(&date);
        let query = eg::hash! {
            "org_unit": org_id,
            "close_start": {"<=": EgValue::from(timestamp.clone())},
            "close_end": {">=": EgValue::from(timestamp)},
        };

        let org_closed = editor.search("aoucd", query)?;

        if org_closed.len() == 0 {
            // No overlapping closings.  We've found our open day.
            if start_date == date {
                // No changes were made.  We're open on the requested day.
                return Ok(OrgOpenState::Open);
            } else {
                // Advancements were made to the date in progress to
                // find an open day.
                return Ok(OrgOpenState::OpensOnDate(date));
            }
        }

        // Find the end of the closed date range and jump ahead to that.
        let mut range_end = org_closed[0]["close_end"].as_str().unwrap();
        for day in org_closed.iter() {
            let end = day["close_end"].as_str().unwrap();
            if end > range_end {
                range_end = end;
            }
        }

        date = date::parse_datetime(&range_end)?;
        date = date + Duration::try_days(1).expect("In Bounds");
    }

    // If we get here it means we never found an open day.
    Ok(OrgOpenState::Never)
}

/// Returns the proximity from from_org to to_org.
pub fn proximity(editor: &mut Editor, from_org: i64, to_org: i64) -> EgResult<Option<i64>> {
    let query = eg::hash! {
        "select": {"aoup": ["prox"]},
        "from": "aoup",
        "where": {
            "from_org": from_org,
            "to_org": to_org
        }
    };

    if let Some(prox) = editor.json_query(query)?.pop() {
        Ok(prox["prox"].as_int())
    } else {
        Ok(None)
    }
}
