use crate::common::holds;
use crate::constants as C;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util::json_int;

pub fn cancel_transit(editor: &mut Editor, transit_id: i64, skip_hold_reset: bool) -> EgResult<()> {
    let flesh = json::object! {
        "flesh": 1,
        "flesh_fields": {
            "atc": ["target_copy", "hold_transit_copy"]
        }
    };

    let mut transit = editor
        .retrieve_with_ops("atc", transit_id, flesh)?
        .ok_or_else(|| editor.die_event())?;

    let mut copy = transit["target_copy"].take();
    transit["target_copy"] = copy["id"].clone();

    let tc_status = json_int(&transit["copy_status"])?;

    let to_lost = tc_status == C::COPY_STATUS_LOST || tc_status == C::COPY_STATUS_LOST_AND_PAID;

    let to_missing = tc_status == C::COPY_STATUS_MISSING;

    if (to_lost && !editor.allowed("ABORT_TRANSIT_ON_LOST")?)
        || (to_missing && !editor.allowed("ABORT_TRANSIT_ON_MISSING")?)
    {
        let mut evt = EgEvent::new("TRANSIT_ABORT_NOT_ALLOWED");
        evt.set_ad_hoc_value("copy_status", json::from(tc_status));
        return Err(evt.into());
    }

    let here = editor.requestor_ws_ou();
    let source = json_int(&transit["source"])?;
    let dest = json_int(&transit["dest"])?;

    if source != here && dest != here {
        // Perl uses "here" as the permission org, but checking
        // at the source + dest kinda makes more sense.
        if !editor.allowed_at("ABORT_REMOTE_TRANSIT", here)? {
            return Err(editor.die_event());
        }
    }

    let mut reset_hold_id = None;
    if transit["hold_transit_copy"].is_object() && !skip_hold_reset {
        // capture this before the transit is consumed below.
        reset_hold_id = Some(json_int(&transit["hold_transit_copy"]["hold"])?);
    }

    transit["cancel_time"] = json::from("now");
    editor.update(transit)?;

    let copy_status = json_int(&copy["status"])?;

    // The status adopted by the copy in transit depends on
    // the intended destination status of the copy.
    if copy_status == C::COPY_STATUS_IN_TRANSIT {
        if tc_status == C::COPY_STATUS_AVAILABLE
            || tc_status == C::COPY_STATUS_CHECKED_OUT
            || tc_status == C::COPY_STATUS_IN_PROCESS
            || tc_status == C::COPY_STATUS_ON_HOLDS_SHELF
            || tc_status == C::COPY_STATUS_IN_TRANSIT
            || tc_status == C::COPY_STATUS_CATALOGING
            || tc_status == C::COPY_STATUS_ON_RESV_SHELF
            || tc_status == C::COPY_STATUS_RESHELVING
        {
            // These transit copy statuses are discarded.
            copy["status"] = json::from(C::COPY_STATUS_CANCELED_TRANSIT);
        } else {
            // Otherwise, adopt the copy status stored on the transit.
            copy["status"] = json::from(tc_status);
        }

        copy["editor"] = json::from(editor.requestor_id());
        copy["edit_date"] = json::from("now");

        editor.update(copy)?;
    }

    if let Some(hold_id) = reset_hold_id {
        holds::reset_hold(editor, hold_id)?;
    }

    Ok(())
}
