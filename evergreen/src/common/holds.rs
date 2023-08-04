use crate::common::org;
use crate::common::settings::Settings;
use crate::common::transit;
use crate::constants as C;
use crate::date;
use crate::editor::Editor;
use crate::event::{EgEvent, Overrides};
use crate::result::EgResult;
use crate::util::{json_bool, json_int};
use chrono::Duration;
use json::JsonValue;

/// Returns an ISO date string if a shelf time was calculated, None
/// if holds do not expire on the shelf.
pub fn calc_hold_shelf_expire_time(
    editor: &mut Editor,
    hold: &JsonValue,
    start_time: Option<&str>,
) -> EgResult<Option<String>> {
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
pub fn captured_hold_for_copy<T>(editor: &mut Editor, copy_id: T) -> EgResult<Option<JsonValue>>
where
    T: Into<JsonValue>,
{
    let query = json::object! {
        current_copy: copy_id.into(),
        capture_time: {"!=": JsonValue::Null},
        fulfillment_time: JsonValue::Null,
        cancel_time: JsonValue::Null,
    };

    Ok(editor.search("ahr", query)?.first().map(|h| h.to_owned()))
}

/// Returns the captured hold if found and a list of hold IDs that
/// will need to be retargeted, since they previously targeted the
/// provided copy.
pub fn find_nearest_permitted_hold<T>(
    editor: &mut Editor,
    copy_id: T,
    check_only: bool,
) -> EgResult<Option<(JsonValue, Vec<i64>)>>
where
    T: Into<JsonValue>,
{
    let mut retarget: Vec<i64> = Vec::new();
    let copy_id = copy_id.into();

    // Fetch the appropriatly fleshed copy.
    let flesh = json::object! {
        flesh: 1,
        flesh_fields: {
            "acp": ["call_number"],
        }
    };

    let copy = match editor.retrieve_with_ops("acp", &copy_id, flesh)? {
        Some(c) => c,
        None => Err(editor.die_event())?,
    };

    let query = json::object! {
       "current_copy": copy_id.clone(),
       "cancel_time": JsonValue::Null,
       "capture_time": JsonValue::Null,
    };

    let mut old_holds = editor.search("ahr", query)?;

    let mut settings = Settings::new(&editor);
    let hold_stall_intvl = settings.get_value("circ.hold_stalling.soft")?;

    let params = vec![
        json::from(editor.requestor_ws_ou()),
        json::from(copy.clone()),
        json::from(100),
        json::from(hold_stall_intvl.to_owned()),
    ];

    // best_holds is a JSON array of JSON hold IDs.
    let best_hold_results = editor.client_mut().send_recv_one(
        "open-ils.storage",
        "open-ils.storage.action.hold_request.nearest_hold.atomic",
        params,
    )?;

    // Map the JSON hold IDs to numbers.
    let mut best_holds: Vec<i64> = Vec::new();
    if let Some(bhr) = best_hold_results {
        for h in bhr.members() {
            best_holds.push(json_int(&h)?);
        }
    }

    // Holds that already target this copy are still in the game.
    for old_hold in old_holds.iter() {
        let old_id = json_int(&old_hold["id"])?;
        if !best_holds.contains(&old_id) {
            best_holds.push(old_id);
        }
    }

    if best_holds.len() == 0 {
        log::info!("Found no suitable holds for item {}", copy["barcode"]);
        return Ok(None);
    }

    let mut best_hold = None;

    for hold_id in best_holds {
        log::info!(
            "Checking if hold {hold_id} is permitted for copy {}",
            copy["barcode"]
        );

        let hold = editor.retrieve("ahr", hold_id)?.unwrap(); // required
        let hold_type = hold["hold_type"].as_str().unwrap(); // required
        if hold_type == "R" || hold_type == "F" {
            // These hold types do not require verification
            best_hold = Some(hold);
            break;
        }

        let result = test_copy_for_hold(
            editor,
            hold["usr"].clone(),
            copy_id.clone(),
            hold["pickup_lib"].clone(),
            hold["request_lib"].clone(),
            hold["requestor"].clone(),
            true,
            None,
        )?;

        if result.success {
            best_hold = Some(hold);
            break;
        }
    }

    let mut targeted_hold = match best_hold {
        Some(h) => h,
        None => {
            log::info!("No suitable holds found for copy {}", copy["barcode"]);
            return Ok(None);
        }
    };

    log::info!(
        "Best hold {} found for copy {}",
        targeted_hold["id"],
        copy["barcode"]
    );

    if check_only {
        return Ok(Some((targeted_hold, retarget)));
    }

    // Target the copy
    targeted_hold["current_copy"] = json::from(copy_id);
    editor.update(&targeted_hold)?;

    // len() test required for drain()
    if old_holds.len() > 0 {
        // Retarget any other holds that currently target this copy.
        for mut hold in old_holds.drain(0..) {
            if hold["id"] == targeted_hold["id"] {
                continue;
            }
            hold["current_copy"].take();
            hold["prev_check_time"].take();
            editor.update(&hold)?;
            retarget.push(json_int(&hold["id"])?);
        }
    }

    return Ok(Some((targeted_hold, retarget)));
}

pub struct HoldPermitResult {
    matchpoint: Option<i64>,
    fail_part: Option<String>,
    mapped_event: Option<EgEvent>,
    failed_override: Option<EgEvent>,
}

impl HoldPermitResult {
    pub fn new() -> HoldPermitResult {
        HoldPermitResult {
            matchpoint: None,
            fail_part: None,
            mapped_event: None,
            failed_override: None,
        }
    }
}

pub struct TestCopyForHoldResult {
    /// True if the permit call returned a success or we were able
    /// to override all failure events.
    success: bool,

    /// Details on the individual permit results.
    permit_results: Vec<HoldPermitResult>,

    /// True if age-protect is the only blocking factor.
    age_protect_only: bool,
}

/// Test if a hold can be used to fill a hold.
pub fn test_copy_for_hold<T, U, V, W, X>(
    editor: &mut Editor,
    patron_id: T,
    copy_id: U,
    pickup_lib: V,
    request_lib: W,
    requestor: X,
    is_retarget: bool,
    overrides: Option<Overrides>,
) -> EgResult<TestCopyForHoldResult>
where
    T: Into<JsonValue>,
    U: Into<JsonValue>,
    V: Into<JsonValue>,
    W: Into<JsonValue>,
    X: Into<JsonValue>,
{
    let copy_id = copy_id.into();
    let patron_id = patron_id.into();
    let pickup_lib = pickup_lib.into();
    let request_lib = request_lib.into();
    let requestor = requestor.into();

    let mut result = TestCopyForHoldResult {
        success: false,
        permit_results: Vec::new(),
        age_protect_only: false,
    };

    let db_func = match is_retarget {
        true => "action.hold_retarget_permit_test",
        false => "action.hold_request_permit_test",
    };

    let query = json::object! {
        "from": [
            db_func,
            pickup_lib,
            request_lib,
            copy_id,
            patron_id,
            requestor,
        ]
    };

    let db_results = editor.json_query(query)?;

    if let Some(row) = db_results.first() {
        // If the first result is a success, we're done.
        if json_bool(&row["success"]) {
            let mut res = HoldPermitResult::new();

            res.matchpoint = json_int(&row["matchpoint"]).ok(); // Option
            result.permit_results.push(res);
            result.success = true;

            return Ok(result);
        }
    }

    let mut pending_results = Vec::new();

    for res in db_results.iter() {
        let fail_part = match res["fail_part"].as_str() {
            Some(s) => s,
            None => continue, // Should not happen.
        };

        let matchpoint = json_int(&db_results[0]["matchpoint"]).ok(); // Option

        let mut res = HoldPermitResult::new();
        res.fail_part = Some(fail_part.to_string());
        res.matchpoint = matchpoint;

        // Map some newstyle fail parts to legacy event codes.
        let evtcode = match fail_part {
            "config.hold_matrix_test.holdable" => "ITEM_NOT_HOLDABLE",
            "item.holdable" => "ITEM_NOT_HOLDABLE",
            "location.holdable" => "ITEM_NOT_HOLDABLE",
            "status.holdable" => "ITEM_NOT_HOLDABLE",
            "transit_range" => "ITEM_NOT_HOLDABLE",
            "no_matchpoint" => "NO_POLICY_MATCHPOINT",
            "config.hold_matrix_test.max_holds" => "MAX_HOLDS",
            "config.rule_age_hold_protect.prox" => "ITEM_AGE_PROTECTED",
            _ => fail_part,
        };

        let mut evt = EgEvent::new(evtcode);
        evt.set_payload(json::object! {
            "fail_part": fail_part,
            "matchpoint": matchpoint,
        });

        res.mapped_event = Some(evt);
        pending_results.push(res);
    }

    if pending_results.len() == 0 {
        // This should not happen, but cannot go unchecked.
        return Ok(result);
    }

    let mut has_failure = false;
    let mut has_age_protect = false;
    for mut pending_result in pending_results.drain(0..) {
        let evt = pending_result.mapped_event.as_ref().unwrap();

        if !has_age_protect {
            has_age_protect = evt.textcode() == "ITEM_AGE_PROTECTED";
        }

        let try_override = if let Some(ov) = overrides.as_ref() {
            match ov {
                Overrides::All => true,
                Overrides::Events(ref list) => list
                    .iter()
                    .map(|e| e.as_str())
                    .collect::<Vec<&str>>()
                    .contains(&evt.textcode()),
            }
        } else {
            false
        };

        if try_override {
            let permission = format!("{}.override", evt.textcode());
            log::debug!("Checking permission to verify copy for hold: {permission}");

            if editor.allowed(&permission)? {
                log::debug!("Override succeeded for {permission}");
            } else {
                has_failure = true;
                if let Some(e) = editor.last_event() {
                    // should be set.
                    pending_result.failed_override = Some(e.clone());
                }
            }
        }

        result.permit_results.push(pending_result);
    }

    result.age_protect_only = has_age_protect && result.permit_results.len() == 1;

    // If all events were successfully overridden, then the end
    // result is a success.
    result.success = !has_failure;

    Ok(result)
}

/// Send holds to the hold targeter service for retargeting.
///
/// The editor is needed so we can have a ref to an opensrf client.
/// TODO: As is, this is NOT run within a transaction, since it's a
/// call to a remote service.  If targeting is ever ported to Rust, it
/// can run in the same transaction.
pub fn retarget_holds<T>(editor: &mut Editor, hold_ids: &[T]) -> EgResult<()>
where
    T: Into<JsonValue> + Clone,
{
    editor.client_mut().send_recv_one(
        "open-ils.hold-targeter",
        "open-ils.hold-targeter.target",
        json::object! {hold: hold_ids},
    )?;

    Ok(())
}

/// Reset a hold and retarget it.
///
/// NOTE: Since retargeting must run outside of our transaction, and our
/// changes must be committed before retargeting occurs, this function
/// begins and commits its own transaction, by way of a cloned copy of
/// the provided editor.
pub fn reset_hold<T>(editor: &mut Editor, hold_id: T) -> EgResult<()>
where
    T: Into<JsonValue>,
{
    let hold_id = hold_id.into();
    log::info!("Resetting hold {hold_id}");

    // Leave the provided editor in whatever state it's already in.
    // and start our own transaction.
    let mut editor = editor.clone();
    editor.xact_begin()?;

    let mut hold = editor
        .retrieve("ahr", &hold_id)?
        .ok_or(editor.die_event())?;

    // Resetting captured holds requires a little more care.
    if !hold["capture_time"].is_null() && !hold["current_copy"].is_null() {
        let mut copy = editor
            .retrieve("acp", hold["current_copy"].clone())?
            .ok_or(editor.die_event())?;

        let copy_status = json_int(&copy["status"])?;

        if copy_status == C::COPY_STATUS_ON_HOLDS_SHELF {
            copy["status"] = json::from(C::COPY_STATUS_RESHELVING);
            copy["editor"] = json::from(editor.requestor_id());
            copy["edit_date"] = json::from("now");

            editor.update(&copy)?;
        } else if copy_status == C::COPY_STATUS_IN_TRANSIT {
            let query = json::object! {
                "hold": hold_id.clone(),
                "cancel_time": JsonValue::Null,
            };

            if let Some(ht) = editor.search("ahtc", query)?.pop() {
                transit::cancel_transit(&mut editor, json_int(&ht["id"])?, true)?;
            }
        }
    }

    hold["capture_time"].take();
    hold["current_copy"].take();
    hold["shelf_time"].take();
    hold["shelf_expire_time"].take();
    hold["current_shelf_lib"].take();

    editor.update(&hold)?;
    editor.commit()?;

    let id = json_int(&hold_id)?; // TODO avoid this translation
    retarget_holds(&mut editor, &[id])
}
