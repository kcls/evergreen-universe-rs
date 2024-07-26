use crate as eg;
use eg::common::org;
use eg::common::settings::Settings;
use eg::common::targeter;
use eg::common::transit;
use eg::constants as C;
use eg::date;
use eg::event::{EgEvent, Overrides};
use eg::Editor;
use eg::EgError;
use eg::EgResult;
use eg::EgValue;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HoldType {
    Copy,
    Recall,
    Force,
    Issuance,
    Volume,
    Part,
    Title,
    Metarecord,
}

/// let s: &str = hold_type.into();
#[rustfmt::skip]
impl From<HoldType> for &'static str {
    fn from(t: HoldType) -> &'static str {
        match t {
            HoldType::Copy       => "C",
            HoldType::Recall     => "R",
            HoldType::Force      => "F",
            HoldType::Volume     => "V",
            HoldType::Issuance   => "I",
            HoldType::Part       => "P",
            HoldType::Title      => "T",
            HoldType::Metarecord => "M",
        }
    }
}

/// let hold_type: HoldType = "T".into();
impl TryFrom<&str> for HoldType {
    type Error = EgError;
    fn try_from(code: &str) -> EgResult<HoldType> {
        match code {
            "C" => Ok(HoldType::Copy),
            "R" => Ok(HoldType::Recall),
            "F" => Ok(HoldType::Force),
            "V" => Ok(HoldType::Volume),
            "I" => Ok(HoldType::Issuance),
            "P" => Ok(HoldType::Part),
            "T" => Ok(HoldType::Title),
            "M" => Ok(HoldType::Metarecord),
            _ => Err(format!("No such hold type: {}", code).into()),
        }
    }
}

/// Just enough hold information to make business decisions.
pub struct MinimalHold {
    id: i64,
    target: i64,
    pickup_lib: i64,
    hold_type: HoldType,
    /// active == not canceled, not fulfilled, and not frozen.
    active: bool,
}

impl MinimalHold {
    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn target(&self) -> i64 {
        self.target
    }
    pub fn pickup_lib(&self) -> i64 {
        self.pickup_lib
    }
    pub fn hold_type(&self) -> HoldType {
        self.hold_type
    }
    pub fn active(&self) -> bool {
        self.active
    }
}

impl fmt::Display for MinimalHold {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let t: &str = self.hold_type.into();
        write!(
            f,
            "hold id={} target={} pickup_lib={} hold_type={} active={}",
            self.id, self.target, self.pickup_lib, t, self.active
        )
    }
}

/// Returns an ISO date string if a shelf time was calculated, None
/// if holds do not expire on the shelf.
pub fn calc_hold_shelf_expire_time(
    editor: &mut Editor,
    hold: &EgValue,
    start_time: Option<&str>,
) -> EgResult<Option<String>> {
    let pickup_lib = hold["pickup_lib"].int()?;

    let mut settings = Settings::new(editor);
    let interval =
        settings.get_value_at_org("circ.holds.default_shelf_expire_interval", pickup_lib)?;

    let interval = match interval.as_str() {
        Some(i) => i,
        None => return Ok(None), // hold never expire on-shelf.
    };

    let start_time = if let Some(st) = start_time {
        date::parse_datetime(st)?
    } else {
        date::now_local()
    };

    let mut start_time = date::add_interval(start_time, interval)?;
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
pub fn captured_hold_for_copy(editor: &mut Editor, copy_id: i64) -> EgResult<Option<EgValue>> {
    let query = eg::hash! {
        current_copy: copy_id,
        capture_time: {"!=": eg::NULL},
        fulfillment_time: eg::NULL,
        cancel_time: eg::NULL,
    };

    Ok(editor.search("ahr", query)?.pop())
}

/// Returns the captured hold if found and a list of hold IDs that
/// will need to be retargeted, since they previously targeted the
/// provided copy.
pub fn find_nearest_permitted_hold(
    editor: &mut Editor,
    copy_id: i64,
    check_only: bool,
) -> EgResult<Option<(EgValue, Vec<i64>)>> {
    let mut retarget: Vec<i64> = Vec::new();

    // Fetch the appropriatly fleshed copy.
    let flesh = eg::hash! {
        flesh: 1,
        flesh_fields: {
            "acp": ["call_number"],
        }
    };

    let copy = match editor.retrieve_with_ops("acp", copy_id, flesh)? {
        Some(c) => c,
        None => Err(editor.die_event())?,
    };

    let query = eg::hash! {
       "current_copy": copy_id,
       "cancel_time": eg::NULL,
       "capture_time": eg::NULL,
    };

    let mut old_holds = editor.search("ahr", query)?;

    let mut settings = Settings::new(editor);
    let hold_stall_intvl = settings.get_value("circ.hold_stalling.soft")?;

    let params = vec![
        EgValue::from(editor.requestor_ws_ou()),
        copy.clone(),
        EgValue::from(100),
        hold_stall_intvl.clone(),
    ];

    // best_holds is an array of hold IDs.
    let best_hold_results = editor.client_mut().send_recv_one(
        "open-ils.storage",
        "open-ils.storage.action.hold_request.nearest_hold.atomic",
        params,
    )?;

    // Map the hold IDs to numbers.
    let mut best_holds: Vec<i64> = Vec::new();
    if let Some(bhr) = best_hold_results {
        for h in bhr.members() {
            best_holds.push(h.int()?);
        }
    }

    // Holds that already target this copy are still in the game.
    for old_hold in old_holds.iter() {
        let old_id = old_hold.id()?;
        if !best_holds.contains(&old_id) {
            best_holds.push(old_id);
        }
    }

    if best_holds.is_empty() {
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
            CopyHoldParams {
                patron_id: hold["usr"].int()?,
                copy_id,
                pickup_lib: hold["pickup_lib"].int()?,
                request_lib: hold["request_lib"].int()?,
                requestor: hold["requestor"].int()?,
                is_retarget: true, // is_retarget
            },
            None, // overrides
            true, // check_only
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
    targeted_hold["current_copy"] = EgValue::from(copy_id);
    editor.update(targeted_hold.clone())?;

    // Retarget any other holds that currently target this copy.
    for mut hold in old_holds.drain(..) {
        if hold["id"] == targeted_hold["id"] {
            continue;
        }
        let hold_id = hold.id()?;

        hold["current_copy"].take();
        hold["prev_check_time"].take();

        editor.update(hold)?;
        retarget.push(hold_id);
    }

    Ok(Some((targeted_hold, retarget)))
}

#[derive(Default)]
pub struct HoldPermitResult {
    matchpoint: Option<i64>,
    fail_part: Option<String>,
    mapped_event: Option<EgEvent>,
    failed_override: Option<EgEvent>,
}

impl HoldPermitResult {
    pub fn new() -> HoldPermitResult {
        Default::default()
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

impl TestCopyForHoldResult {
    pub fn success(&self) -> bool {
        self.success
    }
    pub fn permit_results(&self) -> &Vec<HoldPermitResult> {
        &self.permit_results
    }
    pub fn age_protect_only(&self) -> bool {
        self.age_protect_only
    }
}

pub struct CopyHoldParams {
    pub patron_id: i64,
    pub copy_id: i64,
    pub pickup_lib: i64,
    pub request_lib: i64,
    pub requestor: i64,
    pub is_retarget: bool,
}

/// Test if a hold can be used to fill a hold.
pub fn test_copy_for_hold(
    editor: &mut Editor,
    params: CopyHoldParams,
    overrides: Option<Overrides>,
    // Exit as soon as we know if the permit was allowed or not.
    // If overrides are provided, this flag is ignored, since
    // overrides require the function process all the things.
    check_only: bool,
) -> EgResult<TestCopyForHoldResult> {
    let patron_id = params.patron_id;
    let copy_id = params.copy_id;
    let pickup_lib = params.pickup_lib;
    let request_lib = params.request_lib;
    let requestor = params.requestor;
    let is_retarget = params.is_retarget;

    let mut result = TestCopyForHoldResult {
        success: false,
        permit_results: Vec::new(),
        age_protect_only: false,
    };

    let db_func = match is_retarget {
        true => "action.hold_retarget_permit_test",
        false => "action.hold_request_permit_test",
    };

    let query = eg::hash! {
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
        if row["success"].boolish() {
            let mut res = HoldPermitResult::new();

            res.matchpoint = row["matchpoint"].as_int(); // Option
            result.permit_results.push(res);
            result.success = true;

            return Ok(result);
        }
    }

    if check_only && overrides.is_none() {
        // Permit test failed.  No overrides needed.
        return Ok(result);
    }

    let mut pending_results = Vec::new();

    for res in db_results.iter() {
        let fail_part = match res["fail_part"].as_str() {
            Some(s) => s,
            None => continue, // Should not happen.
        };

        let matchpoint = db_results[0]["matchpoint"].as_int(); // Option

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
        evt.set_payload(eg::hash! {
            "fail_part": fail_part,
            "matchpoint": matchpoint,
        });

        res.mapped_event = Some(evt);
        pending_results.push(res);
    }

    if pending_results.is_empty() {
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

/// Retarget a batch of holds.
///
/// Each hold is targeted within its own transaction, managed by
/// the targeter.  To target holds within an existing transaction,
/// see `retarget_hold()`.
pub fn retarget_holds(editor: &Editor, hold_ids: &[i64]) -> EgResult<()> {
    let mut editor = editor.clone();
    let mut hold_targeter = targeter::HoldTargeter::new(&mut editor);

    for hold_id in hold_ids {
        hold_targeter.target_hold(*hold_id, None)?;
    }

    Ok(())
}

/// Target a single hold.
///
/// Uses an externally managed Editor transaction.
pub fn retarget_hold(editor: &mut Editor, hold_id: i64) -> EgResult<targeter::HoldTargetContext> {
    let mut targeter = targeter::HoldTargeter::new(editor);

    // We're managing the editor transaction.
    targeter.set_transaction_manged_externally(true);

    targeter.init()?;

    let ctx = targeter.target_hold(hold_id, None)?;

    Ok(ctx)
}

/// Reset a hold and retarget it.
///
/// Uses an externally managed Editor transaction.
pub fn reset_hold(editor: &mut Editor, hold_id: i64) -> EgResult<targeter::HoldTargetContext> {
    log::info!("Resetting hold {hold_id}");

    let mut hold = editor
        .retrieve("ahr", hold_id)?
        .ok_or_else(|| editor.die_event())?;

    // Resetting captured holds requires a little more care.
    if !hold["capture_time"].is_null() && !hold["current_copy"].is_null() {
        let mut copy = editor
            .retrieve("acp", hold["current_copy"].clone())?
            .ok_or_else(|| editor.die_event())?;

        let copy_status = copy["status"].int()?;

        if copy_status == C::COPY_STATUS_ON_HOLDS_SHELF {
            copy["status"] = EgValue::from(C::COPY_STATUS_RESHELVING);
            copy["editor"] = EgValue::from(editor.requestor_id()?);
            copy["edit_date"] = EgValue::from("now");

            editor.update(copy)?;
        } else if copy_status == C::COPY_STATUS_IN_TRANSIT {
            let query = eg::hash! {
                "hold": hold_id,
                "cancel_time": eg::NULL,
            };

            if let Some(ht) = editor.search("ahtc", query)?.pop() {
                transit::cancel_transit(editor, ht.id()?, true)?;
            }
        }
    }

    hold["capture_time"].take();
    hold["current_copy"].take();
    hold["shelf_time"].take();
    hold["shelf_expire_time"].take();
    hold["current_shelf_lib"].take();

    editor.update(hold)?;

    retarget_hold(editor, hold_id)
}

/// json_query order by clause for sorting holds by next to be targeted.
pub fn json_query_order_by_targetable() -> EgValue {
    eg::array! [
        {"class": "pgt", "field": "hold_priority"},
        {"class": "ahr", "field": "cut_in_line",
            "direction": "desc", "transform": "coalesce", params: vec!["f"]},
        {"class": "ahr", "field": "selection_depth", "direction": "desc"},
        {"class": "ahr", "field": "request_time"}
    ]
}

/// Returns a list of non-canceled / non-fulfilled holds linked to the
/// provided copy by virtue of sharing a metarecord, IOW, holds that
/// could potentially target the provided copy.  Under the metarecord
/// umbrella, this covers all hold types.
///
/// The list is sorted in the order they would ideally be fulfilled.
pub fn related_to_copy(
    editor: &mut Editor,
    copy_id: i64,
    pickup_lib: Option<i64>,
    frozen: Option<bool>,
    usr: Option<i64>,
    on_shelf: Option<bool>,
) -> EgResult<Vec<MinimalHold>> {
    // "rhrr" / reporter.hold_request_record calculates the bib record
    // linked to a hold regardless of hold type in advance for us.
    // Leverage that.  It's fast.
    //
    // TODO reporter.hold_request_record is not currently updated
    // when items/call numbers are transferred to another call
    // number/record.

    // copy
    //   => call_number
    //     => metarecord source map
    //       => reporter.hold_request_record
    //         => hold
    //           => user
    //             => profile group
    let from = eg::hash! {
        "acp": {
            "acn": {
                "join": {
                    "mmrsm": {
                        // ON acn.record = mmrsm.source
                        "fkey": "record",
                        "field": "source",
                        "join": {
                            "rhrr": {
                                // ON mmrsm.source = rhrr.bib_record
                                "fkey": "source",
                                "field": "bib_record",
                                "join": {
                                    "ahr": {
                                        "join": {
                                            "au": {
                                                "field": "id",
                                                "fkey": "usr",
                                                "join": "pgt"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    let mut query = eg::hash! {
        "select": {
            "ahr": [
                "id",
                "target",
                "hold_type",
                "selection_depth",
                "request_time",
                "cut_in_line",
                "pickup_lib",
            ],
            "pgt": ["hold_priority"]
        },
        "from": from,
        "where": {
            "+acp": {"id": copy_id},
            "+ahr": {
                "cancel_time": eg::NULL,
                "fulfillment_time" => eg::NULL,
            }
        },
        "order_by": json_query_order_by_targetable(),
    };

    if let Some(v) = pickup_lib {
        query["where"]["+ahr"]["pickup_lib"] = EgValue::from(v);
    }

    if let Some(v) = usr {
        query["where"]["+ahr"]["usr"] = EgValue::from(v);
    }

    if let Some(v) = frozen {
        let s = if v { "t" } else { "f" };
        query["where"]["+ahr"]["frozen"] = EgValue::from(s);
    }

    // Limiting on wether current_shelf_lib == pickup_lib.
    if let Some(v) = on_shelf {
        if v {
            query["where"]["+ahr"]["current_shelf_lib"] = eg::hash! {"=": {"+ahr": "pickup_lib"}};
        } else {
            query["where"]["+ahr"]["-or"] = eg::array! [
                {"current_shelf_lib": eg::NULL},
                {"current_shelf_lib": {"!=": {"+ahr": "pickup_lib"}}}
            ];
        }
    }

    let mut list = Vec::new();
    for val in editor.json_query(query)? {
        // We know the hold type returned from the database is valid.
        let hold_type = HoldType::try_from(val["hold_type"].as_str().unwrap()).unwrap();

        let h = MinimalHold {
            id: val.id()?,
            target: val["target"].int()?,
            pickup_lib: val["pickup_lib"].int()?,
            hold_type,
            active: true,
        };

        list.push(h);
    }

    Ok(list)
}

/// Count of open holds that target a bib record or any of its
/// associated call numbers, copies, etc.
///
/// TODO metarecords
pub fn record_hold_counts(
    editor: &mut Editor,
    rec_id: i64,
    pickup_lib_descendant: Option<i64>,
) -> EgResult<i64> {
    let mut query = eg::hash! {
        "select": {
            "ahr": [{"column": "id", "transform": "count", "alias": "count"}]
        },
        "from": {
            "ahr": {
                "rhrr": {
                    "fkey": "id",
                    "field": "id",
                }
            }
        },
        "where": {
            "+ahr": {
                "cancel_time": EgValue::Null,
                "fulfillment_time": EgValue::Null,
            },
            "+rhrr": {
                "bib_record": rec_id
            }
        }
    };

    if let Some(plib) = pickup_lib_descendant {
        query["where"]["+ahr"]["pickup_lib"] = eg::hash! {
            "in": {
                "select": {
                    "aou": [{
                        "column": "id",
                        "transform": "actor.org_unit_descendants",
                        "result_field": "id"
                    }]
                },
                "from": "aou",
                "where": {"id": plib}
            }
        }
    }

    let result = editor
        .json_query(query)?
        .pop()
        .ok_or("record_hold_counts() return no results")?;

    result["count"].int()
}

/// Returns true if the record/metarecord in question has at least
/// one holdable copy.
pub fn record_has_holdable_copy(editor: &mut Editor, rec_id: i64, is_meta: bool) -> EgResult<bool> {
    let key = if is_meta { "metarecord" } else { "record" };
    let func = format!("asset.{key}_has_holdable_copy");

    let query = eg::hash! {"from": [func.as_str(), rec_id]};

    let data = editor
        .json_query(query)?
        .pop()
        .ok_or("json_query returned zero results")?;

    Ok(data[&func].boolish())
}
