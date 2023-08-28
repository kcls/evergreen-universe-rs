use crate::editor::Editor;
use crate::util::{json_int, json_bool, json_string};
use crate::common::org;
use crate::common::trigger;
use crate::result::EgResult;
use crate::date;
use crate::common::settings::Settings;
use json::JsonValue;
use chrono::Duration;
use std::fmt;
use std::collections::HashMap;

const JSON_NULL: JsonValue = JsonValue::Null;

/// Slimmed down copy.
pub struct PotentialCopy {
    id: i64,
    status: i64,
    circ_lib: i64,
}

/// Tracks info for a single hold target run.
pub struct HoldTargetContext {
    /// Hold ID
    hold_id: i64,

    hold: JsonValue,

    /// Targeted copy ID.
    ///
    /// If we have a target, we succeeded.
    target: Option<i64>,

    /// Previously targeted copy ID.
    old_target: Option<i64>,

    /// Lets the caller know we found the copy they were intersted in.
    found_copy: bool,

    /// Number of potentially targetable copies
    eligible_copy_count: u64,

    copies: Vec<PotentialCopy>,

    // Final set of potential copies, including those that may not be
    // currently targetable, that may be eligible for recall processing.
    recall_copies: Vec<PotentialCopy>,

    // Copies that are targeted, but could contribute to pickup lib
    // hard (foreign) stalling.  These are Available-status copies.
    in_use_copies: Vec<PotentialCopy>,

    /// Maps copy IDs to their hold proximity
    copy_prox_map: HashMap<i64, u64>,
}

impl HoldTargetContext {
    fn new(hold_id: i64, hold: JsonValue) -> HoldTargetContext {
        HoldTargetContext {
            hold_id,
            hold,
            copies: Vec::new(),
            recall_copies: Vec::new(),
            in_use_copies: Vec::new(),
            copy_prox_map: HashMap::new(),
            eligible_copy_count: 0,
            target: None,
            old_target: None,
            found_copy: false,
        }
    }
}

/// Targets a batch of holds.
pub struct HoldTargeter {

    /// Editor is required, but stored as an Option so we can give it
    /// back to the caller when we're done in case the caller has
    /// additional work to perform before comitting changes.
    editor: Option<Editor>,

    settings: Settings,

    /// Hold in process -- mainly for logging.
    hold_id: i64,

    holds_to_target: Option<Vec<i64>>,

    retarget_time: Option<String>,
    retarget_interval: Option<String>,
    soft_retarget_interval: Option<String>,
    soft_retarget_time: Option<String>,
    next_check_interval: Option<String>,

    /// IDs of org units closed both now and at the next target time.
    closed_orgs: Vec<i64>,

    /// Copy statuses that are hopeless prone.
    hopeless_prone_statuses: Vec<i64>,

    /// Number of parallel slots; 0 means we are not running in parallel.
    parallel_count: u8,

    /// Which parallel slot do we occupy; 0 is none.
    parallel_slot: u8,

    /// Target holds newest first by request date.
    newest_first: bool,

    transaction_manged_externally: bool,
}

impl fmt::Display for HoldTargeter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "targeter: [hold={}]", self.hold_id)
    }
}

impl HoldTargeter {

    pub fn new(editor: Editor) -> HoldTargeter {
        let settings = Settings::new(&editor);

        HoldTargeter {
            editor: Some(editor),
            settings,
            holds_to_target: None,
            hold_id: 0,
            retarget_time: None,
            retarget_interval: None,
            soft_retarget_interval: None,
            soft_retarget_time: None,
            next_check_interval: None,
            parallel_count: 0,
            parallel_slot: 0,
            newest_first: false,
            closed_orgs: Vec::new(),
            hopeless_prone_statuses: Vec::new(),
            transaction_manged_externally: false,
        }
    }

    /// Set this to true if the targeter should avoid making any
    /// transaction begin / commit calls.
    ///
    /// The transaction may still be rolled back in cases where an action
    /// failed, thus killing the transaction anyway.
    ///
    /// This is useful if the caller wants to target a hold within an
    /// existing transaction.
    pub fn transaction_manged_externally(&mut self, val: bool) {
        self.transaction_manged_externally = val;
    }

    pub fn holds_to_target(&self) -> &Vec<i64> {
        match self.holds_to_target.as_ref() {
            Some(r) => r,
            None => panic!("find_holds_to_target() must be called first"),
        }
    }

    /// Panics if we don't have an editor.
    pub fn editor(&mut self) -> &mut Editor {
        self.editor
            .as_mut()
            .unwrap_or_else(|| panic!("HoldTargeter needs an editor!"))
    }

    pub fn set_editor(&mut self, editor: Editor) {
        self.editor = Some(editor);
    }

    pub fn take_editor(&mut self) -> Editor {
        self.editor.take().unwrap()
    }

    pub fn set_retarget_interval(&mut self, intvl: &str) {
        self.retarget_interval = Some(intvl.to_string());
    }

    pub fn set_soft_retarget_interval(&mut self, intvl: &str) {
        self.soft_retarget_interval = Some(intvl.to_string());
    }

    pub fn set_next_check_interval(&mut self, intvl: &str) {
        self.next_check_interval = Some(intvl.to_string());
    }

    pub fn init(&mut self) -> EgResult<()> {

        let mut retarget_intvl_bind = None;
        let retarget_intvl = if let Some(intvl) = self.retarget_interval.as_ref() {
            intvl
        } else {

            let query = json::object! {
                "name": "circ.holds.retarget_interval",
                "enabled": "t"
            };

            if let Some(intvl) = self.editor().search("cgf", query)?.get(0) {
                retarget_intvl_bind = Some(json_string(&intvl["value"])?);
                retarget_intvl_bind.as_ref().unwrap()
            } else {
                // If all else fails, use a one day retarget interval.
                "24h"
            }
        };

        let retarget_secs = date::interval_to_seconds(retarget_intvl)?;

        let rt = date::to_iso(&(date::now_local() - Duration::seconds(retarget_secs)));

        log::info!("{self} using retarget time: {rt}");

        self.retarget_time = Some(rt);

        if let Some(sri) = self.soft_retarget_interval.as_ref() {
            let secs = date::interval_to_seconds(sri)?;
            let srt = date::to_iso(&(date::now_local() - Duration::seconds(secs)));

            log::info!("{self} using soft retarget time: {srt}");

            self.soft_retarget_time = Some(srt);
        }

        // Holds targeted in the current targeter instance
        // won't be retargeted until the next check date.  If a
        // next_check_interval is provided it overrides the
        // retarget_interval.
        let next_check_secs = match self.next_check_interval.as_ref() {
            Some(intvl) => date::interval_to_seconds(intvl)?,
            None => retarget_secs,
        };

        let next_check_date = date::now_local() + Duration::seconds(next_check_secs);
        let next_check_time = date::to_iso(&next_check_date);

        log::info!("{self} next check time {next_check_time}");

        // An org unit is considered closed for retargeting purposes
        // if it's closed both now and at the next re-target date.
        let query = json::object! {
            "-and": [{
                "close_start": {"<=": "now"},
                "close_end": {">=": "now"}
            }, {
                "close_start": {"<=": next_check_time.as_str()},
                "close_end": {">=": next_check_time.as_str()}
            }]
        };

        let closed_orgs = self.editor().search("aoucd", query)?;

        for co in closed_orgs {
            self.closed_orgs.push(json_int(&co["org_unit"])?);
        }

        for stat in self.editor().search("ccs", json::object! {"hopeless_prone":"t"})? {
            self.hopeless_prone_statuses.push(json_int(&stat["id"])?);
        }

        Ok(())
    }

    /// Find holds that need to be processed.
    ///
    /// When targeting a known hold ID, this step can be skipped.
    pub fn find_holds_to_target(&mut self) -> EgResult<()> {
        let mut query = json::object! {
            "select": {"ahr": ["id"]},
            "from": "ahr",
            "where": {
                "capture_time": JSON_NULL,
                "fulfillment_time": JSON_NULL,
                "cancel_time": JSON_NULL,
                "frozen": "f"
            },
            "order_by": [
                {"class": "ahr", "field": "selection_depth", "direction": "DESC"},
                {"class": "ahr", "field": "request_time"},
                {"class": "ahr", "field": "prev_check_time"}
            ]
        };

        // Target holds that have no prev_check_time or those whose
        // re-target time has come.  If a soft_retarget_time is
        // specified, that acts as the boundary.  Otherwise, the
        // retarget_time is used.
        let start_time = if let Some(t) = self.soft_retarget_time.as_ref() {
            t.as_str()
        } else {
            self.retarget_time.as_ref().unwrap().as_str()
        };

        query["where"]["-or"] = json::array! [
            {"prev_check_time": JSON_NULL},
            {"prev_check_time": {"<=": start_time}},
        ];

        // Parallel < 1 means no parallel
        let parallel = if self.parallel_count > 0 {
            self.parallel_count
        } else {
            0
        };

        if parallel > 0 {
            // In parallel mode, we need to also grab the metarecord for each hold.

            query["from"] = json::object! {
                "ahr": {
                    "rhrr": {
                        "fkey": "id",
                        "field": "id",
                        "join": {
                            "mmrsm": {
                                "field": "source",
                                "fkey": "bib_record"
                            }
                        }
                    }
                }
            };

            // In parallel mode, only process holds within the current
            // process whose metarecord ID modulo the parallel targeter
            // count matches our paralell targeting slot.  This ensures
            // that no 2 processes will be operating on the same
            // potential copy sets.
            //
            // E.g. Running 5 parallel and we are slot 3 (0-based slot
            // 2) of 5, process holds whose metarecord ID's are 2, 7,
            // 12, 17, ... WHERE MOD(mmrsm.id, 5) = 2

            // Slots are 1-based at the API level, but 0-based for modulo.
            let slot = self.parallel_slot - 1;

            query["where"]["+mmrsm"] = json::object! {
                "metarecord": {
                    "=": {
                        "transform": "mod",
                        "value": slot,
                        "params": [parallel]
                    }
                }
            };
        }

        // Newest-first sorting cares only about hold create_time.
        if self.newest_first {
            query["order_by"] = json::array! [{
                "class": "ahr",
                "field": "request_time",
                "direction": "DESC"
            }];
        }

        // NOTE The perl code runs this query in substream mode.
        // At time of writing, the Rust editor has no substream mode.
        // It seems less critical for Redis, but can be added if needed.
        let holds = self.editor().json_query(query)?;

        // Hold IDs better be numeric...
        self.holds_to_target = Some(
            holds.iter().map(|h| json_int(&h["id"]).unwrap()).collect());

        Ok(())
    }

    pub fn exit(&mut self, msg: &str) -> EgResult<()> {
        log::error!("{self} targeting stopped: {msg}");
        return Err(self.editor().die_event_msg(msg));
    }

    /// Caller may use this method directly when targeting only one hold.
    ///
    /// self.init() is still required.
    pub fn target_hold(&mut self, hold_id: i64) -> EgResult<HoldTargetContext> {
        self.hold_id = hold_id;

        if !self.transaction_manged_externally {
            self.editor().xact_begin()?;
        }

        let hold = match self.editor().retrieve("ahr", hold_id)? {
            Some(h) => h,
            None => return Err(self.editor().die_event()),
        };

        if !hold["capture_time"].is_null() ||
            !hold["cancel_time"].is_null() ||
            !hold["fulfillment_time"].is_null() ||
            json_bool(&hold["frozen"]) {

            self.exit("Hold is not eligible for targeting")?;
        }

        let mut context = HoldTargetContext::new(hold_id, hold);

        self.handle_expired_hold(&mut context)?;

        // TODO

        if !self.transaction_manged_externally {
            // Use commit() here to force a disconnect from the cstore
            // backend so the backends have a chance to cycle on large
            // data sets.
            self.editor().commit()?;
        }

        Ok(context)
    }

    /// Cancel expired holds and kick off the A/T no-target event.  Returns
    /// true (i.e. keep going) if the hold is not expired.  Returns false if
    /// the hold is canceled or a non-recoverable error occcurred.
    fn handle_expired_hold(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let hold = &context.hold;

        if let Some(etime) = hold["expire_time"].as_str() {
            let ex_time = date::parse_datetime(&etime)?;

            if ex_time > date::now_local() {
                // Hold has not yet expired.
                return Ok(());
            }
        } else {
            // Hold has no expire time.
            return Ok(());
        }

        // -- Hold is expired --

        let hold = &mut context.hold;
        hold["cancel_time"] = json::from("now");
        hold["cancel_cause"] = json::from(1); // un-targeted expiration

        self.editor().update(&hold)?;

        let pl_lib = json_int(&hold["pickup_lib"])?;

        trigger::create_events_for_object(
            self.editor(),
            "hold_request.cancel.expire_no_target",
            hold,
            pl_lib,
            None,
            None,
            false
        )?;

        // Commit after we've created events so all of our writes
        // occur within the same transaction.
        self.editor().xact_commit()?;

        self.exit("Hold is expired")
    }
}

