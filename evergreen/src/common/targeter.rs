use crate::common::settings::Settings;
use crate::common::trigger;
use crate::constants as C;
use crate::date;
use crate::editor::Editor;
use crate::result::{EgError, EgResult};
use crate::util::{json_bool, json_int, json_string};
use chrono::Duration;
use json::JsonValue;
use std::collections::HashMap;
use std::fmt;

const JSON_NULL: JsonValue = JsonValue::Null;

/// Slimmed down copy.
#[derive(Debug)]
pub struct PotentialCopy {
    id: i64,
    status: i64,
    circ_lib: i64,
    proximity: i64,
    already_targeted: bool,
}

/// Tracks info for a single hold target run.
///
/// Some of these values should in theory be Options instesad of bare
/// i64's, but testing for "0" works just as well and requires (overall,
/// I believe) slightly less overhead.
#[derive(Debug)]
pub struct HoldTargetContext {
    /// Hold ID
    hold_id: i64,

    hold: JsonValue,

    /// Targeted copy ID.
    ///
    /// If we have a target, we succeeded.
    target: i64,

    /// Previously targeted copy ID.
    old_target: i64,

    /// Caller is specifically interested in this copy.
    find_copy: i64,

    /// Previous copy.
    previous_copy_id: i64,

    /// Previous copy that we know to be potentially targetable.
    valid_previous_copy: Option<PotentialCopy>,

    /// Lets the caller know we found the copy they were intersted in.
    found_copy: bool,

    /// Number of potentially targetable copies
    eligible_copy_count: usize,

    copies: Vec<PotentialCopy>,

    // Final set of potential copies, including those that may not be
    // currently targetable, that may be eligible for recall processing.
    recall_copies: Vec<PotentialCopy>,

    // Copies that are targeted, but could contribute to pickup lib
    // hard (foreign) stalling.  These are Available-status copies.
    already_targeted_copies: Vec<PotentialCopy>,

    /// Maps proximities to the weighted list of copy IDs.
    weighted_prox_map: HashMap<i64, Vec<i64>>,
}

impl HoldTargetContext {
    fn new(hold_id: i64, hold: JsonValue) -> HoldTargetContext {
        HoldTargetContext {
            hold_id,
            hold,
            copies: Vec::new(),
            recall_copies: Vec::new(),
            already_targeted_copies: Vec::new(),
            weighted_prox_map: HashMap::new(),
            eligible_copy_count: 0,
            target: 0,
            old_target: 0,
            find_copy: 0,
            valid_previous_copy: None,
            previous_copy_id: 0,
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
        let retarget_intvl_bind;
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

        for stat in self
            .editor()
            .search("ccs", json::object! {"hopeless_prone":"t"})?
        {
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
        self.holds_to_target = Some(holds.iter().map(|h| json_int(&h["id"]).unwrap()).collect());

        Ok(())
    }

    /// Rollback the active transaction.
    ///
    /// Unlike begin/commit, we don't care if our transaction is
    /// externally managed, because its assumed a rollback needs to
    /// occur.
    pub fn rollback(&mut self, msg: &str) -> EgResult<()> {
        log::error!("{self} targeting stopped: {msg}");
        return Err(self.editor().die_event_msg(msg));
    }

    pub fn commit(&mut self) -> EgResult<()> {
        if !self.transaction_manged_externally {
            // Use commit() here to do a commit+disconnect from the cstore
            // backend so the backends have a chance to cycle on large
            // data sets.
            self.editor().commit()?;
        }

        Ok(())
    }

    /// Update our in-process hold with the provided key/value pairs.
    ///
    /// Refresh our copy of the hold once updated to pick up DB-generated
    /// values (dates, etc.).
    fn update_hold(&mut self, context: &mut HoldTargetContext, values: JsonValue) -> EgResult<()> {
        for (field, _) in values.entries() {
            if field == "id" {
                // nope
                continue;
            }
            context.hold[field] = values[field].to_owned();
        }

        self.editor().update(context.hold.clone())?;

        // this hold id must exist.
        context.hold = self
            .editor()
            .retrieve("ahr", context.hold_id)?
            .ok_or("Cannot find hold")?;

        Ok(())
    }

    /// If the hold is not eligible (frozen, etc.) for targeting, rollback
    /// the transaction and return an error.
    fn check_hold_eligible(&mut self, context: &HoldTargetContext) -> EgResult<()> {
        let hold = &context.hold;

        if !hold["capture_time"].is_null()
            || !hold["cancel_time"].is_null()
            || !hold["fulfillment_time"].is_null()
            || json_bool(&hold["frozen"])
        {
            self.rollback("Hold is not eligible for targeting")?;
        }

        Ok(())
    }

    /// Cancel expired holds and kick off the A/T no-target event.
    ///
    /// Returns true if the hold was marked as expired, indicating no
    /// further targeting is needed.
    fn hold_is_expired(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        if let Some(etime) = context.hold["expire_time"].as_str() {
            let ex_time = date::parse_datetime(&etime)?;

            if ex_time > date::now_local() {
                // Hold has not yet expired.
                return Ok(false);
            }
        } else {
            // Hold has no expire time.
            return Ok(false);
        }

        // -- Hold is expired --
        let values = json::object! {
            "cancel_time": "now",
            "cancel_cause": 1, // un-targeted expiration
        };

        self.update_hold(context, values)?;

        let pl_lib = json_int(&context.hold["pickup_lib"])?;

        // Create events that will be fired/processed later.
        trigger::create_events_for_object(
            self.editor(),
            "hold_request.cancel.expire_no_target",
            &context.hold,
            pl_lib,
            None,
            None,
            false,
        )?;

        // Commit after we've created events so all of our writes
        // occur within the same transaction.
        self.commit()?;

        Ok(true)
    }

    /// Find potential copies for mapping/targeting and add them to
    /// the copies list on our context.
    fn get_hold_copies(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let hold = &context.hold;

        let hold_target = json_int(&hold["target"]).unwrap(); // required.
        let hold_type = hold["hold_type"].as_str().unwrap(); // required.
        let org_unit = json_int(&hold["selection_ou"]).unwrap(); // required
        let org_depth = json_int(&hold["selection_depth"]).unwrap_or(0); // not required

        let mut query = json::object! {
            "select": {
                "acp": ["id", "status", "circ_lib"],
                "ahr": ["current_copy"]
            },
            "from": {
                "acp": {
                    // Tag copies that are in use by other holds so we don't
                    // try to target them for our hold.
                    "ahr": {
                        "type": "left",
                        "fkey": "id", // acp.id
                        "field": "current_copy",
                        "filter": {
                            "fulfillment_time": JSON_NULL,
                            "cancel_time": JSON_NULL,
                            "id": {"!=": context.hold_id},
                        }
                    }
                }
            },
            "where": {
                "+acp": {
                    "deleted": "f",
                    "circ_lib": {
                        "in": {
                            "select": {
                                "aou": [{
                                    "transform": "actor.org_unit_descendants",
                                    "column": "id",
                                    "result_field": "id",
                                    "params": [org_depth],
                                }],
                                },
                            "from": "aou",
                            "where": {"id": org_unit},
                        }
                    }
                }
            }
        };

        if hold_type != "R" && hold_type != "F" {
            // Add the holdability filters to the copy query, unless
            // we're processing a Recall or Force hold, which bypass most
            // holdability checks.

            query["from"]["acp"]["acpl"] = json::object! {
                "field": "id",
                "filter": {"holdable": "t", "deleted": "f"},
                "fkey": "location",
            };

            query["from"]["acp"]["ccs"] = json::object! {
                "field": "id",
                "filter": {"holdable": "t"},
                "fkey": "status",
            };

            query["where"]["+acp"]["holdable"] = json::from("t");

            if json_bool(&hold["mint_condition"]) {
                query["where"]["+acp"]["mint_condition"] = json::from("t");
            }
        }

        if hold_type != "C" && hold_type != "I" && hold_type != "P" {
            // For volume and higher level holds, avoid targeting copies that
            // act as instances of monograph parts.

            query["from"]["acp"]["acpm"] = json::object! {
                "type": "left",
                "field": "target_copy",
                "fkey": "id"
            };

            query["where"]["+acpm"]["id"] = JSON_NULL;
        }

        // Add the target filters
        if hold_type == "C" || hold_type == "R" || hold_type == "F" {
            query["where"]["+acp"]["id"] = json::from(hold_target);
        } else if hold_type == "V" {
            query["where"]["+acp"]["call_number"] = json::from(hold_target);
        } else if hold_type == "P" {
            query["from"]["acp"]["acpm"] = json::object! {
                "field" : "target_copy",
                "fkey" : "id",
                "filter": {"part": hold_target},
            };
        } else if hold_type == "I" {
            query["from"]["acp"]["sitem"] = json::object! {
                "field" : "unit",
                "fkey" : "id",
                "filter": {"issuance": hold_target},
            };
        } else if hold_type == "T" {
            query["from"]["acp"]["acn"] = json::object! {
                "field" : "id",
                "fkey" : "call_number",
                "join": {
                    "bre": {
                        "field" : "id",
                        "filter": {"id": hold_target},
                        "fkey"  : "record"
                    }
                }
            };
        } else {
            // Metarecord hold

            query["from"]["acp"]["acn"] = json::object! {
                "field": "id",
                "fkey": "call_number",
                "join": {
                    "bre": {
                        "field": "id",
                        "fkey": "record",
                        "join": {
                            "mmrsm": {
                                "field": "source",
                                "fkey": "id",
                                "filter": {"metarecord": hold_target},
                            }
                        }
                    }
                }
            };

            if let Some(formats) = hold["holdable_formats"].as_str() {
                // Compile the JSON-encoded metarecord holdable formats
                // to an Intarray query_int string.

                let query_ints = self.editor().json_query(json::object! {
                    "from": ["metabib.compile_composite_attr", formats]
                })?;

                if let Some(query_int) = query_ints.get(0) {
                    // Only pull potential copies from records that satisfy
                    // the holdable formats query.
                    if let Some(qint) = query_int["metabib.compile_composite_attr"].as_str() {
                        query["from"]["acp"]["acn"]["join"]["bre"]["join"]["mravl"] = json::object! {
                            "field": "source",
                            "fkey": "id",
                            "filter": {"vlist": {"@@": qint}}
                        }
                    }
                }
            }
        }

        let mut found_copy = false;
        context.copies = self
            .editor()
            .json_query(query)?
            .iter()
            .map(|c| {
                // While we're looping, see if we found the copy the
                // caller was interested in.
                let id = json_int(&c["id"]).unwrap();
                if id == context.find_copy {
                    found_copy = true;
                }

                PotentialCopy {
                    id,
                    status: json_int(&c["status"]).unwrap(),
                    circ_lib: json_int(&c["circ_lib"]).unwrap(),
                    proximity: -1,
                    already_targeted: !c["current_copy"].is_null(),
                }
            })
            .collect();

        context.eligible_copy_count = context.copies.len();
        context.found_copy = found_copy;

        log::info!("{self} {} potential copies", context.eligible_copy_count);

        Ok(())
    }

    /// Tell the DB to update the list of potential copies for our hold
    /// based on the copies we just found.
    fn update_copy_maps(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let ints = context
            .copies
            .iter()
            .map(|c| format!("{}", c.id))
            .collect::<Vec<String>>()
            .join(",");

        // "{1,2,3}"
        let ints = format!("{{{ints}}}");

        let query = json::object! {
            "from": [
                "action.hold_request_regen_copy_maps",
                context.hold_id,
                ints
            ]
        };

        self.editor().json_query(query).map(|_| ())
    }

    /// Set the hopeless date on a hold when needed.
    ///
    /// If no copies were found and hopeless date is not set,
    /// then set it. Otherwise, all found copies have a hopeless
    /// status, set the hold as hopeless.  Otherwise, clear the
    /// date if set.
    fn handle_hopeless_date(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let marked_hopeless = !context.hold["hopeless_date"].is_null();

        if context.copies.len() == 0 {
            if !marked_hopeless {
                log::info!("{self} Marking hold as hopeless");
                return self.update_hold(context, json::object! {"hopeless_date": "now"});
            }
        }

        // Hope left in any of the statuses?
        let we_have_hope = context
            .copies
            .iter()
            .any(|c| !self.hopeless_prone_statuses.contains(&c.status));

        if marked_hopeless {
            if we_have_hope {
                log::info!("{self} Removing hopeless date");
                return self.update_hold(context, json::object! {"hopeless_date": JSON_NULL});
            }
        } else if !we_have_hope {
            log::info!("{self} Marking hold as hopeless");
            return self.update_hold(context, json::object! {"hopeless_date": "now"});
        }

        Ok(())
    }

    /// If the hold has no usable copies, commit the transaction and return
    /// true (i.e. stop targeting), false otherwise.
    fn hold_has_no_copies(
        &mut self,
        context: &mut HoldTargetContext,
        force: bool,
        process_recalls: bool,
    ) -> EgResult<bool> {
        if !force {
            // If 'force' is set, the caller is saying that all copies have
            // failed.  Otherwise, see if we have any copies left to inspect.
            if context.copies.len() > 0 || context.valid_previous_copy.is_some() {
                return Ok(false);
            }
        }

        // At this point, all copies have been inspected and none
        // have yielded a targetable item.

        if process_recalls {
            // Regardless of whether we find a circulation to recall,
            // we want to clear the hold below.
            self.process_recalls(context)?;
        }

        let values = json::object! {
            "current_copy": JSON_NULL,
            "prev_check_time": "now"
        };

        self.update_hold(context, values)?;
        self.commit()?;

        Ok(true)
    }

    /// Attempts to recall a circulation so its item may be used to
    /// fill the hold once returned.
    ///
    /// Note that recalling (or not) a circ has no direct impact on the hold.
    fn process_recalls(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        if context.recall_copies.len() == 0 {
            return Ok(());
        }

        let pickup_lib = json_int(&context.hold["pickup_lib"])?;

        let recall_threshold = self
            .settings
            .get_value_at_org("circ.holds.recall_threshold", pickup_lib)?;

        let recall_threshold = match json_string(&recall_threshold) {
            Ok(t) => t,
            Err(_) => return Ok(()), // null / not set
        };

        let return_interval = self
            .settings
            .get_value_at_org("circ.holds.recall_return_interval", pickup_lib)?;

        let return_interval = match json_string(&return_interval) {
            Ok(t) => t,
            Err(_) => return Ok(()), // null / not set
        };

        let thresh_intvl_secs = date::interval_to_seconds(&recall_threshold)?;
        let return_intvl_secs = date::interval_to_seconds(&return_interval)?;

        let copy_ids = context
            .recall_copies
            .iter()
            .map(|c| c.id)
            .collect::<Vec<i64>>();

        // See if we have a circulation linked to our recall copies
        // that we can recall.
        let query = json::object! {
            "target_copy": copy_ids,
            "checkin_time": JSON_NULL,
            "duration": {">": recall_threshold}
        };

        let ops = json::object! {
            "order_by": [{"class": "circ", "field": "due_date"}],
            "limit": 1
        };

        let mut circs = self.editor().search_with_ops("circ", query, ops)?;

        let mut circ = match circs.pop() {
            Some(c) => c,
            // Tried our best to recall a circ but could not find one.
            None => return Ok(()),
        };

        log::info!("{self} recalling circ {}", circ["id"]);

        let old_due_date = date::parse_datetime(circ["due_date"].as_str().unwrap())?;
        let xact_start_date = date::parse_datetime(circ["xact_start"].as_str().unwrap())?;
        let thresh_date = xact_start_date + Duration::seconds(thresh_intvl_secs);
        let mut return_date = date::now_local() + Duration::seconds(return_intvl_secs);

        // Give the user a new due date of either a full recall threshold,
        // or the return interval, whichever is further in the future.
        if thresh_date > return_date {
            return_date = thresh_date;
        }

        // ... but avoid exceeding the old due date.
        if return_date > old_due_date {
            return_date = old_due_date;
        }

        circ["due_date"] = json::from(date::to_iso(&return_date));
        circ["renewal_remaining"] = json::from(0);

        let mut fine_rules = self
            .settings
            .get_value_at_org("circ.holds.recall_fine_rules", pickup_lib)?
            .clone();

        log::debug!("{self} recall fine rules: {}", fine_rules);

        // fine_rules => [fine, interval, max];
        if fine_rules.is_array() && fine_rules.len() == 3 {
            circ["max_fine"] = fine_rules.pop();
            circ["fine_interval"] = fine_rules.pop();
            circ["recurring_fine"] = fine_rules.pop();
        }

        // Create events that will be fired/processed later.  Run this
        // before update(circ) so the editor call can consume the circ.
        // Trigger gets its values for 'target' from the target value
        // provided, so it's OK to create the trigger events before the
        // circ is updated in the database.
        trigger::create_events_for_object(
            self.editor(),
            "circ.recall.target",
            &circ,
            json_int(&circ["circ_lib"])?,
            None,
            None,
            false,
        )?;

        self.editor().update(circ)?;

        Ok(())
    }

    /// Trim the copy list to those that are currently targetable and
    /// move checked out items to the recall list.
    fn filter_copies_by_status_and_targeted(&self, context: &mut HoldTargetContext) {
        let mut targetable = Vec::new();

        while let Some(copy) = context.copies.pop() {
            if copy.status == C::COPY_STATUS_CHECKED_OUT {
                context.recall_copies.push(copy);
                continue;
            }

            if copy.already_targeted {
                context.already_targeted_copies.push(copy);
                continue;
            }

            if copy.status == C::COPY_STATUS_AVAILABLE || copy.status == C::COPY_STATUS_RESHELVING {
                targetable.push(copy);
            }
        }

        context.copies = targetable;
    }

    /// Removes copies for consideration when they live at a closed org unit
    /// and settings prevent targeting when closed.
    fn filter_closed_date_copies(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let pickup_lib = json_int(&context.hold["pickup_lib"])?;
        let mut targetable = Vec::new();

        while let Some(copy) = context.copies.pop() {
            if self.closed_orgs.contains(&copy.circ_lib) {
                let setting = if copy.circ_lib == pickup_lib {
                    "circ.holds.target_when_closed_if_at_pickup_lib"
                } else {
                    "circ.holds.target_when_closed"
                };

                let value = self.settings.get_value_at_org(setting, copy.circ_lib)?;

                if json_bool(&value) {
                    log::info!("{self} skipping copy at closed org unit {}", copy.circ_lib);
                    continue;
                }
            }

            targetable.push(copy);
        }

        context.copies = targetable;

        Ok(())
    }

    /// Returns true if the on-DB permit test says this copy is permitted.
    fn copy_is_permitted(
        &mut self,
        context: &mut HoldTargetContext,
        copy_id: i64,
    ) -> EgResult<bool> {
        let query = json::object! {
            "from": [
                "action.hold_retarget_permit_test",
                context.hold["pickup_lib"].clone(),
                context.hold["request_lib"].clone(),
                copy_id,
                context.hold["usr"].clone(),
                context.hold["requestor"].clone(),
            ]
        };

        let result = self.editor().json_query(query)?;

        if result.len() > 0 && json_bool(&result[0]["success"]) {
            return Ok(true);
        }

        // Copy is non-viable.  Remove it from our list.
        if let Some(pos) = context.copies.iter().position(|c| c.id == copy_id) {
            context.copies.remove(pos);
        }

        Ok(false)
    }

    /// Returns true if we have decided to retarget the existing copy.
    ///
    /// Otherwise, sets aside the previously targeted copy in case in
    /// may be of use later... and returns false.
    fn inspect_previous_target(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        let prev_copy = match json_int(&context.hold["current_copy"]) {
            Ok(c) => c,
            Err(_) => return Ok(false), // value was null
        };

        context.previous_copy_id = prev_copy;

        if !context.copies.iter().any(|c| c.id == prev_copy) {
            return Ok(false);
        }

        let mut soft_retarget = false;
        if self.soft_retarget_time.is_some() {
            // A hold is soft-retarget-able if its prev_check_time is
            // later then the retarget_time, i.e. it sits between the
            // soft_retarget_time and the retarget_time.

            if let Some(prev_check_time) = context.hold["prev_check_time"].as_str() {
                if let Some(retarget_time) = self.retarget_time.as_deref() {
                    soft_retarget = prev_check_time > retarget_time;
                }
            }
        }

        let mut retain_prev = false;
        if soft_retarget {
            // In soft-retarget mode, exit early if the existing copy is valid.
            if self.copy_is_permitted(context, prev_copy)? {
                log::info!("{self} retaining previous copy in soft-retarget");
                return Ok(true);
            }

            log::info!("{self} previous copy is no longer viable.  Retargeting");
        } else {
            // Previously targeted copy may yet be useful.
            retain_prev = true;
        }

        // Remove the previous copy from the working set of potential
        // copies.  It will be revisited later if needed.
        if let Some(pos) = context.copies.iter().position(|c| c.id == prev_copy) {
            let copy = context.copies.remove(pos);
            if retain_prev {
                context.valid_previous_copy = Some(copy);
            }
        }

        Ok(false)
    }

    /// Store info in the database about the fact that this hold was
    /// not captured.
    fn log_unfulfilled_hold(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        if context.previous_copy_id == 0 {
            return Ok(());
        }

        log::info!(
            "{self} hold was not captured with previously targeted copy {}",
            context.previous_copy_id
        );

        let circ_lib = if let Some(copy) = context.valid_previous_copy.as_ref() {
            copy.circ_lib
        } else {
            // We don't have a handle on the previous copy to get its
            // circ lib.  Fetch it.

            let copy = self
                .editor()
                .retrieve("acp", context.previous_copy_id)?
                .ok_or(format!("Cannot find copy {}", context.previous_copy_id))?;

            json_int(&copy["circ_lib"])?
        };

        let unful = json::object! {
            "hold": self.hold_id,
            "circ_lib": circ_lib,
            "current_copy": context.previous_copy_id
        };

        let unful = self.editor().idl().create_from("aufh", unful)?;
        self.editor().create(unful)?;

        Ok(())
    }

    /// Force and recall holds bypass validity tests.  Returns the first
    /// (and presumably only) copy ID in our list of valid copies when a
    /// F or R hold is encountered.
    fn attempt_force_recall_target(&self, context: &mut HoldTargetContext) -> Option<i64> {
        if let Some(ht) = context.hold["hold_type"].as_str() {
            if ht == "R" || ht == "F" {
                return context.copies.get(0).map(|c| c.id);
            }
        }

        None
    }

    fn attempt_to_find_copy(&mut self, context: &mut HoldTargetContext) -> EgResult<Option<i64>> {
        let max_loops = self.settings.get_value_at_org(
            "circ.holds.max_org_unit_target_loops",
            json_int(&context.hold["pickup_lib"])?,
        )?;

        if let Ok(max) = json_int(&max_loops) {
            return self.target_by_org_loops(context, max);
        }

        // When not using target loops, targeting is based solely on
        // proximity and org unit target weight.
        self.compile_weighted_proximity_map(context);

        self.find_nearest_copy(context)
    }


    fn find_nearest_copy(&mut self, context: &mut HoldTargetContext) -> EgResult<Option<i64>> {
        todo!()
    }

    /// Find libs whose unfulfilled target count is less than the maximum
    /// configured loop count.  Target copies in order of their circ_lib's
    /// target count (starting at 0) and moving up.  Copies within each
    /// loop count group are weighted based on configured hold weight.  If
    /// no copies in a given group are targetable, move up to the next
    /// unfulfilled target level.  Keep doing this until all potential
    /// copies have been tried or max targets loops is exceeded.
    /// Returns a targetable copy if one is found, undef otherwise.
    fn target_by_org_loops(
        &mut self,
        context: &mut HoldTargetContext,
        max_loops: i64,
    ) -> EgResult<Option<i64>> {
        let query = json::object! {
            "select": {"aufhl": ["circ_lib", "count"]},
            "from": "aufhl",
            "where": {"hold": self.hold_id},
            "order_by": [{"class": "aufhl", "field": "count"}]
        };

        let targeted_libs = self.editor().json_query(query)?;

        // Highest per-lib target attempts
        let mut max_tried = 0;
        for lib in targeted_libs.iter() {
            let count = json_int(&lib["count"])?;
            if count > max_tried {
                max_tried = count;
            }
        }

        log::info!("{self} max lib attempts is {max_tried}");
        log::info!(
            "{self} {} libs have been targeted at least once",
            targeted_libs.len()
        );

        // loop_iter represents per-lib target attemtps already made.
        // When loop_iter equals max loops, all libs with targetable copies
        // have been targeted the maximum number of times.  loop_iter starts
        // at 0 to pick up libs that have never been targeted.
        let mut loop_iter = 0;

        while loop_iter < max_loops {
            loop_iter += 1;

            // Ran out of copies to try before exceeding max target loops.
            // Nothing else to do here.
            if context.copies.len() == 0 {
                return Ok(None);
            }

            let (iter_copies, remaining_copies) =
                self.get_copies_at_loop_iter(context, &targeted_libs, loop_iter - 1);

            if iter_copies.len() == 0 {
                // None at this level.  Bump up a level.
                context.copies = remaining_copies;
                continue;
            }

            context.copies = iter_copies;

            // Update the proximity map to only include the copies
            // from this loop-depth iteration.
            self.compile_weighted_proximity_map(context);

            if let Some(copy) = self.find_nearest_copy(context)? {
                // OK for context.copies to be partially cleared at this
                // point, because this copy we have found is known
                // to be permitted.  No more copy checks needed.
                return Ok(Some(copy));
            }

           // No targetable copy at the current target leve.
           // Update our current copy set to the not-yet-tested copies.
           context.copies = remaining_copies;
        }

        if max_tried >= max_loops {
            // At least one lib has been targeted max-loops times and zero
            // other copies are targetable.  All options have been exhausted.
            self.handle_exceeds_target_loops(context)?;
        }

        Ok(None)
    }

    fn handle_exceeds_target_loops(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        todo!()
    }

    /// Returns a map of proximity values to arrays of copy hashes.
    /// The copy hash arrays are weighted consistent with the org unit hold
    /// target weight, meaning that a given copy may appear more than once
    /// in its proximity list.
    fn compile_weighted_proximity_map(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        // Collect copy proximity info (generated via DB trigger)
        // from our newly create copy maps.

        let query = json::object! {
            "select": {"ahcm": ["target_copy", "proximity"]},
            "from": "ahcm",
            "where": {"hold": self.hold_id}
        };

        let copy_maps = self.editor().json_query(query)?;

        let mut flat_map: HashMap<i64, i64> = HashMap::new();

        for map in copy_maps.iter() {
            let copy_id = json_int(&map["target_copy"])?;
            let proximity = json_int(&map["proximity"])?;
            flat_map.insert(copy_id, proximity);
        }

        // The weight of a copy at a give proximity is a function
        // of how many times the copy ID appears in the list
        // at that proximity.
        let mut weighted: HashMap<i64, Vec<i64>> = HashMap::new();
        for copy in context.copies.iter_mut() {

            let prox = match flat_map.get(&copy.id) {
                Some(p) => *p, // &i64
                None => continue, // should not happen
            };

            copy.proximity = prox;

            if weighted.get(&prox).is_none() {
                weighted.insert(prox, Vec::new());
            }

            let weight = self.settings.get_value_at_org(
                "circ.holds.org_unit_target_weight", copy.circ_lib)?;

            let weight = if weight.is_null() {
                1
            } else {
                json_int(&weight)?
            };

            if let Some(list) = weighted.get_mut(&prox) {
                for _ in 0 .. weight {
                    list.push(copy.id);
                }
            }
        }

        // We need to grab the proximity for copies targeted by other
        // holds that belong to this pickup lib for hard-stalling tests
        // later. We'll just grab them all in case it's useful later.
        for copy in context.already_targeted_copies.iter_mut() {
            if let Some(prox) = flat_map.get(&copy.id) {
                copy.proximity = *prox;
            }
        }

        // We also need the proximity for the previous target.
        if let Some(copy) = context.valid_previous_copy.as_mut() {
            if let Some(prox) = flat_map.get(&copy.id) {
                copy.proximity = *prox;
            }
        }

        context.weighted_prox_map = weighted;

        Ok(())
    }

    /// Returns 2 vecs.  The first is a list of copies whose circ lib's
    /// unfulfilled target count matches the provided loop_iter value.  The
    /// second list is all other copies, returned for convenience.
    ///
    /// NOTE this drains context.copies into the two arrays returned!
    fn get_copies_at_loop_iter(
        &self,
        context: &mut HoldTargetContext,
        targeted_libs: &Vec<JsonValue>,
        loop_iter: i64,
    ) -> (Vec<PotentialCopy>, Vec<PotentialCopy>) {
        let mut iter_copies = Vec::new();
        let mut remaining_copies = Vec::new();

        while let Some(copy) = context.copies.pop() {
            let mut match_found = false;

            if loop_iter == 0 {
                // Start with copies at circ libs that have never been targeted.
                match_found = !targeted_libs
                    .iter()
                    .any(|l| json_int(&l["circ_lib"]).unwrap() == copy.circ_lib);
            } else {
                // Find copies at branches whose target count
                // matches the current (non-zero) loop depth.
                match_found = targeted_libs.iter().any(|l| {
                    return json_int(&l["circ_lib"]).unwrap() == copy.circ_lib
                        && json_int(&l["count"]).unwrap() == loop_iter;
                });
            }

            if match_found {
                iter_copies.push(copy);
            } else {
                remaining_copies.push(copy);
            }
        }

        log::info!(
            "{self} {} potential copies at max-loops iter level {loop_iter}. \
            {} remain to be tested at a higher loop iteration level",
            iter_copies.len(),
            remaining_copies.len()
        );

        (iter_copies, remaining_copies)
    }

    /// Target one hold by ID.
    pub fn target_hold(&mut self, hold_id: i64, find_copy: i64) -> EgResult<HoldTargetContext> {
        if !self.transaction_manged_externally {
            self.editor().xact_begin()?;
        }

        let result = self.target_hold_internal(hold_id, find_copy);

        if result.is_ok() {
            let ctx = result.unwrap();

            // This call can result in a secondary commit in some cases,
            // but it will be a no-op.
            self.commit()?;
            return Ok(ctx);
        }

        // Not every error condition results in a rollback.
        // Force it regardless of whether our transaction is
        // managed externally.
        self.editor().rollback()?;

        let err = result.unwrap_err();

        // If the caller only provides an error message and the
        // editor has a last-event, return the editor's last event
        // with the message added.
        if let EgError::Debug(ref msg) = err {
            log::error!("{self} exited early with error message {msg}");

            if let Some(mut evt) = self.editor().take_last_event() {
                evt.set_debug(msg);
                return Err(EgError::Event(evt));
            }
        }

        Err(err)
    }

    /// Caller may use this method directly when targeting only one hold.
    ///
    /// self.init() is still required.
    fn target_hold_internal(
        &mut self,
        hold_id: i64,
        find_copy: i64,
    ) -> EgResult<HoldTargetContext> {
        self.hold_id = hold_id;

        let hold = self
            .editor()
            .retrieve("ahr", hold_id)?
            .ok_or("No such hold")?;

        let mut context = HoldTargetContext::new(hold_id, hold);
        let ctx = &mut context; // local shorthand
        ctx.find_copy = find_copy;

        self.check_hold_eligible(ctx)?;

        if self.hold_is_expired(ctx)? {
            // Exit early if the hold is expired.
            return Ok(context);
        }

        self.get_hold_copies(ctx)?;
        self.update_copy_maps(ctx)?;
        self.handle_hopeless_date(ctx)?;

        if self.hold_has_no_copies(ctx, false, false)? {
            // Exit early if we have no copies.
            return Ok(context);
        }

        // Trim the set of working copies down to those that are
        // currently targetable.
        self.filter_copies_by_status_and_targeted(ctx);
        self.filter_closed_date_copies(ctx)?;

        if self.inspect_previous_target(ctx)? {
            // Exits early if we are retargeting the previous copy.
            return Ok(context);
        }

        self.log_unfulfilled_hold(ctx)?;

        if self.hold_has_no_copies(ctx, false, true)? {
            // Exit early if we have no copies.
            return Ok(context);
        }

        // At this point, the working list of copies has been trimmed to
        // those that are currently targetable at a superficial level.
        // (They are holdable and available).  Now the code steps through
        // these copies in order of priority and pickup lib proximity to
        // find a copy that is confirmed targetable by policy.

        let mut copy = self.attempt_force_recall_target(ctx);
        if copy.is_none() {
            copy = self.attempt_to_find_copy(ctx)?;
        }

        /*
        my $copy = $self->attempt_force_recall_target ||
               $self->attempt_to_find_copy        ||
               $self->attempt_prev_copy_retarget;
        */

        // TODO

        Ok(context)
    }
}