use crate as eg;
use eg::common::holds;
use eg::common::settings::Settings;
use eg::common::trigger;
use eg::constants as C;
use eg::date;
use eg::{Editor, EgError, EgResult, EgValue};
use rand;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::fmt;

const PRECACHE_ORG_SETTINGS: &[&str] = &[
    "circ.pickup_hold_stalling.hard",
    "circ.holds.max_org_unit_target_loops",
    "circ.holds.org_unit_target_weight",
    "circ.holds.recall_threshold",
];

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
    /// Did we successfully target our hold?
    success: bool,

    /// Hold ID
    hold_id: i64,

    hold: EgValue,

    pickup_lib: i64,

    /// Targeted copy ID.
    ///
    /// If we have a target, we succeeded.
    target: i64,

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
    otherwise_targeted_copies: Vec<PotentialCopy>,

    /// Maps proximities to the weighted list of copy IDs.
    weighted_prox_map: HashMap<i64, Vec<i64>>,
}

impl HoldTargetContext {
    fn new(hold_id: i64, hold: EgValue) -> HoldTargetContext {
        // Required, numeric value.
        let pickup_lib = hold["pickup_lib"].int().expect("Hold Pickup Lib Required");

        HoldTargetContext {
            success: false,
            hold_id,
            hold,
            pickup_lib,
            copies: Vec::new(),
            recall_copies: Vec::new(),
            otherwise_targeted_copies: Vec::new(),
            weighted_prox_map: HashMap::new(),
            eligible_copy_count: 0,
            target: 0,
            find_copy: 0,
            valid_previous_copy: None,
            previous_copy_id: 0,
            found_copy: false,
        }
    }

    pub fn hold_id(&self) -> i64 {
        self.hold_id
    }
    pub fn success(&self) -> bool {
        self.success
    }
    pub fn found_copy(&self) -> bool {
        self.found_copy
    }
    /// Returns a summary of this context as a JSON object.
    pub fn to_json(&self) -> EgValue {
        eg::hash! {
            "hold": self.hold_id,
            "success": self.success,
            "target": self.target,
            "old_target": self.previous_copy_id,
            "found_copy": self.found_copy,
            "eligible_copies": self.eligible_copy_count,
        }
    }
}

/// Targets a batch of holds.
pub struct HoldTargeter<'a> {
    editor: &'a mut Editor,

    settings: Settings,

    /// Hold in process -- mainly for logging.
    hold_id: i64,

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

    /// If true the targeter will NOT make any begin or commit
    /// calls to its editor, assuming the caller will manage that.
    ///
    /// This is useful for cases where targeting a hold is part
    /// of a larger transaction of changes.
    ///
    /// This should only be used when targeting a single hold
    /// since each hold requires its own transaction to avoid deadlocks.
    /// Alternatively, the caller should be prepared to begin/commit
    /// before/after each call to target_hold().
    transaction_manged_externally: bool,

    thread_rng: rand::rngs::ThreadRng,
}

impl fmt::Display for HoldTargeter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "targeter: [hold={}]", self.hold_id)
    }
}

impl<'a> HoldTargeter<'a> {
    pub fn new(editor: &'a mut Editor) -> HoldTargeter {
        let settings = Settings::new(&editor);

        HoldTargeter {
            editor,
            settings,
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
            thread_rng: rand::thread_rng(),
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
    pub fn set_transaction_manged_externally(&mut self, val: bool) {
        self.transaction_manged_externally = val;
    }

    pub fn editor(&mut self) -> &mut Editor {
        self.editor
    }

    pub fn set_parallel_count(&mut self, count: u8) {
        self.parallel_count = count;
    }

    pub fn set_parallel_slot(&mut self, slot: u8) {
        self.parallel_slot = slot;
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
            let query = eg::hash! {
                "name": "circ.holds.retarget_interval",
                "enabled": "t"
            };

            if let Some(intvl) = self.editor().search("cgf", query)?.get(0) {
                retarget_intvl_bind = intvl["value"].to_string();
                retarget_intvl_bind.as_ref().unwrap()
            } else {
                // If all else fails, use a one day retarget interval.
                "24 h"
            }
        };

        log::info!("{self} using retarget interval: {retarget_intvl}");

        let retarget_date = date::subtract_interval(date::now(), retarget_intvl)?;
        let rt = date::to_iso(&retarget_date);

        log::info!("{self} using retarget time: {rt}");

        self.retarget_time = Some(rt);

        if let Some(sri) = self.soft_retarget_interval.as_ref() {
            let rt_date = date::subtract_interval(date::now(), sri)?;
            let srt = date::to_iso(&rt_date);

            log::info!("{self} using soft retarget time: {srt}");

            self.soft_retarget_time = Some(srt);
        }

        // Holds targeted in the current targeter instance
        // won't be retargeted until the next check date.  If a
        // next_check_interval is provided it overrides the
        // retarget_interval.
        let next_check_intvl = self
            .next_check_interval
            .as_ref()
            .map(|i| i.as_str())
            .unwrap_or(retarget_intvl);

        let next_check_date = date::add_interval(date::now(), next_check_intvl)?;
        let next_check_time = date::to_iso(&next_check_date);

        log::info!("{self} next check time {next_check_time}");

        // An org unit is considered closed for retargeting purposes
        // if it's closed both now and at the next re-target date.
        let query = eg::hash! {
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
            self.closed_orgs.push(co["org_unit"].int()?);
        }

        for stat in self
            .editor()
            .search("ccs", eg::hash! {"hopeless_prone":"t"})?
        {
            self.hopeless_prone_statuses.push(stat["id"].int()?);
        }

        Ok(())
    }

    /// Find holds that need to be processed.
    ///
    /// When targeting a known hold ID, this step can be skipped.
    pub fn find_holds_to_target(&mut self) -> EgResult<Vec<i64>> {
        let mut query = eg::hash! {
            "select": {"ahr": ["id"]},
            "from": "ahr",
            "where": {
                "capture_time": eg::NULL,
                "fulfillment_time": eg::NULL,
                "cancel_time": eg::NULL,
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

        query["where"]["-or"] = eg::array! [
            {"prev_check_time": eg::NULL},
            {"prev_check_time": {"<=": start_time}},
        ];

        let parallel = self.parallel_count;

        // The Perl code checks parallel > 0, but a parallel value of 1
        // is also, by definition, non-parallel, so we can skip the
        // theatrics below for values of <= 1.
        if parallel > 1 {
            // In parallel mode, we need to also grab the metarecord for each hold.

            query["from"] = eg::hash! {
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

            query["where"]["+mmrsm"] = eg::hash! {
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
            query["order_by"] = eg::array! [{
                "class": "ahr",
                "field": "request_time",
                "direction": "DESC"
            }];
        }

        // NOTE The perl code runs this query in substream mode.
        // At time of writing, the Rust editor has no substream mode.
        // It seems far less critical for Redis, but can be added if needed.
        let holds = self.editor().json_query(query)?;

        log::info!("{self} found {} holds to target", holds.len());

        Ok(holds.iter().map(|h| h["id"].int_required()).collect())
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
    fn update_hold(
        &mut self,
        context: &mut HoldTargetContext,
        mut values: EgValue,
    ) -> EgResult<()> {
        for (k, v) in values.entries_mut() {
            if k == "id" {
                continue;
            }
            context.hold[k] = v.take();
        }

        self.editor().update(context.hold.clone())?;

        // this hold id must exist.
        context.hold = self
            .editor()
            .retrieve("ahr", context.hold_id)?
            .ok_or("Cannot find hold")?;

        Ok(())
    }

    /// Return false if the hold is not eligible for targeting (frozen,
    /// canceled, etc.)
    fn hold_is_targetable(&mut self, context: &HoldTargetContext) -> bool {
        let hold = &context.hold;

        if hold["capture_time"].is_null()
            && hold["cancel_time"].is_null()
            && hold["fulfillment_time"].is_null()
            && !hold["frozen"].boolish()
        {
            return true;
        }

        log::info!("{self} hold is not targetable");

        false
    }

    /// Cancel expired holds and kick off the A/T no-target event.
    ///
    /// Returns true if the hold was marked as expired, indicating no
    /// further targeting is needed.
    fn hold_is_expired(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        if let Some(etime) = context.hold["expire_time"].as_str() {
            let ex_time = date::parse_datetime(&etime)?;

            if ex_time > date::now() {
                // Hold has not yet expired.
                return Ok(false);
            }
        } else {
            // Hold has no expire time.
            return Ok(false);
        }

        // -- Hold is expired --
        let values = eg::hash! {
            "cancel_time": "now",
            "cancel_cause": 1, // un-targeted expiration
        };

        self.update_hold(context, values)?;

        // Create events that will be fired/processed later.
        trigger::create_events_for_object(
            self.editor(),
            "hold_request.cancel.expire_no_target",
            &context.hold,
            context.pickup_lib,
            None,
            None,
            false,
        )?;

        Ok(true)
    }

    /// Find potential copies for mapping/targeting and add them to
    /// the copies list on our context.
    fn get_hold_copies(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let hold = &context.hold;

        let hold_target = hold["target"].int()?;
        let hold_type = hold["hold_type"].as_str().unwrap(); // required.
        let org_unit = hold["selection_ou"].int()?;
        let org_depth = hold["selection_depth"].as_int().unwrap_or(0); // not required

        let mut query = eg::hash! {
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
                            "fulfillment_time": eg::NULL,
                            "cancel_time": eg::NULL,
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

            query["from"]["acp"]["acpl"] = eg::hash! {
                "field": "id",
                "filter": {"holdable": "t", "deleted": "f"},
                "fkey": "location",
            };

            query["from"]["acp"]["ccs"] = eg::hash! {
                "field": "id",
                "filter": {"holdable": "t"},
                "fkey": "status",
            };

            query["where"]["+acp"]["holdable"] = EgValue::from("t");

            if hold["mint_condition"].boolish() {
                query["where"]["+acp"]["mint_condition"] = EgValue::from("t");
            }
        }

        if hold_type != "C" && hold_type != "I" && hold_type != "P" {
            // For volume and higher level holds, avoid targeting copies that
            // act as instances of monograph parts.

            query["from"]["acp"]["acpm"] = eg::hash! {
                "type": "left",
                "field": "target_copy",
                "fkey": "id"
            };

            query["where"]["+acpm"]["id"] = eg::NULL;
        }

        // Add the target filters
        if hold_type == "C" || hold_type == "R" || hold_type == "F" {
            query["where"]["+acp"]["id"] = EgValue::from(hold_target);
        } else if hold_type == "V" {
            query["where"]["+acp"]["call_number"] = EgValue::from(hold_target);
        } else if hold_type == "P" {
            query["from"]["acp"]["acpm"] = eg::hash! {
                "field" : "target_copy",
                "fkey" : "id",
                "filter": {"part": hold_target},
            };
        } else if hold_type == "I" {
            query["from"]["acp"]["sitem"] = eg::hash! {
                "field" : "unit",
                "fkey" : "id",
                "filter": {"issuance": hold_target},
            };
        } else if hold_type == "T" {
            query["from"]["acp"]["acn"] = eg::hash! {
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

            query["from"]["acp"]["acn"] = eg::hash! {
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

                let query_ints = self.editor().json_query(eg::hash! {
                    "from": ["metabib.compile_composite_attr", formats]
                })?;

                if let Some(query_int) = query_ints.get(0) {
                    // Only pull potential copies from records that satisfy
                    // the holdable formats query.
                    if let Some(qint) = query_int["metabib.compile_composite_attr"].as_str() {
                        query["from"]["acp"]["acn"]["join"]["bre"]["join"]["mravl"] = eg::hash! {
                            "field": "source",
                            "fkey": "id",
                            "filter": {"vlist": {"@@": qint}}
                        }
                    }
                }
            }
        }

        let mut found_copy = false;
        let mut circ_libs: HashSet<i64> = HashSet::new();
        context.copies = self
            .editor()
            .json_query(query)?
            .iter()
            .map(|c| {
                // While we're looping, see if we found the copy the
                // caller was interested in.
                let id = c["id"].int_required();
                if id == context.find_copy {
                    found_copy = true;
                }

                let copy = PotentialCopy {
                    id,
                    status: c["status"].int_required(),
                    circ_lib: c["circ_lib"].int_required(),
                    proximity: -1,
                    already_targeted: !c["current_copy"].is_null(),
                };

                circ_libs.insert(copy.circ_lib);

                copy
            })
            .collect();

        context.eligible_copy_count = context.copies.len();
        context.found_copy = found_copy;

        log::info!("{self} {} potential copies", context.eligible_copy_count);

        // Pre-cache some org unit settings
        for lib in circ_libs.iter() {
            log::info!("{self} pre-caching org settings for {lib}");
            self.settings
                .fetch_values_for_org(*lib, PRECACHE_ORG_SETTINGS)?;
        }

        Ok(())
    }

    /// Tell the DB to update the list of potential copies for our hold
    /// based on the copies we just found.
    fn update_copy_maps(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        log::info!("{self} creating {} hold copy maps", context.copies.len());

        let ints = context
            .copies
            .iter()
            .map(|c| format!("{}", c.id))
            .collect::<Vec<String>>()
            .join(",");

        // "{1,2,3}"
        let ints = format!("{{{ints}}}");

        let query = eg::hash! {
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

        if context.copies.len() == 0 && !marked_hopeless {
            log::info!("{self} Marking hold as hopeless");
            return self.update_hold(context, eg::hash! {"hopeless_date": "now"});
        }

        // Hope left in any of the statuses?
        let we_have_hope = context
            .copies
            .iter()
            .any(|c| !self.hopeless_prone_statuses.contains(&c.status));

        if marked_hopeless {
            if we_have_hope {
                log::info!("{self} Removing hopeless date");
                return self.update_hold(context, eg::hash! {"hopeless_date": eg::NULL});
            }
        } else if !we_have_hope {
            log::info!("{self} Marking hold as hopeless");
            return self.update_hold(context, eg::hash! {"hopeless_date": "now"});
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

        let values = eg::hash! {
            "current_copy": eg::NULL,
            "prev_check_time": "now"
        };

        self.update_hold(context, values)?;

        log::info!("{self} hold officially has no targetable copies");

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

        let recall_threshold = self
            .settings
            .get_value_at_org("circ.holds.recall_threshold", context.pickup_lib)?;

        let recall_threshold = match recall_threshold.to_string() {
            Some(t) => t,
            None => return Ok(()),
        };

        let return_interval = self
            .settings
            .get_value_at_org("circ.holds.recall_return_interval", context.pickup_lib)?;

        let return_interval = match return_interval.to_string() {
            Some(t) => t,
            None => return Ok(()),
        };

        let copy_ids = context
            .recall_copies
            .iter()
            .map(|c| c.id)
            .collect::<Vec<i64>>();

        // See if we have a circulation linked to our recall copies
        // that we can recall.
        let query = eg::hash! {
            "target_copy": copy_ids,
            "checkin_time": eg::NULL,
            "duration": {">": recall_threshold.as_str()}
        };

        let ops = eg::hash! {
            "order_by": [{"class": "circ", "field": "due_date"}],
            "limit": 1
        };

        let mut circs = self.editor().search_with_ops("circ", query, ops)?;

        let mut circ = match circs.pop() {
            Some(c) => c,
            // Tried our best to recall a circ but could not find one.
            None => {
                log::info!("{self} no circulations to recall");
                return Ok(());
            }
        };

        log::info!("{self} recalling circ {}", circ["id"]);

        let old_due_date = date::parse_datetime(circ["due_date"].as_str().unwrap())?;
        let xact_start_date = date::parse_datetime(circ["xact_start"].as_str().unwrap())?;

        let thresh_date = date::add_interval(xact_start_date, &recall_threshold)?;
        let mut return_date = date::add_interval(date::now(), &return_interval)?;

        // Give the user a new due date of either a full recall threshold,
        // or the return interval, whichever is further in the future.
        if thresh_date > return_date {
            return_date = thresh_date;
        }

        // ... but avoid exceeding the old due date.
        if return_date > old_due_date {
            return_date = old_due_date;
        }

        circ["due_date"] = date::to_iso(&return_date).into();
        circ["renewal_remaining"] = 0.into();

        let mut fine_rules = self
            .settings
            .get_value_at_org("circ.holds.recall_fine_rules", context.pickup_lib)?
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
            circ["circ_lib"].int()?,
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
                context.otherwise_targeted_copies.push(copy);
                continue;
            }

            if copy.status == C::COPY_STATUS_AVAILABLE || copy.status == C::COPY_STATUS_RESHELVING {
                targetable.push(copy);
            }
        }

        log::info!(
            "{self} potential copies checked out={}, otherwise targeted={}, available={}",
            context.recall_copies.len(),
            context.otherwise_targeted_copies.len(),
            targetable.len()
        );

        context.copies = targetable;
    }

    /// Removes copies for consideration when they live at a closed org unit
    /// and settings prevent targeting when closed.
    fn filter_closed_date_copies(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let mut targetable = Vec::new();

        while let Some(copy) = context.copies.pop() {
            if self.closed_orgs.contains(&copy.circ_lib) {
                let setting = if copy.circ_lib == context.pickup_lib {
                    "circ.holds.target_when_closed_if_at_pickup_lib"
                } else {
                    "circ.holds.target_when_closed"
                };

                let value = self.settings.get_value_at_org(setting, copy.circ_lib)?;

                if value.boolish() {
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
        let result = holds::test_copy_for_hold(
            self.editor(),
            context.hold["usr"].int()?,
            copy_id,
            context.pickup_lib,
            context.hold["request_lib"].int()?,
            context.hold["requestor"].int()?,
            true, // is_retarget
            None, // overrides
            true, // check_only
        )?;

        if result.success() {
            log::info!("{self} copy {copy_id} is permitted");
            return Ok(true);
        }

        // Copy is non-viable.  Remove it from our list.
        if let Some(pos) = context.copies.iter().position(|c| c.id == copy_id) {
            log::info!("{self} copy {copy_id} is not permitted");
            context.copies.remove(pos);
        }

        Ok(false)
    }

    /// Returns true if we have decided to retarget the existing copy.
    ///
    /// Otherwise, sets aside the previously targeted copy in case in
    /// may be of use later... and returns false.
    fn inspect_previous_target(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        let prev_copy = match context.hold["current_copy"].as_int() {
            Some(c) => c,
            None => return Ok(false), // value was null
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
            "{self} logging unsuccessful capture of previous copy: {}",
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

            copy["circ_lib"].int()?
        };

        let mut unful = eg::hash! {
            "hold": self.hold_id,
            "circ_lib": circ_lib,
            "current_copy": context.previous_copy_id
        };

        unful.bless("aufh")?;
        self.editor().create(unful)?;

        Ok(())
    }

    /// Set the 'target' value on the context to the first (and presumably
    /// only) copy in our list of valid copies if this is a Force or Recall
    /// hold, which bypass policy checks.
    fn attempt_force_recall_target(&self, context: &mut HoldTargetContext) {
        if let Some(ht) = context.hold["hold_type"].as_str() {
            if ht == "R" || ht == "F" {
                if let Some(c) = context.copies.get(0) {
                    context.target = c.id;
                    log::info!("{self} force/recall hold using copy {}", c.id);
                    return;
                }
            }
        }
    }

    /// Returns true if the hold was canceled while looking for a target
    /// (e.g. hits max target loops).
    /// Sets context.target if it can.
    fn attempt_to_find_copy(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        if context.target > 0 {
            return Ok(false);
        }

        let max_loops = self
            .settings
            .get_value_at_org("circ.holds.max_org_unit_target_loops", context.pickup_lib)?;

        if let Some(max) = max_loops.as_int() {
            if let Some(copy_id) = self.target_by_org_loops(context, max)? {
                context.target = copy_id;
            }
        } else {
            // When not using target loops, targeting is based solely on
            // proximity and org unit target weight.
            self.compile_weighted_proximity_map(context)?;

            if let Some(copy_id) = self.find_nearest_copy(context)? {
                context.target = copy_id;
            }
        }

        Ok(!context.hold["cancel_time"].is_null())
    }

    /// Returns the closest copy by proximity that is a confirmed valid
    /// targetable copy.
    fn find_nearest_copy(&mut self, context: &mut HoldTargetContext) -> EgResult<Option<i64>> {
        let inside_hard_stall = self.inside_hard_stall_interval(context)?;
        let mut have_local_copies = false;

        // If we're still hard stallin', see if we have any local
        // copies in use.
        if inside_hard_stall {
            have_local_copies = context
                .otherwise_targeted_copies
                .iter()
                .any(|c| c.proximity <= 0);
        }

        // Pick a copy at random from each tier of the proximity map,
        // starting at the lowest proximity and working up, until a
        // copy is found that is suitable for targeting.
        let mut sorted_proximities: Vec<i64> =
            context.weighted_prox_map.keys().map(|i| *i).collect();

        sorted_proximities.sort();

        let mut already_tested_copies: HashSet<i64> = HashSet::new();

        for prox in sorted_proximities {
            let copy_ids = match context.weighted_prox_map.get_mut(&prox) {
                Some(list) => list,
                None => continue, // Shouldn't happen
            };

            if copy_ids.len() == 0 {
                continue;
            }

            if prox <= 0 {
                have_local_copies = true;
            }

            if have_local_copies && inside_hard_stall && prox > 0 {
                // We have attempted to target all local (prox <= 0)
                // copies and come up with zilch.
                //
                // We're also still in the hard-stall interval and we
                // have local copies that could be targeted later.
                // There's nothing else we can do until the stall time
                // expires or a local copy becomes targetable on a
                // future targeting run.
                break;
            }

            // Clone the list so we can modify at will and avoid a
            // parallell borrow on the context.
            let mut copy_ids = copy_ids.clone();

            // Shuffle the weighted list for random selection.
            copy_ids.shuffle(&mut self.thread_rng);

            for copy_id in copy_ids.iter() {
                if already_tested_copies.contains(copy_id) {
                    // No point in testing the same copy twice.
                    continue;
                }

                if self.copy_is_permitted(context, *copy_id)? {
                    return Ok(Some(*copy_id));
                }

                already_tested_copies.insert(*copy_id);
            }
        }

        if have_local_copies && inside_hard_stall {
            // If we have local copies and we're still hard stallin',
            // we're no longer interested in non-local copies.  Clear
            // the valid_previous_copy if it's not local.
            if let Some(copy) = context.valid_previous_copy.as_ref() {
                if copy.proximity > 0 {
                    context.valid_previous_copy = None;
                }
            }
        }

        Ok(None)
    }

    fn inside_hard_stall_interval(&mut self, context: &mut HoldTargetContext) -> EgResult<bool> {
        let interval = self
            .settings
            .get_value_at_org("circ.pickup_hold_stalling.hard", context.pickup_lib)?;

        let interval = match interval.as_str() {
            Some(s) => s,
            None => return Ok(false),
        };

        // Required, string field
        let req_time = context.hold["request_time"].as_str().unwrap();
        let req_time = date::parse_datetime(&req_time)?;

        let hard_stall_time = date::add_interval(req_time, interval)?;

        log::info!("{self} hard stall deadline is/was {hard_stall_time}");

        let inside = hard_stall_time > date::now();

        log::info!("{self} still within hard stall interval? {inside}");

        Ok(inside)
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
        let query = eg::hash! {
            "select": {"aufhl": ["circ_lib", "count"]},
            "from": "aufhl",
            "where": {"hold": self.hold_id},
            "order_by": [{"class": "aufhl", "field": "count"}]
        };

        let targeted_libs = self.editor().json_query(query)?;

        // Highest per-lib target attempts
        let mut max_tried = 0;
        for lib in targeted_libs.iter() {
            let count = lib["count"].int()?;
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
            self.compile_weighted_proximity_map(context)?;

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

    /// Cancel the hold and fire the no-target A/T event creator.
    fn handle_exceeds_target_loops(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        let values = eg::hash! {
            "cancel_time": "now",
            "cancel_cause": 1, // un-targeted expiration
        };

        self.update_hold(context, values)?;

        trigger::create_events_for_object(
            self.editor(),
            "hold_request.cancel.expire_no_target",
            &context.hold,
            context.pickup_lib,
            None,
            None,
            false,
        )?;

        Ok(())
    }

    /// Returns a map of proximity values to arrays of copy hashes.
    /// The copy hash arrays are weighted consistent with the org unit hold
    /// target weight, meaning that a given copy may appear more than once
    /// in its proximity list.
    fn compile_weighted_proximity_map(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        // Collect copy proximity info (generated via DB trigger)
        // from our newly create copy maps.

        let query = eg::hash! {
            "select": {"ahcm": ["target_copy", "proximity"]},
            "from": "ahcm",
            "where": {"hold": self.hold_id}
        };

        let copy_maps = self.editor().json_query(query)?;

        let mut flat_map: HashMap<i64, i64> = HashMap::new();

        for map in copy_maps.iter() {
            let copy_id = map["target_copy"].int()?;
            let proximity = map["proximity"].int()?;
            flat_map.insert(copy_id, proximity);
        }

        // The weight of a copy at a give proximity is a function
        // of how many times the copy ID appears in the list
        // at that proximity.
        let mut weighted: HashMap<i64, Vec<i64>> = HashMap::new();
        for copy in context.copies.iter_mut() {
            let prox = match flat_map.get(&copy.id) {
                Some(p) => *p,    // &i64
                None => continue, // should not happen
            };

            copy.proximity = prox;

            if weighted.get(&prox).is_none() {
                weighted.insert(prox, Vec::new());
            }

            let weight = self
                .settings
                .get_value_at_org("circ.holds.org_unit_target_weight", copy.circ_lib)?;

            let weight = if weight.is_null() { 1 } else { weight.int()? };

            if let Some(list) = weighted.get_mut(&prox) {
                for _ in 0..weight {
                    list.push(copy.id);
                }
            }
        }

        // We need to grab the proximity for copies targeted by other
        // holds that belong to this pickup lib for hard-stalling tests
        // later. We'll just grab them all in case it's useful later.
        for copy in context.otherwise_targeted_copies.iter_mut() {
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
        targeted_libs: &Vec<EgValue>,
        loop_iter: i64,
    ) -> (Vec<PotentialCopy>, Vec<PotentialCopy>) {
        let mut iter_copies = Vec::new();
        let mut remaining_copies = Vec::new();

        while let Some(copy) = context.copies.pop() {
            let match_found;

            if loop_iter == 0 {
                // Start with copies at circ libs that have never been targeted.
                match_found = !targeted_libs
                    .iter()
                    .any(|l| l["circ_lib"].int_required() == copy.circ_lib);
            } else {
                // Find copies at branches whose target count
                // matches the current (non-zero) loop depth.
                match_found = targeted_libs.iter().any(|l| {
                    return l["circ_lib"].int_required() == copy.circ_lib
                        && l["count"].int_required() == loop_iter;
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

    /// All we might have left is the copy this hold previously targeted.
    /// Grab it if we can.
    fn attempt_prev_copy_retarget(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        if context.target > 0 {
            return Ok(());
        }

        if let Some(copy_id) = context.valid_previous_copy.as_ref().map(|c| c.id) {
            log::info!(
                "Attempting to retarget previously targeted copy {}",
                copy_id
            );

            if self.copy_is_permitted(context, copy_id)? {
                context.target = copy_id;
            }
        }

        Ok(())
    }

    fn apply_copy_target(&mut self, context: &mut HoldTargetContext) -> EgResult<()> {
        log::info!("{self} successfully targeted copy: {}", context.target);

        let values = eg::hash! {
            "current_copy": context.target,
            "prev_check_time": "now"
        };

        self.update_hold(context, values)
    }

    /// Target one hold by ID.
    /// Caller should use this method directly when targeting only one hold.
    /// self.init() is still required.
    pub fn target_hold(
        &mut self,
        hold_id: i64,
        find_copy: Option<i64>,
    ) -> EgResult<HoldTargetContext> {
        if !self.transaction_manged_externally {
            self.editor().xact_begin()?;
        }

        let result = self.target_hold_internal(hold_id, find_copy.unwrap_or(0));

        if result.is_ok() {
            let ctx = result.unwrap();
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

    /// Runs through the actual targeting logic w/o concern for
    /// transaction management.
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

        if !self.hold_is_targetable(ctx) {
            return Ok(context);
        }

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
        // those that are targetable at a superficial level.  (They are
        // holdable and available).  Now the code steps through these
        // copies in order of priority/proximity to find a copy that is
        // confirmed targetable by policy.

        self.attempt_force_recall_target(ctx);

        if self.attempt_to_find_copy(ctx)? {
            // Hold was canceled while seeking a target.
            return Ok(context);
        }

        self.attempt_prev_copy_retarget(ctx)?;

        if ctx.target > 0 {
            // At long great last we found a copy to target.
            self.apply_copy_target(ctx)?;
            ctx.success = true;
        } else {
            // Targeting failed.  Make one last attempt to process a
            // recall and mark the hold as un-targeted.
            self.hold_has_no_copies(ctx, true, true)?;
        }

        Ok(context)
    }
}
