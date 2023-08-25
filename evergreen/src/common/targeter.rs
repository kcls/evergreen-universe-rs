use crate::editor::Editor;
use crate::util::{json_int, json_bool, json_string};
use crate::common::org;
use crate::result::EgResult;
use crate::date;
use crate::common::settings::Settings;
use json::JsonValue;
use chrono::Duration;
use std::fmt;

const JSON_NULL: JsonValue = JsonValue::Null;

/// Targets a batch of holds.
pub struct HoldTargeter {
    editor: Option<Editor>,

    /// Only target this exact hold.
    one_hold: Option<i64>,

    retarget_time: Option<String>,
    retarget_interval: Option<String>,
    soft_retarget_interval: Option<String>,
    soft_retarget_time: Option<String>,
    next_check_interval: Option<String>,

    closed_orgs: Vec<i64>,

    hopeless_prone_statuses: Vec<i64>,

    /// Number of parallel slots; 0 means we are not running in parallel.
    parallel_count: u8,

    /// Which parallel slot do we occupy; 0 is none.
    parallel_slot: u8,

    /// Target holds newest first by request date.
    newest_first: bool,
}

impl fmt::Display for HoldTargeter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "targeter:")
    }
}

impl HoldTargeter {

    pub fn new() -> HoldTargeter {
        HoldTargeter {
            editor: None,
            one_hold: None,
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

        let mut retarget_intvl_binding = None;
        let retarget_intvl = if let Some(intvl) = self.retarget_interval.as_ref() {
            intvl
        } else {

            let query = json::object! {
                "name": "circ.holds.retarget_interval",
                "enabled": "t"
            };

            if let Some(intvl) = self.editor().search("cgf", query)?.get(0) {
                retarget_intvl_binding = Some(json_string(&intvl["value"])?);
                retarget_intvl_binding.as_ref().unwrap()
            } else {
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

    pub fn find_holds_to_target(&mut self) -> EgResult<Vec<i64>> {
        if let Some(id) = self.one_hold {
            return Ok(vec![id]);
        }

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
        Ok(holds.iter().map(|h| json_int(&h["id"]).unwrap()).collect())
    }
}


/// Targets one hold.
pub struct HoldTargeterSingle {
}

