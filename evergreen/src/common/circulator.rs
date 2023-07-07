use crate::common::org;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;
use json::JsonValue;
use std::collections::HashMap;
use std::fmt;

/// These copy fields are assumed to be fleshed throughout.
const COPY_FLESH: &[&str] = &["status", "call_number", "parts", "floating", "location"];

/// Context and shared methods for circulation actions.
///
/// Innards are 'pub' since the impl's are spread across multiple files.
pub struct Circulator {
    pub editor: Editor,
    pub settings: Settings,
    pub exit_early: bool,
    pub circ_lib: i64,
    pub events: Vec<EgEvent>,
    pub copy: Option<JsonValue>,
    pub copy_id: Option<i64>,
    pub copy_state: Option<String>,
    pub open_circ: Option<JsonValue>,
    pub patron: Option<JsonValue>,
    pub transit: Option<JsonValue>,
    pub is_noncat: bool,
    pub changes_applied: bool,

    /// This one comes up in a variety of places.
    pub is_renewal: bool,

    /// Storage for the large list of circulation API flags that we
    /// don't explicitly defined elsewhere in this struct.
    pub options: HashMap<String, JsonValue>,

    /// Action string for logging / debugging
    pub action: Option<String>,
}

impl fmt::Display for Circulator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let empty = "null";
        let mut patron_barcode = String::from(empty);
        let mut copy_barcode = String::from(empty);
        let mut copy_status = -1;

        if let Some(p) = &self.patron {
            if let Some(bc) = &p["card"]["barcode"].as_str() {
                patron_barcode = bc.to_string();
            }
        }

        if let Some(c) = &self.copy {
            if let Some(bc) = &c["barcode"].as_str() {
                copy_barcode = bc.to_string()
            }
            if let Ok(s) = util::json_int(&c["status"]["id"]) {
                copy_status = s;
            }
        }

        let action = self.action.as_deref().unwrap_or(empty);

        write!(
            f,
            "Circulator action={} copy={} copy_status={} patron={}",
            action, copy_barcode, patron_barcode, copy_status
        )
    }
}

impl Circulator {
    /// Create a new Circulator.
    ///
    ///
    pub fn new(e: Editor, options: HashMap<String, JsonValue>) -> Result<Circulator, String> {
        if e.requestor().is_none() {
            Err(format!("Circulator requires an authenticated requestor"))?;
        }

        let settings = Settings::new(&e);
        let circ_lib = e.requestor_ws_ou();

        Ok(Circulator {
            editor: e,
            settings,
            options,
            circ_lib,
            exit_early: false,
            events: Vec::new(),
            open_circ: None,
            copy: None,
            copy_id: None,
            copy_state: None,
            patron: None,
            transit: None,
            is_noncat: false,
            is_renewal: false,
            changes_applied: false,
            action: None,
        })
    }

    /// Unchecked copy getter.
    ///
    /// Panics if copy is None.
    pub fn copy(&self) -> &JsonValue {
        self.copy.as_ref().unwrap()
    }

    /// Allows the caller to recover the original editor object after
    /// the circ action has completed.  The caller can use this to
    /// do other stuff with the original editor, make queries, etc.
    /// then finally commit the transaction.
    ///
    /// For code simplicity, replace the value with a cloned Editor
    /// instead of storing the Editor as an Option.
    pub fn take_editor(&mut self) -> Editor {
        let new_e = self.editor.clone();
        std::mem::replace(&mut self.editor, new_e)
    }

    pub fn begin(&mut self) -> Result<(), String> {
        self.editor.xact_begin()
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.editor.commit()
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        self.editor.rollback()
    }

    /// Returns Result so we can cause early exit on methods.
    pub fn exit_on_event_code(&mut self, code: &str) -> Result<(), String> {
        self.add_event_code(code);
        self.exit_early = true;
        Err(format!("Bailing on event: {code}"))
    }

    pub fn add_event_code(&mut self, code: &str) {
        self.events.push(EgEvent::new(code));
    }

    /// Search for the copy in question
    fn load_copy(&mut self) -> Result<(), String> {
        let copy_flesh = json::object! {
            flesh: 1,
            flesh_fields: {
                acp: COPY_FLESH
            }
        };

        // If we have loaded our item before, we can reload it directly
        // via its ID.
        let copy_id_op = match self.copy_id {
            Some(id) => Some(id),
            None => match self.options.get("copy_id") {
                Some(id2) => Some(util::json_int(&id2)?),
                None => None,
            },
        };

        if let Some(copy_id) = copy_id_op {
            let query = json::object! {id: copy_id};

            if let Some(copy) = self.editor.retrieve_with_ops("acp", query, copy_flesh)? {
                self.copy = Some(copy.to_owned());
            } else {
                self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
            }
        } else if let Some(copy_barcode) = self.options.get("copy_barcode") {
            // Non-cataloged items are assumed to not exist.
            if !self.is_noncat {
                let query = json::object! {
                    barcode: copy_barcode.clone(),
                    deleted: "f", // cstore turns false into NULL :\
                };

                if let Some(copy) = self
                    .editor
                    .search_with_ops("acp", query, copy_flesh)?
                    .first()
                {
                    self.copy = Some(copy.to_owned());
                } else {
                    self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
                }
            }
        }

        if let Some(c) = self.copy.as_ref() {
            self.copy_id = Some(util::json_int(&c["id"])?);
        }

        Ok(())
    }

    pub fn load_copy_alerts(&mut self, events: &[&str]) -> Result<(), String> {
        let copy_id = match self.copy_id {
            Some(i) => i,
            None => return Ok(()),
        };

        let list = self.editor.json_query(json::object! {
            from: ["asset.copy_state", copy_id]
        })?;

        if let Some(resp) = list.get(0) {
            // should always be a value.
            self.copy_state = resp["asset.copy_state"].as_str().map(|s| s.to_string());
        };

        self.generate_system_copy_alerts(events)?;

        Ok(())
    }

    ///
    fn generate_system_copy_alerts(&mut self, events: &[&str]) -> Result<(), String> {
        // System events need event types to focus on.
        if events.len() == 0 {
            return Ok(());
        }

        // Value set in load_copy_alerts()
        let copy_state = self.copy_state.as_deref().unwrap();

        // Avoid creating system copy alerts for "NORMAL" copies.
        if copy_state.eq("NORMAL") {
            return Ok(());
        }

        let copy_circ_lib = util::json_int(&self.copy()["circ_lib"])?;

        let query = json::object! {
            org: org::full_path(&mut self.editor, self.circ_lib, None)?
        };

        // actor.copy_alert_suppress
        let suppressions = self.editor.search("acas", query)?;

        let alert_orgs = org::ancestors(&mut self.editor, self.circ_lib)?;

        let query = json::object! {
            "active": "t",
            "scope_org": alert_orgs,
            "event": events,
            "state": copy_state,
            "-or": [{"in_renew": self.is_renewal}, {"in_renew": JsonValue::Null}]
        };

        // config.copy_alert_type
        let mut alert_types = self.editor.search("ccat", query)?;
        let mut wanted_types = Vec::new();

        while let Some(atype) = alert_types.pop() {
            // Filter on "only at circ lib"
            if util::json_bool(&atype["at_circ"]) {
                let at_circ_orgs = org::descendants(&mut self.editor, copy_circ_lib)?;

                if util::json_bool(&atype["invert_location"]) {
                    if at_circ_orgs.contains(&self.circ_lib) {
                        continue;
                    }
                } else if !at_circ_orgs.contains(&self.circ_lib) {
                    continue;
                }
            }

            // filter on "only at owning lib"
            if util::json_bool(&atype["at_owning"]) {
                let owner = util::json_int(&self.copy()["call_number"]["owning_lib"])?;
                let at_owner_orgs = org::descendants(&mut self.editor, owner)?;

                if util::json_bool(&atype["invert_location"]) {
                    if at_owner_orgs.contains(&self.circ_lib) {
                        continue;
                    }
                } else if !at_owner_orgs.contains(&self.circ_lib) {
                    continue;
                }
            }

            wanted_types.push(atype);
        }

        log::info!(
            "{self} settled on {} final copy alert types",
            wanted_types.len()
        );

        for mut atype in wanted_types {
            if let Some(ns) = atype["next_status"].as_str() {
                if suppressions.iter().any(|v| &v["alert_type"] == &atype["id"]) {
                    atype["next_status"] = JsonValue::new_array();
                } else {
                    atype["next_status"] = json::from(util::pg_unpack_int_array(ns));
                }
            }

            let alert = json::object! {
                alert_type: atype["id"].clone(),
                copy: self.copy_id.unwrap(),
                temp: "t",
                create_staff: self.editor.requestor_id(),
                create_time: "now",
                ack_staff: self.editor.requestor_id(),
                ack_time: "now",
            };

            let alert = self.editor.idl().create_from("aca", alert)?;
            let mut alert = self.editor.create(&alert)?;

            alert["alert_type"] = atype.clone(); // flesh

            if let Some(stat) = atype["next_status"].members().next() {
                // The Perl version tracks all of the next statuses,
                // but only ever uses the first.  Just track the first.
                self.options.insert("next_copy_status".to_string(), stat.clone());
            }

            // TODO

        }

        Ok(())
    }

    /// Find an open circulation linked to our copy if possible.
    fn load_open_circ(&mut self) -> Result<(), String> {
        if self.open_circ.is_some() {
            log::info!("{self} found an open circulation");
            // May have been set in load_patron()
            return Ok(());
        }

        if let Some(copy) = self.copy.as_ref() {
            let query = json::object! {
                target_copy: copy["id"].clone(),
                checkin_time: JsonValue::Null,
            };

            if let Some(circ) = self.editor.search("circ", query)?.first() {
                self.open_circ = Some(circ.to_owned());
                log::info!("{self} found an open circulation");
            }
        }

        Ok(())
    }

    /// Find the requested patron if possible.
    ///
    /// Also sets a value for self.circ if needed to find the patron.
    fn load_patron(&mut self) -> Result<(), String> {
        if let Some(patron_id) = self.options.get("patron_id") {
            let flesh = json::object! {
                flesh: 1,
                flesh_fields: {
                    au: ["card"]
                }
            };

            if let Some(patron) = self.editor.retrieve_with_ops("au", patron_id, flesh)? {
                self.patron = Some(patron.to_owned());
            } else {
                self.exit_on_event_code("ACTOR_USER_NOT_FOUND")?;
            }
        } else if let Some(patron_barcode) = self.options.get("patron_barcode") {
            let query = json::object! {barcode: patron_barcode.clone()};
            let flesh = json::object! {flesh: 1, flesh_fields: {"ac": ["usr"]}};

            if let Some(card) = self.editor.search_with_ops("ac", query, flesh)?.first() {
                let mut card = card.to_owned();

                let mut patron = card["usr"].take();
                card["usr"] = patron["id"].clone(); // de-flesh card->user
                patron["card"] = card; // flesh user->card
            } else {
                self.exit_on_event_code("ACTOR_USER_NOT_FOUND")?;
            }
        } else if let Some(ref copy) = self.copy {
            // See if we can find the circulation / patron related
            // to the provided copy.

            let query = json::object! {
                target_copy: copy["id"].clone(),
                checkin_time: JsonValue::Null,
            };

            let flesh = json::object! {
                flesh: 2,
                flesh_fields: {
                    circ: ["usr"],
                    au: ["card"],
                }
            };

            if let Some(circ) = self.editor.search_with_ops("circ", query, flesh)?.first() {
                // Flesh consistently
                let mut circ = circ.to_owned();
                let patron = circ["usr"].take();

                circ["usr"] = patron["id"].clone();

                self.patron = Some(patron);
                self.open_circ = Some(circ);
            }
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<(), String> {
        if let Some(cl) = self.options.get("circ_lib") {
            self.circ_lib = util::json_int(cl)?;
        }

        self.settings.set_org_id(self.circ_lib);
        self.is_noncat = util::json_bool_op(self.options.get("is_noncat"));

        self.load_copy()?;
        self.load_patron()?;
        self.load_open_circ()?;

        Ok(())
    }

    /// Update our copy with the values provided.
    ///
    /// * `changes` - a JSON Object with key/value copy attributes to update.
    pub fn update_copy(&mut self, changes: JsonValue) -> Result<&JsonValue, String> {
        let mut copy = match self.copy.take() {
            Some(c) => c,
            None => Err(format!("We have no copy to update"))?,
        };

        for (k, v) in changes.entries() {
            copy[k] = v.to_owned();
        }

        self.editor.idl().de_flesh_object(&mut copy)?;

        self.editor.update(&copy)?;

        // Load the updated copy with the usual fleshing.
        self.load_copy()?;

        Ok(self.copy.as_ref().unwrap())
    }

    /// Set a free-text option value to true.
    pub fn set_option_true(&mut self, name: &str) {
        self.options.insert(name.to_string(), json::from(true));
    }

    /// Get the value for a boolean option.
    ///
    /// Returns false if the value is unset or false-ish.
    pub fn get_option_bool(&self, name: &str) -> bool {
        if let Some(op) = self.options.get(name) {
            util::json_bool(op)
        } else {
            false
        }
    }
}
