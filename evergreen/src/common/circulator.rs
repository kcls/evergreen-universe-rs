use crate::common::org;
use crate::common::settings::Settings;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::util;
use crate::util::{json_bool, json_bool_op, json_int};
use json::JsonValue;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// These copy fields are assumed to be fleshed throughout.
const COPY_FLESH: &[&str] = &["status", "call_number", "parts", "floating", "location"];

/// Map of some newer override event types to simplified legacy override codes .
/// First entry in each sub-array is the newer event, followed by one or more
/// legacy event types.
const COPY_ALERT_OVERRIDES: &[&[&str]] = &[
    &["CLAIMSRETURNED\tCHECKOUT", "CIRC_CLAIMS_RETURNED"],
    &["CLAIMSRETURNED\tCHECKIN", "CIRC_CLAIMS_RETURNED"],
    &["LOST\tCHECKOUT", "circULATION_EXISTS"],
    &["LONGOVERDUE\tCHECKOUT", "circULATION_EXISTS"],
    &["MISSING\tCHECKOUT", "COPY_NOT_AVAILABLE"],
    &["DAMAGED\tCHECKOUT", "COPY_NOT_AVAILABLE"],
    &[
        "LOST_AND_PAID\tCHECKOUT",
        "COPY_NOT_AVAILABLE",
        "circULATION_EXISTS",
    ],
];

#[derive(Debug, PartialEq, Clone)]
pub enum CircOp {
    Checkout,
    Checkin,
    Renew,
    Other,
}

impl fmt::Display for CircOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Checkout => write!(f, "checkout"),
            Self::Checkin => write!(f, "checkin"),
            Self::Renew => write!(f, "renew"),
            Self::Other => write!(f, "other"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Overrides {
    All,
    Events(Vec<String>),
}

/// Context and shared methods for circulation actions.
///
/// Innards are 'pub' since the impl's are spread across multiple files.
pub struct Circulator {
    pub editor: Editor,
    pub settings: Settings,
    pub circ_lib: i64,
    pub copy: Option<JsonValue>,
    pub copy_id: Option<i64>,
    pub circ: Option<JsonValue>,
    pub hold: Option<JsonValue>,
    pub reservation: Option<JsonValue>,
    pub patron: Option<JsonValue>,
    pub transit: Option<JsonValue>,
    pub hold_transit: Option<JsonValue>,
    pub is_noncat: bool,
    pub system_copy_alerts: Vec<JsonValue>,
    pub runtime_copy_alerts: Vec<JsonValue>,
    pub is_override: bool,
    pub circ_op: CircOp,

    /// When true, stop further processing and exit.
    /// This is not necessarily an error condition.
    pub exit_early: bool,

    pub override_args: Option<Overrides>,

    /// Events that need to be addressed.
    pub events: Vec<EgEvent>,

    /// Override failures are tracked here so they can all be returned
    /// to the caller.
    pub failed_events: Vec<EgEvent>,

    /// None until a status is determined one way or the other.
    pub is_booking_enabled: Option<bool>,

    /// Storage for the large list of circulation API flags that we
    /// don't explicitly define in this struct.
    ///
    /// General plan so far is if the value is only used by a specific
    /// circ_op (e.g. checkin) then make it an option.  If it's used
    /// more or less globally for circ stuff, make it part of the
    /// Circulator proper.
    pub options: HashMap<String, JsonValue>,
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
            if let Ok(s) = json_int(&c["status"]["id"]) {
                copy_status = s;
            }
        }

        write!(
            f,
            "Circulator action={} circ_lib={} copy={} copy_status={} patron={}",
            self.circ_op, self.circ_lib, copy_barcode, patron_barcode, copy_status
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
            events: Vec::new(),
            circ: None,
            hold: None,
            reservation: None,
            copy: None,
            copy_id: None,
            patron: None,
            transit: None,
            hold_transit: None,
            is_noncat: false,
            system_copy_alerts: Vec::new(),
            runtime_copy_alerts: Vec::new(),
            is_override: false,
            override_args: None,
            failed_events: Vec::new(),
            exit_early: false,
            is_booking_enabled: None,
            circ_op: CircOp::Other,
        })
    }

    /// Panics if the booking status is unknown.
    pub fn is_booking_enabled(&self) -> bool {
        self.is_booking_enabled.unwrap()
    }

    /// Unchecked copy getter.
    ///
    /// Panics if copy is None.
    pub fn copy(&self) -> &JsonValue {
        self.copy
            .as_ref()
            .expect("{self} self.copy() requires a copy")
    }

    /// Returns the copy status ID.
    ///
    /// Panics if we have no copy.
    pub fn copy_status(&self) -> i64 {
        let copy = self
            .copy
            .as_ref()
            .expect("{self} copy required for copy_status()");
        json_int(&copy["status"]["id"]).expect("{self} invalid fleshed copy status value")
    }

    /// Returns the copy circ lib ID.
    ///
    /// Panics if we have no copy.
    pub fn copy_circ_lib(&self) -> i64 {
        let copy = self
            .copy
            .as_ref()
            .expect("{self} copy required for copy_circ_lib()");

        json_int(&copy["circ_lib"]).expect("{self} invlid copy circ lib")
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

    /// Used for Error events that stop processing, i.e. cannot be overridden.
    pub fn exit_err_on_event_code(&mut self, code: &str) -> Result<(), String> {
        self.add_event_code(code);
        Err(format!("Bailing on event: {code}"))
    }

    /// Sets a final event and sets the exit_early flag.
    ///
    /// This is for non-Error events that occur when logic has
    /// reached an endpoint that requires to further processing.
    pub fn exit_ok_on_event(&mut self, evt: EgEvent) -> Result<(), String> {
        self.add_event(evt);
        self.exit_ok()
    }

    /// Exit now without adding any additional events.
    pub fn exit_ok(&mut self) -> Result<(), String> {
        self.exit_early = true;
        Ok(())
    }


    /// Add a potentially overridable event to our events list (by code).
    pub fn add_event_code(&mut self, code: &str) {
        self.add_event(EgEvent::new(code));
    }

    /// Add a potentially overridable event to our events list.
    pub fn add_event(&mut self, evt: EgEvent) {
        self.events.push(evt);
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
                Some(id2) => Some(json_int(&id2)?),
                None => None,
            },
        };

        if let Some(copy_id) = copy_id_op {
            let query = json::object! {id: copy_id};

            if let Some(copy) = self.editor.retrieve_with_ops("acp", query, copy_flesh)? {
                self.copy = Some(copy.to_owned());
            } else {
                self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
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
                    self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
                }
            }
        }

        if let Some(c) = self.copy.as_ref() {
            self.copy_id = Some(json_int(&c["id"])?);
        }

        Ok(())
    }

    pub fn load_runtime_copy_alerts(&mut self) -> Result<(), String> {
        if self.copy.is_none() {
            return Ok(());
        }

        let query = json::object! {
            copy: self.copy_id.unwrap(), // if have copy, have id.
            ack_time: JsonValue::Null,
        };

        let flesh = json::object! {
            flesh: 1,
            flesh_fields: {aca: ["alert_type"]}
        };

        for alert in self.editor.search_with_ops("aca", query, flesh)? {
            self.runtime_copy_alerts.push(alert.to_owned());
        }

        self.filter_runtime_copy_alerts()
    }

    fn filter_runtime_copy_alerts(&mut self) -> Result<(), String> {
        if self.runtime_copy_alerts.len() == 0 {
            return Ok(());
        }

        let query = json::object! {
            org: org::full_path(&mut self.editor, self.circ_lib, None)?
        };

        // actor.copy_alert_suppress
        let suppressions = self.editor.search("acas", query)?;
        let copy_circ_lib = json_int(&self.copy()["circ_lib"])?;

        let mut wanted_alerts = Vec::new();

        for alert in self.runtime_copy_alerts.drain(0..) {
            let atype = &alert["alert_type"];

            // Does this alert type only apply to renewals?
            let wants_renew = json_bool(&atype["in_renew"]);

            // Verify the alert type event matches what is currently happening.
            if self.circ_op == CircOp::Renew {
                if !wants_renew {
                    continue;
                }
            } else {
                if wants_renew {
                    continue;
                }
                if let Some(event) = atype["event"].as_str() {
                    if event.eq("CHECKOUT") && self.circ_op != CircOp::Checkout {
                        continue;
                    }
                    if event.eq("CHECKIN") && self.circ_op != CircOp::Checkin {
                        continue;
                    }
                }
            }

            // Verify this alert type is not locally suppressed.
            if suppressions.iter().any(|a| a["alert_type"] == atype["id"]) {
                continue;
            }

            // TODO below mimics load_system_copy_alerts - refactor?

            // Filter on "only at circ lib"
            if json_bool(&atype["at_circ"]) {
                let at_circ_orgs = org::descendants(&mut self.editor, copy_circ_lib)?;

                if json_bool(&atype["invert_location"]) {
                    if at_circ_orgs.contains(&self.circ_lib) {
                        continue;
                    }
                } else if !at_circ_orgs.contains(&self.circ_lib) {
                    continue;
                }
            }

            // filter on "only at owning lib"
            if json_bool(&atype["at_owning"]) {
                let owner = json_int(&self.copy.as_ref().unwrap()["call_number"]["owning_lib"])?;
                let at_owner_orgs = org::descendants(&mut self.editor, owner)?;

                if json_bool(&atype["invert_location"]) {
                    if at_owner_orgs.contains(&self.circ_lib) {
                        continue;
                    }
                } else if !at_owner_orgs.contains(&self.circ_lib) {
                    continue;
                }
            }

            // The Perl code unnests the alert type's next_status value
            // here, but I have not yet found where it uses it.
            wanted_alerts.push(alert);
        }

        self.runtime_copy_alerts = wanted_alerts;

        Ok(())
    }

    ///
    pub fn load_system_copy_alerts(&mut self) -> Result<(), String> {
        let copy_id = match self.copy_id {
            Some(i) => i,
            None => return Ok(()),
        };

        // System events need event types to focus on.
        let events: &[&str] = if self.circ_op == CircOp::Renew {
            &["CHECKOUT", "CHECKIN"]
        } else if self.circ_op == CircOp::Checkout {
            &["CHECKOUT"]
        } else if self.circ_op == CircOp::Checkin {
            &["CHECKIN"]
        } else {
            return Ok(());
        };

        let list = self.editor.json_query(json::object! {
            from: ["asset.copy_state", copy_id]
        })?;

        // Every copy as a copy state
        let copy_state = list[0].as_str().unwrap();

        // Avoid creating system copy alerts for "NORMAL" copies.
        if copy_state.eq("NORMAL") {
            return Ok(());
        }

        let copy_circ_lib = json_int(&self.copy()["circ_lib"])?;

        let query = json::object! {
            org: org::full_path(&mut self.editor, self.circ_lib, None)?
        };

        // actor.copy_alert_suppress
        let suppressions = self.editor.search("acas", query)?;

        let alert_orgs = org::ancestors(&mut self.editor, self.circ_lib)?;

        let is_renew_filter = if self.circ_op == CircOp::Renew {
            "t"
        } else {
            "f"
        };

        let query = json::object! {
            "active": "t",
            "scope_org": alert_orgs,
            "event": events,
            "state": copy_state,
            "-or": [{"in_renew": is_renew_filter}, {"in_renew": JsonValue::Null}]
        };

        // config.copy_alert_type
        let mut alert_types = self.editor.search("ccat", query)?;
        let mut wanted_types = Vec::new();

        while let Some(atype) = alert_types.pop() {
            // Filter on "only at circ lib"
            if json_bool(&atype["at_circ"]) {
                let at_circ_orgs = org::descendants(&mut self.editor, copy_circ_lib)?;

                if json_bool(&atype["invert_location"]) {
                    if at_circ_orgs.contains(&self.circ_lib) {
                        continue;
                    }
                } else if !at_circ_orgs.contains(&self.circ_lib) {
                    continue;
                }
            }

            // filter on "only at owning lib"
            if json_bool(&atype["at_owning"]) {
                let owner = json_int(&self.copy()["call_number"]["owning_lib"])?;
                let at_owner_orgs = org::descendants(&mut self.editor, owner)?;

                if json_bool(&atype["invert_location"]) {
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

        let mut auto_override_conditions = HashSet::new();

        for mut atype in wanted_types {
            if let Some(ns) = atype["next_status"].as_str() {
                if suppressions
                    .iter()
                    .any(|v| &v["alert_type"] == &atype["id"])
                {
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
                self.options
                    .insert("next_copy_status".to_string(), stat.clone());
            }

            if suppressions.iter().any(|a| a["alert_type"] == atype["id"]) {
                auto_override_conditions.insert(format!("{}\t{}", atype["state"], atype["event"]));
            } else {
                self.system_copy_alerts.push(alert);
            }
        }

        self.add_overrides_from_system_copy_alerts(auto_override_conditions)
    }

    fn add_overrides_from_system_copy_alerts(
        &mut self,
        conditions: HashSet<String>,
    ) -> Result<(), String> {
        for condition in conditions.iter() {
            let map = match COPY_ALERT_OVERRIDES
                .iter()
                .filter(|m| m[0].eq(condition))
                .next()
            {
                Some(m) => m,
                None => continue,
            };

            self.is_override = true;
            let mut checkin_required = false;

            for copy_override in &map[1..] {
                if let Some(ov_args) = &mut self.override_args {
                    // Only track specific events if we are not overriding "All".
                    if let Overrides::Events(ev) = ov_args {
                        ev.push(copy_override.to_string());
                    }
                }

                if copy_override.ne(&"circULATION_EXISTS") {
                    continue;
                }

                // Special handling for lsot/long-overdue circs

                let setting = match condition.split("\t").next().unwrap() {
                    "LOST" | "LOST_AND_PAID" => "circ.copy_alerts.forgive_fines_on_lost_checkin",
                    "LONGOVERDUE" => "circ.copy_alerts.forgive_fines_on_long_overdue_checkin",
                    _ => continue,
                };

                if json_bool(self.settings.get_value(setting)?) {
                    self.set_option_true("void_overdues");
                }

                self.set_option_true("noop");
                checkin_required = true;
            }

            // If we are mid-checkout (not checkin or renew), force
            // a checkin here (which will be no-op) so the item can be
            // reset before the checkout resumes.
            if CircOp::Checkout == self.circ_op {
                if checkin_required {
                    self.checkin()?;
                }
            }
        }

        Ok(())
    }

    /// Assumes new-style alerts are supported.
    pub fn check_copy_alerts(&mut self) -> Result<(), String> {
        if self.copy.is_none() {
            return Ok(());
        }

        let mut alert_on = Vec::new();
        for alert in self.runtime_copy_alerts.iter() {
            alert_on.push(alert.clone());
        }

        for alert in self.system_copy_alerts.iter() {
            alert_on.push(alert.clone());
        }

        if alert_on.len() > 0 {
            // We have new-style alerts to reports.
            let mut evt = EgEvent::new("COPY_ALERT_MESSAGE");
            evt.set_payload(json::from(alert_on));
            self.add_event(evt);
            return Ok(());
        }

        // No new-style alerts.  See if the copy itself has one.
        if self.circ_op == CircOp::Renew {
            return Ok(());
        }

        if let Some(msg) = self.copy()["alert_message"].as_str() {
            let mut evt = EgEvent::new("COPY_ALERT_MESSAGE");
            evt.set_payload(json::from(msg));
            self.add_event(evt);
        }

        Ok(())
    }

    /// Find an open circulation linked to our copy if possible.
    fn load_circ(&mut self) -> Result<(), String> {
        if self.circ.is_some() {
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
                self.circ = Some(circ.to_owned());
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
                self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND")?;
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
                self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND")?;
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
                self.circ = Some(circ);
            }
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<(), String> {
        if let Some(cl) = self.options.get("circ_lib") {
            self.circ_lib = json_int(cl)?;
        }

        self.settings.set_org_id(self.circ_lib);
        self.is_noncat = json_bool_op(self.options.get("is_noncat"));

        self.load_copy()?;
        self.load_patron()?;
        self.load_circ()?;
        self.set_booking_status()?;

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

    pub fn clear_option(&mut self, name: &str) {
        self.options.remove(name);
    }

    /// Get the value for a boolean option.
    ///
    /// Returns false if the value is unset or false-ish.
    pub fn get_option_bool(&self, name: &str) -> bool {
        if let Some(op) = self.options.get(name) {
            json_bool(op)
        } else {
            false
        }
    }

    /// Attempts to override any events we have collected so far.
    ///
    /// Returns Err to exit early if any events exist that cannot
    /// be overridden either becuase we are not actively overriding
    /// or because an override permission check fails.
    pub fn try_override_events(&mut self) -> Result<(), String> {
        if self.events.len() == 0 {
            return Ok(());
        }

        // If we have a success event, keep it for returning later.
        let mut success: Option<EgEvent> = None;
        let selfstr = format!("{self}");

        for evt in self.events.drain(0..) {
            if evt.textcode() == "SUCCESS" {
                success = Some(evt);
                continue;
            }

            if !self.is_override || self.override_args.is_none() {
                self.failed_events.push(evt);
                continue;
            }

            let oargs = self.override_args.as_ref().unwrap(); // verified above

            // Asked to override specific event types.  See if this
            // event type matches.
            if let Overrides::Events(v) = oargs {
                if !v.iter().map(|s| s.as_str()).any(|s| s == evt.textcode()) {
                    self.failed_events.push(evt);
                    continue;
                }
            }

            let perm = format!("{}.override", evt.textcode());
            log::info!("{selfstr} attempting to override: {perm}");

            // Override permissions are all global
            if !self.editor.allowed(&perm, None)? {
                if let Some(e) = self.editor.last_event() {
                    // Track the permission failure as the event to return.
                    self.failed_events.push(e.clone());
                } else {
                    // Should not get here.
                    self.failed_events.push(evt);
                }
            }
        }

        if self.failed_events.len() > 0 {
            Err(format!(
                "Exiting early on failed events: {:?}",
                self.failed_events
            ))
        } else {
            // If all is well and we encountered a SUCCESS event, keep
            // it in place so it can ultimately be returned to the caller.
            if let Some(evt) = success {
                self.events = vec![evt];
            };

            Ok(())
        }
    }


    /// Sets the is_booking_enable flag if not previously set.
    ///
    /// TODO: make this a host setting so we can avoid the network call.
    pub fn set_booking_status(&mut self) -> Result<(), String> {
        if self.is_booking_enabled.is_some() {
            return Ok(());
        }

        if let Some(services) = self.editor
            .client_mut()
            .send_recv_one("router", "opensrf.router.info.class.list", None)? {

            self.is_booking_enabled = Some(services.contains("open-ils.booking"));

        } else {
            // Should not get here since it means the Router is not resonding.
            self.is_booking_enabled = Some(false);
        }

        return Ok(())
    }
}
