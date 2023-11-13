use crate::common::holds;
use crate::common::org;
use crate::common::settings::Settings;
use crate::common::trigger;
use crate::constants as C;
use crate::editor::Editor;
use crate::event::{EgEvent, Overrides};
use crate::result::{EgError, EgResult};
use crate::util;
use crate::util::{json_bool, json_bool_op, json_int, json_string};
use json::JsonValue;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// These copy fields are assumed to be fleshed throughout.
/// NOTE changing these values can impact assumptions in the code.
const COPY_FLESH: &[&str] = &["status", "call_number", "parts", "floating", "location"];

/// Map of some newer override event types to simplified legacy override codes .
/// First entry in each sub-array is the newer event, followed by one or more
/// legacy event types.
const COPY_ALERT_OVERRIDES: &[&[&str]] = &[
    &["CLAIMSRETURNED\tCHECKOUT", "CIRC_CLAIMS_RETURNED"],
    &["CLAIMSRETURNED\tCHECKIN", "CIRC_CLAIMS_RETURNED"],
    &["LOST\tCHECKOUT", "CIRCULATION_EXISTS"],
    &["LONGOVERDUE\tCHECKOUT", "CIRCULATION_EXISTS"],
    &["MISSING\tCHECKOUT", "COPY_NOT_AVAILABLE"],
    &["DAMAGED\tCHECKOUT", "COPY_NOT_AVAILABLE"],
    &[
        "LOST_AND_PAID\tCHECKOUT",
        "COPY_NOT_AVAILABLE",
        "CIRCULATION_EXISTS",
    ],
];

#[derive(Debug, PartialEq, Clone)]
pub enum CircOp {
    Checkout,
    Checkin,
    Renew,
    Unset,
}

impl fmt::Display for CircOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: &str = self.into();
        write!(f, "{}", s)
    }
}

impl From<&CircOp> for &'static str {
    fn from(op: &CircOp) -> &'static str {
        match *op {
            CircOp::Checkout => "checkout",
            CircOp::Checkin => "checkin",
            CircOp::Renew => "renewal",
            CircOp::Unset => "unset",
        }
    }
}

/// Contains circ policy matchpoint data.
#[derive(Debug)]
pub struct CircPolicy {
    pub max_fine: f64,
    pub duration: String,
    pub recurring_fine: f64,
    pub matchpoint: JsonValue,
    pub duration_rule: JsonValue,
    pub recurring_fine_rule: JsonValue,
    pub max_fine_rule: JsonValue,
    pub hard_due_date: Option<JsonValue>,
    pub limit_groups: Option<JsonValue>,
}

/// Context and shared methods for circulation actions.
///
/// Innards are 'pub' since the impl's are spread across multiple files.
pub struct Circulator {
    pub editor: Option<Editor>,
    pub settings: Settings,
    pub circ_lib: i64,
    pub copy: Option<JsonValue>,
    pub copy_id: i64,
    pub copy_barcode: Option<String>,
    pub circ: Option<JsonValue>,
    pub hold: Option<JsonValue>,
    pub reservation: Option<JsonValue>,
    pub patron: Option<JsonValue>,
    pub patron_id: i64,
    pub transit: Option<JsonValue>,
    pub hold_transit: Option<JsonValue>,
    pub is_noncat: bool,
    pub system_copy_alerts: Vec<JsonValue>,
    pub runtime_copy_alerts: Vec<JsonValue>,
    pub is_override: bool,
    pub circ_op: CircOp,
    pub parent_circ: Option<i64>,
    pub deposit_billing: Option<JsonValue>,
    pub rental_billing: Option<JsonValue>,

    /// A circ test can be successfull without a matched policy
    /// if the matched policy is for
    pub circ_test_success: bool,
    pub circ_policy_unlimited: bool,

    /// Compiled rule set for a successful policy match.
    pub circ_policy_rules: Option<CircPolicy>,

    /// Raw results from the database.
    pub circ_policy_results: Option<Vec<JsonValue>>,

    /// When true, stop further processing and exit.
    /// This is not necessarily an error condition.
    pub exit_early: bool,

    pub override_args: Option<Overrides>,

    /// Events that need to be addressed.
    pub events: Vec<EgEvent>,

    pub renewal_remaining: i64,
    pub auto_renewal_remaining: i64,

    /// Override failures are tracked here so they can all be returned
    /// to the caller.
    pub failed_events: Vec<EgEvent>,

    /// None until a status is determined one way or the other.
    pub is_booking_enabled: Option<bool>,

    /// List of hold IDs for holds that need to be retargeted.
    pub retarget_holds: Option<Vec<i64>>,

    pub fulfilled_hold_ids: Option<Vec<i64>>,

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
        let mut patron_barcode = "null";
        let mut copy_status = "null";

        if let Some(p) = &self.patron {
            if let Some(bc) = &p["card"]["barcode"].as_str() {
                patron_barcode = bc;
            }
        }

        let copy_barcode = match self.copy_barcode.as_ref() {
            Some(b) => b,
            None => "null",
        };

        if let Some(c) = &self.copy {
            if let Some(s) = c["status"]["name"].as_str() {
                copy_status = s;
            }
        }

        write!(
            f,
            "Circ: op={} lib={} copy={} copy_status={} patron={}",
            self.circ_op, self.circ_lib, copy_barcode, copy_status, patron_barcode
        )
    }
}

impl Circulator {
    /// Create a new Circulator.
    ///
    pub fn new(e: Editor, options: HashMap<String, JsonValue>) -> EgResult<Circulator> {
        if e.requestor().is_none() {
            Err(format!("Circulator requires an authenticated requestor"))?;
        }

        let settings = Settings::new(&e);
        let circ_lib = e.requestor_ws_ou();

        Ok(Circulator {
            editor: Some(e),
            settings,
            options,
            circ_lib,
            events: Vec::new(),
            circ: None,
            parent_circ: None,
            hold: None,
            reservation: None,
            copy: None,
            copy_id: 0,
            copy_barcode: None,
            patron: None,
            patron_id: 0,
            transit: None,
            hold_transit: None,
            is_noncat: false,
            renewal_remaining: 0,
            deposit_billing: None,
            rental_billing: None,
            auto_renewal_remaining: 0,
            fulfilled_hold_ids: None,
            circ_test_success: false,
            circ_policy_unlimited: false,
            circ_policy_rules: None,
            circ_policy_results: None,
            system_copy_alerts: Vec::new(),
            runtime_copy_alerts: Vec::new(),
            is_override: false,
            override_args: None,
            failed_events: Vec::new(),
            exit_early: false,
            is_booking_enabled: None,
            retarget_holds: None,
            circ_op: CircOp::Unset,
        })
    }

    /// Panics if we have no editor
    pub fn editor(&mut self) -> &mut Editor {
        self.editor.as_mut().unwrap()
    }

    /// Panics unless we have an editor and a requestor.
    pub fn requestor_id(&self) -> i64 {
        self.editor.as_ref().unwrap().requestor_id()
    }

    /// Consumes an editor so we can use it.
    pub fn give_editor(&mut self, editor: Editor) {
        self.editor = Some(editor);
    }

    /// Gives the caller our editor.
    /// Panics if we have no editor.
    pub fn take_editor(&mut self) -> Editor {
        self.editor.take().unwrap()
    }

    pub fn is_renewal(&self) -> bool {
        self.circ_op == CircOp::Renew
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

    pub fn begin(&mut self) -> EgResult<()> {
        self.editor().xact_begin()
    }

    pub fn commit(&mut self) -> EgResult<()> {
        self.editor().commit()
    }

    pub fn rollback(&mut self) -> EgResult<()> {
        self.editor().rollback()
    }

    /// Used for events that stop processing and should result in
    /// a rollback on the main editor.
    pub fn exit_err_on_event_code(&mut self, code: &str) -> EgResult<()> {
        self.exit_err_on_event(EgEvent::new(code))
    }

    /// Used for events that stop processing and should result in
    /// a rollback on the main editor.
    pub fn exit_err_on_event(&mut self, evt: EgEvent) -> EgResult<()> {
        self.add_event(evt.clone());
        Err(EgError::Event(evt))
    }

    /// Sets a final event and sets the exit_early flag.
    ///
    /// This is for non-Error events that occur when logic has
    /// reached an endpoint that requires to further processing.
    pub fn exit_ok_on_event(&mut self, evt: EgEvent) -> EgResult<()> {
        self.add_event(evt);
        self.exit_ok()
    }

    /// Exit now without adding any additional events.
    pub fn exit_ok(&mut self) -> EgResult<()> {
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
    pub fn load_copy(&mut self) -> EgResult<()> {
        let copy_flesh = json::object! {
            flesh: 1,
            flesh_fields: {
                acp: COPY_FLESH
            }
        };

        // If we have loaded our item before, we can reload it directly
        // via its ID.
        let copy_id = if self.copy_id > 0 {
            self.copy_id
        } else if let Some(id) = self.options.get("copy_id") {
            json_int(id)?
        } else {
            0
        };

        if copy_id > 0 {
            if let Some(copy) = self
                .editor()
                .retrieve_with_ops("acp", copy_id, copy_flesh)?
            {
                self.copy = Some(copy);
            } else {
                self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
            }
        } else if let Some(copy_barcode) = self.options.get("copy_barcode") {
            self.copy_barcode = Some(json_string(&copy_barcode)?);

            let query = json::object! {
                barcode: copy_barcode.clone(),
                deleted: "f", // cstore turns json false into NULL :\
            };

            if let Some(copy) = self
                .editor()
                .search_with_ops("acp", query, copy_flesh)?
                .pop()
            {
                self.copy = Some(copy)
            } else {
                if self.circ_op != CircOp::Checkout {
                    // OK to checkout precat copies
                    self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
                }
            }
        }

        if let Some(c) = self.copy.as_ref() {
            self.copy_id = json_int(&c["id"])?;
            if self.copy_barcode.is_none() {
                self.copy_barcode = Some(json_string(&c["barcode"])?);
            }
        }

        Ok(())
    }

    /// Load copy alerts related to the copy we're working on.
    pub fn load_runtime_copy_alerts(&mut self) -> EgResult<()> {
        if self.copy.is_none() {
            return Ok(());
        }

        let query = json::object! {
            copy: self.copy_id,
            ack_time: JsonValue::Null,
        };

        let flesh = json::object! {
            flesh: 1,
            flesh_fields: {aca: ["alert_type"]}
        };

        for alert in self
            .editor()
            .search_with_ops("aca", query, flesh)?
            .drain(..)
        {
            self.runtime_copy_alerts.push(alert);
        }

        self.filter_runtime_copy_alerts()
    }

    /// Filter copy alerts by circ action, location, etc.
    fn filter_runtime_copy_alerts(&mut self) -> EgResult<()> {
        if self.runtime_copy_alerts.len() == 0 {
            return Ok(());
        }

        let circ_lib = self.circ_lib;
        let query = json::object! {
            org: org::full_path(self.editor(), circ_lib, None)?
        };

        // actor.copy_alert_suppress
        let suppressions = self.editor().search("acas", query)?;
        let copy_circ_lib = json_int(&self.copy()["circ_lib"])?;

        let mut wanted_alerts = Vec::new();

        let is_renewal = self.is_renewal();
        loop {
            let alert = match self.runtime_copy_alerts.pop() {
                Some(a) => a,
                None => break,
            };

            let atype = &alert["alert_type"];

            // Does this alert type only apply to renewals?
            let wants_renew = json_bool(&atype["in_renew"]);

            // Verify the alert type event matches what is currently happening.
            if is_renewal {
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
                let at_circ_orgs = org::descendants(self.editor(), copy_circ_lib)?;

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
                let at_owner_orgs = org::descendants(self.editor(), owner)?;

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
    pub fn load_system_copy_alerts(&mut self) -> EgResult<()> {
        if self.copy_id == 0 {
            return Ok(());
        }
        let copy_id = self.copy_id;
        let circ_lib = self.circ_lib;

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

        let list = self.editor().json_query(json::object! {
            from: ["asset.copy_state", copy_id]
        })?;

        let mut copy_state = "NORMAL";
        if let Some(hash) = list.get(0) {
            if let Some(state) = hash["asset.copy_state"].as_str() {
                copy_state = state;
            }
        }

        // Avoid creating system copy alerts for "NORMAL" copies.
        if copy_state.eq("NORMAL") {
            return Ok(());
        }

        let copy_circ_lib = json_int(&self.copy()["circ_lib"])?;

        let query = json::object! {
            org: org::full_path(self.editor(), circ_lib, None)?
        };

        // actor.copy_alert_suppress
        let suppressions = self.editor().search("acas", query)?;

        let alert_orgs = org::ancestors(self.editor(), circ_lib)?;

        let is_renew_filter = if self.is_renewal() { "t" } else { "f" };

        let query = json::object! {
            "active": "t",
            "scope_org": alert_orgs,
            "event": events,
            "state": copy_state,
            "-or": [{"in_renew": is_renew_filter}, {"in_renew": JsonValue::Null}]
        };

        // config.copy_alert_type
        let mut alert_types = self.editor().search("ccat", query)?;
        let mut wanted_types = Vec::new();

        while let Some(atype) = alert_types.pop() {
            // Filter on "only at circ lib"
            if json_bool(&atype["at_circ"]) {
                let at_circ_orgs = org::descendants(self.editor(), copy_circ_lib)?;

                if json_bool(&atype["invert_location"]) {
                    if at_circ_orgs.contains(&circ_lib) {
                        continue;
                    }
                } else if !at_circ_orgs.contains(&circ_lib) {
                    continue;
                }
            }

            // filter on "only at owning lib"
            if json_bool(&atype["at_owning"]) {
                let owner = json_int(&self.copy()["call_number"]["owning_lib"])?;
                let at_owner_orgs = org::descendants(self.editor(), owner)?;

                if json_bool(&atype["invert_location"]) {
                    if at_owner_orgs.contains(&circ_lib) {
                        continue;
                    }
                } else if !at_owner_orgs.contains(&circ_lib) {
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
                copy: self.copy_id,
                temp: "t",
                create_staff: self.requestor_id(),
                create_time: "now",
                ack_staff: self.requestor_id(),
                ack_time: "now",
            };

            let alert = self.editor().idl().create_from("aca", alert)?;
            let mut alert = self.editor().create(alert)?;

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
    ) -> EgResult<()> {
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

                if copy_override.ne(&"CIRCULATION_EXISTS") {
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

    /// Map alerts to events, which will be returned to the caller.
    ///
    /// Assumes new-style alerts are supported.
    pub fn check_copy_alerts(&mut self) -> EgResult<()> {
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
        if self.is_renewal() {
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
    fn load_circ(&mut self) -> EgResult<()> {
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

            if let Some(circ) = self.editor().search("circ", query)?.pop() {
                self.circ = Some(circ);
                log::info!("{self} found an open circulation");
            }
        }

        Ok(())
    }

    /// Find the requested patron if possible.
    ///
    /// Also sets a value for self.circ if needed to find the patron.
    fn load_patron(&mut self) -> EgResult<()> {
        if self.load_patron_by_id()? {
            return Ok(());
        }

        if self.load_patron_by_barcode()? {
            return Ok(());
        }

        if self.load_patron_by_copy()? {
            return Ok(());
        }

        Ok(())
    }

    /// Returns true if we were able to load the patron.
    fn load_patron_by_copy(&mut self) -> EgResult<bool> {
        let copy = match self.copy.as_ref() {
            Some(c) => c,
            None => return Ok(false),
        };

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

        let mut circ = match self.editor().search_with_ops("circ", query, flesh)?.pop() {
            Some(c) => c,
            None => return Ok(false),
        };

        // Flesh consistently
        let patron = circ["usr"].take();

        circ["usr"] = patron["id"].clone();

        self.patron_id = json_int(&patron["id"])?;
        self.patron = Some(patron);
        self.circ = Some(circ);

        Ok(true)
    }

    /// Returns true if we were able to load the patron.
    fn load_patron_by_barcode(&mut self) -> EgResult<bool> {
        let barcode = match self.options.get("patron_barcode") {
            Some(b) => b,
            None => return Ok(false),
        };

        let query = json::object! {barcode: barcode.clone()};
        let flesh = json::object! {flesh: 1, flesh_fields: {"ac": ["usr"]}};

        let mut card = match self.editor().search_with_ops("ac", query, flesh)?.pop() {
            Some(c) => c,
            None => {
                self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND")?;
                return Ok(false);
            }
        };

        let mut patron = card["usr"].take();

        card["usr"] = patron["id"].clone(); // de-flesh card->user
        patron["card"] = card; // flesh user->card

        self.patron_id = json_int(&patron["id"])?;
        self.patron = Some(patron);

        return Ok(true);
    }

    /// Returns true if we were able to load the patron by ID.
    fn load_patron_by_id(&mut self) -> EgResult<bool> {
        let patron_id = match self.options.get("patron_id") {
            Some(id) => id.clone(),
            None => return Ok(false),
        };

        let flesh = json::object! {flesh: 1, flesh_fields: {au: ["card"]}};

        let patron = self
            .editor()
            .retrieve_with_ops("au", patron_id, flesh)?
            .ok_or_else(|| self.editor().die_event())?;

        self.patron_id = json_int(&patron["id"])?;
        self.patron = Some(patron);

        Ok(true)
    }

    /// Load data common to most/all circulation operations.
    ///
    /// This should be called before any other circulation actions.
    pub fn init(&mut self) -> EgResult<()> {
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

    /// Perform post-commit tasks and cleanup, i.e. jobs that can
    /// be performed after one of our core actions (e.g. checkin) has
    /// completed and produced a response.
    pub fn post_commit_tasks(&mut self) -> EgResult<()> {
        self.retarget_holds()?;
        self.make_trigger_events()
    }

    /// Update our copy with the values provided.
    ///
    /// * `changes` - a JSON Object with key/value copy attributes to update.
    pub fn update_copy(&mut self, mut changes: JsonValue) -> EgResult<&JsonValue> {
        let mut copy = match self.copy.take() {
            Some(c) => c,
            None => Err(format!("We have no copy to update"))?,
        };

        copy["editor"] = json::from(self.requestor_id());
        copy["edit_date"] = json::from("now");

        for (k, v) in changes.entries_mut() {
            copy[k] = v.take();
        }

        self.editor().idl().de_flesh_object(&mut copy)?;

        self.editor().update(copy)?;

        // Load the updated copy with the usual fleshing.
        self.load_copy()?;

        Ok(self.copy.as_ref().unwrap())
    }

    /// Set a free-text option value to true.
    pub fn set_option_true(&mut self, name: &str) {
        self.options.insert(name.to_string(), json::from(true));
    }

    /// Delete an option key and value from our options hash.
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

    pub fn can_override_event(&self, textcode: &str) -> bool {
        if !self.is_override {
            return false;
        }

        let oargs = match self.override_args.as_ref() {
            Some(o) => o,
            None => return false,
        };

        match oargs {
            Overrides::All => true,
            // True if the list of events that we want to override
            // contains the textcode provided.
            Overrides::Events(v) => v.iter().map(|s| s.as_str()).any(|s| s == textcode),
        }
    }

    /// Attempts to override any events we have collected so far.
    ///
    /// Returns Err to exit early if any events exist that cannot
    /// be overridden either becuase we are not actively overriding
    /// or because an override permission check fails.
    pub fn try_override_events(&mut self) -> EgResult<()> {
        if self.events.len() == 0 {
            return Ok(());
        }

        // If we have a success event, keep it for returning later.
        let success: Option<EgEvent> = None;
        let selfstr = format!("{self}");

        loop {
            let evt = match self.events.pop() {
                Some(e) => e,
                None => break,
            };

            let can_override = self.can_override_event(evt.textcode());

            if !can_override {
                self.failed_events.push(evt);
                continue;
            }

            let perm = format!("{}.override", evt.textcode());
            log::info!("{selfstr} attempting to override: {perm}");

            // Override permissions are all global
            if !self.editor().allowed(&perm)? {
                if let Some(e) = self.editor().last_event().map(|e| e.clone()) {
                    // Track the permission failure as the event to return.
                    self.failed_events.push(e);
                } else {
                    // Should not get here.
                    self.failed_events.push(evt);
                }
            }
        }

        if self.failed_events.len() > 0 {
            log::info!("Exiting early on failed events: {:?}", self.failed_events);
            Err(EgError::Event(self.failed_events[0].clone()))
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
    pub fn set_booking_status(&mut self) -> EgResult<()> {
        if self.is_booking_enabled.is_some() {
            return Ok(());
        }

        if let Some(services) = self.editor().client_mut().send_recv_one(
            "router",
            "opensrf.router.info.class.list",
            None,
        )? {
            self.is_booking_enabled = Some(services.contains("open-ils.booking"));
        } else {
            // Should not get here since it means the Router is not resonding.
            self.is_booking_enabled = Some(false);
        }

        return Ok(());
    }

    /// True if the caller wants us to treat this as a precat circ/item.
    /// item must be a precat due to it using the precat call number.
    pub fn is_precat(&self) -> bool {
        json_bool_op(self.options.get("is_precat"))
    }

    /// True if we found a copy to work on and it's a precat item.
    pub fn is_precat_copy(&self) -> bool {
        if let Some(copy) = self.copy.as_ref() {
            if let Ok(cn) = json_int(&copy["call_number"]) {
                return cn == C::PRECAT_CALL_NUMBER;
            }
        }
        false
    }

    /// Retarget holds in our collected list of holds to retarget.
    fn retarget_holds(&mut self) -> EgResult<()> {
        let hold_ids = match self.retarget_holds.as_ref() {
            Some(list) => list.clone(),
            None => return Ok(()),
        };
        holds::retarget_holds(self.editor(), hold_ids.as_slice())
    }

    /// Create A/T events for checkout/checkin/renewal actions.
    fn make_trigger_events(&mut self) -> EgResult<()> {
        let circ = match self.circ.as_ref() {
            Some(c) => c,
            None => return Ok(()),
        };

        let action: &str = (&self.circ_op).into();

        if action == "other" {
            return Ok(());
        }

        trigger::create_events_for_object(
            self.editor.as_mut().unwrap(),
            action,
            circ,
            self.circ_lib,
            None,
            None,
            false,
        )
    }

    /// Remove duplicate events and remove any SUCCESS events if other
    /// event types are present.
    pub fn cleanup_events(&mut self) {
        if self.events.len() == 0 {
            return;
        }

        // Deduplicate
        let mut events: Vec<EgEvent> = Vec::new();
        for evt in self.events.drain(0..) {
            if !events.iter().any(|e| e.textcode() == evt.textcode()) {
                events.push(evt);
            }
        }

        if events.len() > 1 {
            // Multiple events mean something failed somewhere.
            // Remove any success events to avoid confusion.
            let mut new_events = Vec::new();
            for e in events.drain(..) {
                if !e.is_success() {
                    new_events.push(e);
                }
            }
            events = new_events;
        }

        self.events = events;
    }

    /// Events we have accumulated so far.
    pub fn events(&self) -> &Vec<EgEvent> {
        &self.events
    }

    /// Clears our list of compiled events and returns them to the caller.
    pub fn take_events(&mut self) -> Vec<EgEvent> {
        std::mem::replace(&mut self.events, Vec::new())
    }

    /// Make sure the requested item exists and is not marked deleted.
    pub fn basic_copy_checks(&mut self) -> EgResult<()> {
        if self.copy.is_none() {
            self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
        }
        self.handle_deleted_copy();
        Ok(())
    }

    pub fn handle_deleted_copy(&mut self) {
        if let Some(c) = self.copy.as_ref() {
            if json_bool(&c["deleted"]) {
                self.options
                    .insert(String::from("capture"), json::from("nocapture"));
            }
        }
    }
}
