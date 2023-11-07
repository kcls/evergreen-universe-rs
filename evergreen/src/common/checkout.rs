use crate::common::billing;
use crate::common::circulator::{CircOp, Circulator, CircPolicy};
use crate::common::holds;
use crate::common::noncat;
use crate::common::org;
use crate::common::penalty;
use crate::common::targeter;
use crate::common::transit;
use crate::constants as C;
use crate::date;
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util::{json_bool, json_int};
use json::JsonValue;
use std::time::Duration;

/// Performs item checkins
impl Circulator {
    /// Checkout an item.
    ///
    /// Returns Ok(()) if the active transaction should be committed and
    /// Err(EgError) if the active transaction should be rolled backed.
    pub fn checkout(&mut self) -> EgResult<()> {
        if self.circ_op == CircOp::Unset {
            self.circ_op = CircOp::Checkout;
        }

        log::info!("{self} starting checkout");

        if !self.is_renewal() {
            // We'll already be init-ed if we're renewing.
            self.init()?;
        }

        if self.patron.is_none() {
            return self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND");
        }

        self.handle_deleted_copy();

        if self.is_noncat {
            return self.checkout_noncat();
        }

        if self.is_precat() {
            self.create_precat_copy()?;
        } else if self.is_precat_copy() {
            self.exit_err_on_event_code("ITEM_NOT_CATALOGED")?;
        } else if self.copy.is_none() {
            self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
        }

        self.check_copy_status()?;
        self.handle_claims_returned()?;
        self.check_for_open_circ()?;
        self.check_circ_permit()?;

        Ok(())
    }

    fn checkout_noncat(&mut self) -> EgResult<()> {
        let noncat_type = match self.options.get("noncat_type") {
            Some(v) => v,
            None => return Err(format!("noncat_type required").into()),
        };

        let circ_lib = match self.options.get("noncat_circ_lib") {
            Some(cl) => json_int(&cl)?,
            None => self.circ_lib,
        };

        let count = match self.options.get("noncat_count") {
            Some(c) => json_int(&c)?,
            None => 1,
        };

        let mut checkout_time = None;
        if let Some(ct) = self.options.get("checkout_time") {
            if let Some(ct2) = ct.as_str() {
                checkout_time = Some(ct2);
            }
        }

        let mut circs = noncat::checkout(
            &mut self.editor,
            json_int(&self.patron.as_ref().unwrap()["id"])?,
            json_int(&noncat_type)?,
            circ_lib,
            count,
            checkout_time,
        )?;

        let mut evt = EgEvent::success();
        if let Some(c) = circs.pop() {
            // Perl API only returns the last created circulation
            evt.set_payload(json::object! {"noncat_circ": c});
        }
        self.add_event(evt);

        Ok(())
    }

    fn create_precat_copy(&mut self) -> EgResult<()> {
        if !self.is_renewal() {
            if !self.editor.allowed("CREATE_PRECAT")? {
                return Err(self.editor.die_event());
            }
        }

        // We already have a matching precat copy.
        // Update so we can reuse it.
        if self.copy.is_some() {
            return self.update_existing_precat();
        }

        let dummy_title = self
            .options
            .get("dummy_title")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let dummy_author = self
            .options
            .get("dummy_author")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let dummy_isbn = self
            .options
            .get("dummy_isbn")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let circ_modifier = self
            .options
            .get("circ_modifier")
            .map(|m| m.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        // Barcode required to get this far.
        let copy_barcode = self.copy_barcode.as_deref().unwrap();

        log::info!("{self} creating new pre-cat copy {copy_barcode}");

        let copy = json::object! {
            "circ_lib": self.circ_lib,
            "creator": self.editor.requestor_id(),
            "editor": self.editor.requestor_id(),
            "barcode": copy_barcode,
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
            "call_number": C::PRECAT_CALL_NUMBER,
            "loan_duration": C::PRECAT_COPY_LOAN_DURATION,
            "fine_level": C::PRECAT_COPY_FINE_LEVEL,
        };

        let mut copy = self.editor.idl().create_from("acp", copy)?;

        let pclib = self.settings.get_value("circ.pre_cat_copy_circ_lib")?;

        if let Some(sn) = pclib.as_str() {
            let o = org::by_shortname(&mut self.editor, sn)?;
            copy["circ_lib"] = json::from(o["id"].clone());
        }

        let copy = self.editor.create(copy)?;

        self.copy_id = Some(json_int(&copy["id"])?);

        // Reload a fleshed version of the copy we just created.
        self.load_copy()?;

        Ok(())
    }

    fn update_existing_precat(&mut self) -> EgResult<()> {
        let copy = self.copy.as_ref().unwrap(); // known good.

        log::info!("{self} modifying existing pre-cat copy {}", copy["id"]);

        let dummy_title = self
            .options
            .get("dummy_title")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_title"].as_str())
            .unwrap_or("");

        let dummy_author = self
            .options
            .get("dummy_author")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_author"].as_str())
            .unwrap_or("");

        let dummy_isbn = self
            .options
            .get("dummy_isbn")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_isbn"].as_str())
            .unwrap_or("");

        let circ_modifier = self
            .options
            .get("circ_modifier")
            .map(|m| m.as_str())
            .unwrap_or(copy["circ_modifier"].as_str());

        self.update_copy(json::object! {
            "editor": self.editor.requestor_id(),
            "edit_date": "now",
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
        })?;

        return Ok(());
    }

    fn check_copy_status(&mut self) -> EgResult<()> {
        if let Some(copy) = self.copy.as_ref() {
            if let Some(id) = copy["status"]["id"].as_i64() {
                if id == C::COPY_STATUS_IN_TRANSIT {
                    self.exit_err_on_event_code("COPY_IN_TRANSIT")?;
                }
            }
        }
        Ok(())
    }

    /// If there is an open claims-returned circ on our copy and
    /// we are in override mode, check in the circ.  Otherwise,
    /// exit with an event.
    fn handle_claims_returned(&mut self) -> EgResult<()> {

        let query = json::object! {
            "target_copy": self.copy_id.unwrap(),
            "stop_fines": "CLAIMSRETURNED",
            "checkin_time": JsonValue::Null,
        };

        let mut circ = match self.editor.search("circ", query)?.pop() {
            Some(c) => c,
            None => return Ok(()),
        };


        if !self.can_override_event("CIRC_CLAIMS_RETURNED") {
            return self.exit_err_on_event_code("CIRC_CLAIMS_RETURNED");
        }

        circ["checkin_time"] = json::from("now");
        circ["checkin_scan_time"] = json::from("now");
        circ["checkin_lib"] = json::from(self.circ_lib);
        circ["checkin_workstation"] = json::from(self.editor.requestor_ws_id().unwrap());
        circ["checkin_staff"] = json::from(self.editor.requestor_id());

        self.editor.update(circ).map(|_| ())
    }

    fn check_for_open_circ(&mut self) -> EgResult<()> {
        if self.is_renewal() {
            return Ok(());
        }

        let query = json::object! {
            "target_copy":  self.copy_id.unwrap(),
            "checkin_time": JsonValue::Null,
        };

        let circ = match self.editor.search("circ", query)?.pop() {
            Some(c) => c,
            None => return Ok(()),
        };

        let mut payload = json::object! {"copy": self.copy().clone()};

        if self.patron_id.unwrap() == json_int(&circ["usr"])? {
            payload["old_circ"] = circ.clone();

            // If there is an open circulation on the checkout item and
            // an auto-renew interval is defined, inform the caller
            // that they should go ahead and renew the item instead of
            // warning about open circulations.

            if let Some(intvl) =
                self.settings.get_value("circ.checkout_auto_renew_age")?.as_str() {
                let interval = date::interval_to_seconds(intvl)?;
                let xact_start = date::parse_datetime(circ["xact_start"].as_str().unwrap())?;

                let cutoff = xact_start + Duration::from_secs(interval as u64);

                if date::now() > cutoff {
                    payload["auto_renew"] = json::from(1);
                }
            }
        }

        let mut evt = EgEvent::new("OPEN_CIRCULATION_EXISTS");
        evt.set_payload(payload);

        self.exit_err_on_event(evt)
    }

    /// Collect runtime circ policy data from the database.
    ///
    /// self.circ_policy_results will contain whatever the database resturns.
    /// On success, self.circ_policy_rules will be populated.
    fn check_circ_permit(&mut self) -> EgResult<()> {
        let func = if self.is_renewal() {
            "action.item_user_renew_test"
        } else {
            "action.item_user_circ_test"
        };

        let copy_id = if self.is_noncat || (
            self.is_precat() && !self.is_override && !self.is_renewal()) {
            JsonValue::Null
        } else {
            json::from(self.copy_id.unwrap())
        };

        let query = json::object! {
            "from": [
                func,
                self.circ_lib,
                copy_id,
                self.patron_id.unwrap(),
            ]
        };

        let results = self.editor.json_query(query)?;

        if results.len() == 0 {
            return self.exit_err_on_event_code("NO_POLICY_MATCHPOINT");
        };

        // Pull the policy data from the first one, which will be the
        // success data if we have any.

        let policy = &results[0];

        self.circ_test_success = json_bool(&policy["success"]);

        if policy["matchpoint"].is_null() {
            self.circ_policy_results = Some(results);
            return Ok(());
        }

        // Delay generation of the err string if we don't need it.
        let err = || format!("Incomplete circ policy: {}", policy);

        let matchpoint = json_int(&policy["matchpoint"])?;
        let mut duration_rule = self.editor.retrieve("crcd", policy["duration_rule"].clone())?.ok_or_else(err)?;
        let mut recurring_fine_rule = self.editor.retrieve("crrf", policy["recurring_fine_rule"].clone())?.ok_or_else(err)?;
        let max_fine_rule = self.editor.retrieve("crmf", policy["max_fine_rule"].clone())?.ok_or_else(err)?;
        let hard_due_date = self.editor.retrieve("chdd", policy["hard_due_date"].clone())?.ok_or_else(err)?;
        let limit_groups = policy["limit_groups"].clone();

        if let Ok(n) = json_int(&policy["renewals"]) {
            duration_rule["max_renewals"] = json::from(n);
        }

        if let Some(s) = policy["grace_period"].as_str() {
            recurring_fine_rule["grace_period"] = json::from(s);
        }

        let rules = CircPolicy {
            matchpoint,
            duration_rule,
            recurring_fine_rule,
            max_fine_rule,
            hard_due_date,
            limit_groups,
        };

        self.circ_policy_rules = Some(rules);
        self.circ_policy_results = Some(results);

        return Ok(());
    }
}
