use crate::common::billing;
use crate::common::circulator::{CircOp, CircPolicy, Circulator};
use crate::common::noncat;
use crate::common::org;
use crate::constants as C;
use crate::date;
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util::{json_bool, json_bool_op, json_float, json_int, json_string};
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
        self.set_circ_policy()?;
        self.build_checkout_circ()?;
        self.apply_due_date()?;

        self.circ = Some(
            // At this point we know we have a circ.
            self.editor.create(self.circ.as_ref().unwrap().clone())?,
        );

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
        circ["checkin_staff"] = json::from(self.editor.requestor_id());

        if let Some(id) = self.editor.requestor_ws_id() {
            circ["checkin_workstation"] = json::from(id);
        }

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

            if let Some(intvl) = self
                .settings
                .get_value("circ.checkout_auto_renew_age")?
                .as_str()
            {
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
    fn set_circ_policy(&mut self) -> EgResult<()> {
        let func = if self.is_renewal() {
            "action.item_user_renew_test"
        } else {
            "action.item_user_circ_test"
        };

        let copy_id =
            if self.is_noncat || (self.is_precat() && !self.is_override && !self.is_renewal()) {
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

        if self.circ_test_success && policy["duration_rule"].is_null() {
            // Successful lookup with no duration rule indicates
            // unlimited item checkout.  Nothing left to lookup.
            self.circ_policy_unlimited = true;
            return Ok(());
        }

        if policy["matchpoint"].is_null() {
            self.circ_policy_results = Some(results);
            return Ok(());
        }

        // Delay generation of the err string if we don't need it.
        let err = || format!("Incomplete circ policy: {}", policy);

        let limit_groups = if policy["limit_groups"].is_array() {
            Some(policy["limit_groups"].clone())
        } else {
            None
        };

        let mut duration_rule = self
            .editor
            .retrieve("crcd", policy["duration_rule"].clone())?
            .ok_or_else(err)?;

        let mut recurring_fine_rule = self
            .editor
            .retrieve("crrf", policy["recurring_fine_rule"].clone())?
            .ok_or_else(err)?;

        let max_fine_rule = self
            .editor
            .retrieve("crmf", policy["max_fine_rule"].clone())?
            .ok_or_else(err)?;

        // optional
        let hard_due_date = self
            .editor
            .retrieve("chdd", policy["hard_due_date"].clone())?;

        if let Ok(n) = json_int(&policy["renewals"]) {
            duration_rule["max_renewals"] = json::from(n);
        }

        if let Some(s) = policy["grace_period"].as_str() {
            recurring_fine_rule["grace_period"] = json::from(s);
        }

        let max_fine = self.calc_max_fine(&max_fine_rule)?;
        let copy = self.copy();

        let copy_duration = json_int(&copy["loan_duration"])?;
        let copy_fine_level = json_int(&copy["fine_level"])?;

        let duration = match copy_duration {
            C::CIRC_DURATION_SHORT => json_string(&duration_rule["shrt"])?,
            C::CIRC_DURATION_EXTENDED => json_string(&duration_rule["extended"])?,
            _ => json_string(&duration_rule["normal"])?,
        };

        let recurring_fine = match copy_fine_level {
            C::CIRC_FINE_LEVEL_LOW => json_float(&recurring_fine_rule["low"])?,
            C::CIRC_FINE_LEVEL_HIGH => json_float(&recurring_fine_rule["high"])?,
            _ => json_float(&recurring_fine_rule["normal"])?,
        };

        let matchpoint = policy["matchpoint"].clone();

        let rules = CircPolicy {
            matchpoint,
            duration,
            recurring_fine,
            max_fine,
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

    fn calc_max_fine(&mut self, max_fine_rule: &JsonValue) -> EgResult<f64> {
        let rule_amount = json_float(&max_fine_rule["amount"])?;

        if json_bool(&max_fine_rule["is_percent"]) {
            let copy_price = billing::get_copy_price(&mut self.editor, self.copy_id.unwrap())?;
            return Ok((copy_price * rule_amount) / 100.0);
        }

        if json_bool(self.settings.get_value("circ.max_fine.cap_at_price")?) {
            let copy_price = billing::get_copy_price(&mut self.editor, self.copy_id.unwrap())?;
            let amount = if rule_amount > copy_price {
                copy_price
            } else {
                rule_amount
            };

            return Ok(amount);
        }

        Ok(rule_amount)
    }

    fn build_checkout_circ(&mut self) -> EgResult<()> {
        let mut circ = json::object! {
            "target_copy": self.copy_id.unwrap(),
            "usr": self.patron_id.unwrap(),
            "circ_lib": self.circ_lib,
            "circ_staff": self.editor.requestor_id(),
        };

        if let Some(ws) = self.editor.requestor_ws_id() {
            circ["workstation"] = json::from(ws);
        };

        if let Some(ct) = self.options.get("checkout_time") {
            circ["xact_start"] = ct.clone();
        }

        if let Some(id) = self.parent_circ {
            circ["parent_circ"] = json::from(id);
        }

        if self.is_renewal() {
            if json_bool_op(self.options.get("opac_renewal")) {
                circ["opac_renewal"] = json::from("t");
            }
            if json_bool_op(self.options.get("phone_renewal")) {
                circ["phone_renewal"] = json::from("t");
            }
            if json_bool_op(self.options.get("desk_renewal")) {
                circ["desk_renewal"] = json::from("t");
            }
            if json_bool_op(self.options.get("auto_renewal")) {
                circ["auto_renewal"] = json::from("t");
            }

            circ["renewal_remaining"] = json::from(self.renewal_remaining);
            circ["auto_renewal_remaining"] = json::from(self.auto_renewal_remaining);
        }

        if self.circ_policy_unlimited {
            circ["duration_rule"] = json::from(C::CIRC_POLICY_UNLIMITED);
            circ["recurring_fine_rule"] = json::from(C::CIRC_POLICY_UNLIMITED);
            circ["max_fine_rule"] = json::from(C::CIRC_POLICY_UNLIMITED);
            circ["renewal_remaining"] = json::from(0);
            circ["grace_period"] = json::from(0);
        } else if let Some(policy) = self.circ_policy_rules.as_ref() {
            circ["duration"] = json::from(policy.duration.to_string());
            circ["duration_rule"] = policy.duration_rule["name"].clone();

            circ["recurring_fine"] = json::from(policy.recurring_fine);
            circ["recurring_fine_rule"] = policy.recurring_fine_rule["name"].clone();
            circ["fine_interval"] = policy.recurring_fine_rule["recurrence_interval"].clone();

            circ["max_fine"] = json::from(policy.max_fine);
            circ["max_fine_rule"] = policy.max_fine_rule["name"].clone();

            circ["renewal_remaining"] = policy.duration_rule["max_renewals"].clone();
            circ["auto_renewal_remaining"] = policy.duration_rule["max_auto_renewals"].clone();

            // may be null
            circ["grace_period"] = policy.recurring_fine_rule["grace_period"].clone();
        } else {
            return Err(format!("Cannot build circ without a policy").into());
        }

        // We don't create the circ in the DB yet.
        self.circ = Some(circ);

        Ok(())
    }

    fn apply_due_date(&mut self) -> EgResult<()> {
        let is_manual = self.set_manual_due_date()?;

        if !is_manual {
            self.set_initial_due_date()?;
        }

        let shift_to_start = self.apply_booking_due_date(is_manual)?;

        if !is_manual {
            self.extend_due_date(shift_to_start)?;
        }

        Ok(())
    }

    /// Apply the user-provided due date.
    fn set_manual_due_date(&mut self) -> EgResult<bool> {
        if let Some(due_op) = self.options.get("due_date") {
            let due_str = due_op.as_str().ok_or(format!("Invalid manual due date"))?;

            if !self
                .editor
                .allowed_at("CIRC_OVERRIDE_DUE_DATE", self.circ_lib)?
            {
                return Err(self.editor.die_event());
            }

            self.circ.as_mut().unwrap()["due_date"] = json::from(due_str);
            return Ok(true);
        }

        Ok(false)
    }

    /// Set the initial circ due date based on the circulation policy info.
    fn set_initial_due_date(&mut self) -> EgResult<()> {
        // A force / manual due date overrides any policy calculation.
        let policy = match self.circ_policy_rules.as_ref() {
            Some(p) => p,
            None => return Ok(()),
        };

        let timezone = match self.settings.get_value("lib.timezone")?.as_str() {
            Some(s) => s,
            None => "local",
        };

        let start_date = match self.circ.as_ref().unwrap()["xact_start"].as_str() {
            Some(d) => date::parse_datetime(d)?,
            None => date::now(),
        };

        let start_date = date::set_timezone(start_date, timezone)?;

        let dur_secs = date::interval_to_seconds(&policy.duration)?;

        let mut due_date = start_date + Duration::from_secs(dur_secs as u64);

        if let Some(hdd) = policy.hard_due_date.as_ref() {
            let cdate_str = hdd["ceiling_date"].as_str().unwrap();
            let cdate = date::parse_datetime(cdate_str)?;
            let force = json_bool(&hdd["forceto"]);

            if cdate > date::now() {
                if cdate < due_date || force {
                    due_date = cdate;
                }
            }
        }

        self.circ.as_mut().unwrap()["due_date"] = json::from(date::to_iso(&due_date));

        Ok(())
    }

    /// Check for booking conflicts and shorten the due date if we need
    /// to apply some elbow room.
    fn apply_booking_due_date(&mut self, is_manual: bool) -> EgResult<bool> {
        if !self.is_booking_enabled() {
            return Ok(false);
        }

        let due_date = match self.circ.as_ref().unwrap()["due_date"].as_str() {
            Some(s) => s,
            None => return Ok(false),
        };

        let query = json::object! {"barcode": self.copy()["barcode"].clone()};
        let flesh = json::object! {"flesh": 1, "flesh_fields": {"brsrc": ["type"]}};

        let resource = match self.editor.search_with_ops("brsrc", query, flesh)?.pop() {
            Some(r) => r,
            None => return Ok(false),
        };

        let stop_circ = json_bool(
            self.settings
                .get_value("circ.booking_reservation.stop_circ")?,
        );

        let query = json::object! {
            "resource": resource["id"].clone(),
            "search_start": "now",
            "search_end": due_date,
            "fields": {
                "cancel_time": JsonValue::Null,
                "return_time": JsonValue::Null,
            }
        };

        let booking_ids_op = self.editor.client_mut().send_recv_one(
            "open-ils.booking",
            "open-ils.booking.reservations.filtered_id_list",
            query,
        )?;

        let booking_ids = match booking_ids_op {
            Some(i) => i,
            None => return Ok(false),
        };

        if !booking_ids.is_array() || booking_ids.len() == 0 {
            return Ok(false);
        }

        // See if any of the reservations overlap with our checkout
        let due_date_dt = date::parse_datetime(due_date)?;
        let now_dt = date::now();
        let mut bookings = Vec::new();

        // First see if we need to block the circulation due to
        // reservation overlap / stop-circ setting.
        for id in booking_ids.members() {
            let booking = self
                .editor
                .retrieve("bresv", id.clone())?
                .ok_or_else(|| self.editor.die_event())?;

            let booking_start = date::parse_datetime(booking["start_time"].as_str().unwrap())?;

            // Block the circ if a reservation is already active or
            // we're told to prevent new circs on matching resources.
            if booking_start < now_dt || stop_circ {
                self.exit_err_on_event_code("COPY_RESERVED")?;
            }

            bookings.push(booking);
        }

        if is_manual {
            // Manual due dates are not modified.  Note in the Perl
            // code they appear to be modified, but are later set
            // to the manual value, overwriting the booking logic
            // for manual dates.  Guessing manaul due date are an
            // outlier.
            return Ok(false);
        }

        // See if we need to shorten the circ duration for this resource.
        let shorten_by = match resource["type"]["elbow_room"].as_str() {
            Some(s) => s,
            None => match self
                .settings
                .get_value("circ.booking_reservation.default_elbow_room")?
                .as_str()
            {
                Some(s) => s,
                None => return Ok(false),
            },
        };

        // We're configured to shorten the circ in the presence of
        // reservations on this resource.
        let interval = date::interval_to_seconds(shorten_by)?;
        let due_date_dt = due_date_dt - Duration::from_secs(interval as u64);

        if due_date_dt < now_dt {
            self.exit_err_on_event_code("COPY_RESERVED")?;
        }

        // Apply the new due date and duration to our circ.
        let mut duration = due_date_dt.timestamp() - now_dt.timestamp();
        if duration % 86400 == 0 {
            // Avoid precise day-granular durations because they
            // result in bumping the due time to 23:59:59 via
            // DB trigger, which we don't want here.
            duration += 1;
        }

        let circ = self.circ.as_mut().unwrap();
        circ["duration"] = json::from(format!("{duration} seconds"));
        circ["due_date"] = json::from(date::to_iso(&due_date_dt));

        // Changes were made.
        Ok(true)
    }

    /// Extend the circ due date to avoid org unit closures.
    fn extend_due_date(&mut self, shift_to_start: bool) -> EgResult<()> {
        if self.is_renewal() {
            self.extend_renewal_due_date()?;
        }

        let due_date_str = match self.circ.as_ref().unwrap()["due_date"].as_str() {
            Some(s) => s,
            None => return Ok(()),
        };

        let due_date_dt = date::parse_datetime(due_date_str)?;

        let org_open_data = org::next_open_date(&mut self.editor, self.circ_lib, &due_date_dt)?;

        let due_date_dt = match org_open_data {
            // No org unit closuers to consider.
            org::OrgOpenState::Never | org::OrgOpenState::Open => return Ok(()),
            org::OrgOpenState::OpensOnDate(d) => d,
        };

        // NOTE the Perl uses shift_to_start (for booking) to bump the
        // due date to the beginning of the org unit closed period.
        // However, if the org unit is closed now, that can result in
        // an item being due now (or possibly in the past?).  There's a
        // TODO in the code about the logic.  Fow now, set the due date
        // to the first available time on or after the calculated due date.
        log::info!("{self} bumping due date to avoid closures: {}", due_date_dt);

        self.circ.as_mut().unwrap()["due_date"] = json::from(date::to_iso(&due_date_dt));

        Ok(())
    }

    /// Optionally extend the due date of a renewal if time was
    /// lost on renewing early.
    fn extend_renewal_due_date(&mut self) -> EgResult<()> {
        let policy = match self.circ_policy_rules.as_ref() {
            Some(p) => p,
            None => return Ok(()),
        };

        if !json_bool(&policy.matchpoint["renew_extends_due_date"]) {
            // Not configured to extend on the matching policy.
            return Ok(());
        }

        let due_date_str = match self.circ.as_ref().unwrap()["due_date"].as_str() {
            Some(d) => d,
            None => return Ok(()),
        };

        let due_date = date::parse_datetime(due_date_str)?;

        let parent_circ = self
            .parent_circ
            .ok_or_else(|| format!("Renewals require a parent circ"))?;

        let prev_circ = match self.editor.retrieve("circ", json::from(self.parent_circ))? {
            Some(c) => c,
            None => return Err(self.editor.die_event()),
        };

        let start_time_str = prev_circ["xact_start"].as_str().expect("required");
        let start_time = date::parse_datetime(start_time_str)?;

        let prev_due_date_str = prev_circ["due_date"].as_str().expect("required");
        let prev_due_date = date::parse_datetime(prev_due_date_str)?;

        let now_time = date::now();

        if prev_due_date < now_time {
            // Renewed circ was overdue.  No extension to apply.
            return Ok(());
        }

        // Make sure the renewal is not occurring too early in the
        // parent circ's lifecycle.
        if let Some(intvl) = policy.matchpoint["renew_extend_min_interval"].as_str() {
            let min_duration = date::interval_to_seconds(intvl)?;
            let co_duration = now_time - start_time;

            if co_duration.num_seconds() < min_duration {
                // Renewal occurred too early in the cycle to result in an
                // extension of the due date on the renewal.

                // If the new due date falls before the due date of
                // the previous circulation, though, use the due date of the
                // prev.  circ so the patron does not lose time.
                let due = if due_date < prev_due_date {
                    prev_due_date
                } else {
                    due_date
                };

                self.circ.as_mut().unwrap()["due_date"] = json::from(date::to_iso(&due));

                return Ok(());
            }
        }

        // Item was checked out long enough during the previous circulation
        // to consider extending the due date of the renewal to cover the gap.

        // Amount of the previous duration that was left unused.
        let remaining_duration = prev_due_date - now_time;

        let due_date = due_date + remaining_duration;

        // If the calculated due date falls before the due date of the previous
        // circulation, use the due date of the prev. circ so the patron does
        // not lose time.
        let due = if due_date < prev_due_date {
            prev_due_date
        } else {
            due_date
        };

        log::info!(
            "{self} renewal due date extension landed on due date: {}",
            due
        );

        self.circ.as_mut().unwrap()["due_date"] = json::from(date::to_iso(&due));

        Ok(())
    }
}
