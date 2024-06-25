use crate as eg;
use eg::common::bib;
use eg::common::billing;
use eg::common::circulator::{CircOp, CircPolicy, Circulator, LEGACY_CIRC_EVENT_MAP};
use eg::common::holds;
use eg::common::noncat;
use eg::common::org;
use eg::common::penalty;
use eg::constants as C;
use eg::date;
use eg::event::EgEvent;
use eg::result::EgResult;
use eg::EgValue;
use std::time::Duration;

/// Performs item checkins
impl Circulator<'_> {
    /// Checkout an item.
    ///
    /// Returns Ok(()) if the active transaction completed and should
    /// (probably) be committed and Err(EgError) if the active
    /// transaction should be rolled backed.
    pub fn checkout(&mut self) -> EgResult<()> {
        if self.circ_op == CircOp::Unset {
            self.circ_op = CircOp::Checkout;
        }
        self.init()?;

        log::info!("{self} starting checkout");

        if self.patron.is_none() {
            return self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND");
        }

        self.base_checkout_perms()?;

        self.set_circ_policy()?;
        self.inspect_policy_failures()?;
        self.check_copy_alerts()?;
        self.try_override_events()?;

        if self.is_inspect() {
            return Ok(());
        }

        if self.is_noncat {
            return self.checkout_noncat();
        }

        if self.precat_requested() {
            self.create_precat_copy()?;
        } else if self.is_precat_copy() && !self.is_renewal() {
            self.exit_err_on_event_code("ITEM_NOT_CATALOGED")?;
        }

        self.basic_copy_checks()?;
        self.set_item_deposit_events()?;
        self.check_captured_hold()?;
        self.check_copy_status()?;
        self.handle_claims_returned()?;
        self.check_for_open_circ()?;

        self.try_override_events()?;

        // We've tested everything we can.  Build the circulation.

        self.build_checkout_circ()?;
        self.apply_due_date()?;
        self.save_checkout_circ()?;
        self.apply_limit_groups()?;

        self.apply_deposit_fee()?;
        self.handle_checkout_holds()?;

        penalty::calculate_penalties(self.editor, self.patron_id, self.circ_lib, None)?;

        self.build_checkout_response()
    }

    /// Perms that are always needed for checkout.
    fn base_checkout_perms(&mut self) -> EgResult<()> {
        let cl = self.circ_lib;

        if !self.is_renewal() && !self.editor().allowed_at("COPY_CHECKOUT", cl)? {
            return Err(self.editor().die_event());
        }

        if self.patron_id != self.editor().requestor_id()? {
            // Users are allowed to "inspect" their own data.
            if !self.editor().allowed_at("VIEW_PERMIT_CHECKOUT", cl)? {
                return Err(self.editor().die_event());
            }
        }

        Ok(())
    }

    fn checkout_noncat(&mut self) -> EgResult<()> {
        let noncat_type = match self.options.get("noncat_type") {
            Some(v) => v,
            None => return Err(format!("noncat_type required").into()),
        };

        let circ_lib = match self.options.get("noncat_circ_lib") {
            Some(cl) => cl.int()?,
            None => self.circ_lib,
        };

        let count = match self.options.get("noncat_count") {
            Some(c) => c.int()?,
            None => 1,
        };

        let mut checkout_time = None;
        if let Some(ct) = self.options.get("checkout_time") {
            if let Some(ct2) = ct.as_str() {
                checkout_time = Some(ct2.to_string());
            }
        }

        let patron_id = self.patron_id;
        let noncat_type = noncat_type.int()?;

        let mut circs = noncat::checkout(
            self.editor(),
            patron_id,
            noncat_type,
            circ_lib,
            count,
            checkout_time.as_deref(),
        )?;

        let mut evt = EgEvent::success();
        if let Some(c) = circs.pop() {
            // Perl API only returns the last created circulation
            evt.set_payload(eg::hash! {"noncat_circ": c});
        }
        self.add_event(evt);

        Ok(())
    }

    fn create_precat_copy(&mut self) -> EgResult<()> {
        if !self.is_renewal() && !self.editor().allowed("CREATE_PRECAT")? {
            return Err(self.editor().die_event());
        }

        // We already have a matching precat copy.
        // Update so we can reuse it.
        if self.copy.is_some() {
            return self.update_existing_precat();
        }

        let reqr_id = self.requestor_id()?;

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

        let copy = eg::hash! {
            "circ_lib": self.circ_lib,
            "creator": reqr_id,
            "editor": reqr_id,
            "barcode": copy_barcode,
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
            "call_number": C::PRECAT_CALL_NUMBER,
            "loan_duration": C::PRECAT_COPY_LOAN_DURATION,
            "fine_level": C::PRECAT_COPY_FINE_LEVEL,
        };

        let mut copy = EgValue::create("acp", copy)?;

        let pclib = self
            .settings
            .get_value("circ.pre_cat_copy_circ_lib")?
            .clone();

        if let Some(sn) = pclib.as_str() {
            let o = org::by_shortname(self.editor(), sn)?;
            copy["circ_lib"] = o["id"].clone();
        }

        let copy = self.editor().create(copy)?;

        self.copy_id = copy.id()?;

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

        self.update_copy(eg::hash! {
            "editor": self.requestor_id()?,
            "edit_date": "now",
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
        })?;

        return Ok(());
    }

    fn set_item_deposit_events(&mut self) -> EgResult<()> {
        if self.is_deposit() && !self.is_deposit_exempt()? {
            let mut evt = EgEvent::new("ITEM_DEPOSIT_REQUIRED");
            evt.set_payload(self.copy().clone());
            self.add_event(evt)
        }

        if self.is_rental() && !self.is_rental_exempt()? {
            let mut evt = EgEvent::new("ITEM_RENTAL_FEE_REQUIRED");
            evt.set_payload(self.copy().clone());
            self.add_event(evt)
        }

        Ok(())
    }

    fn check_captured_hold(&mut self) -> EgResult<()> {
        if self.copy()["status"].id()? != C::COPY_STATUS_ON_HOLDS_SHELF {
            return Ok(());
        }

        let query = eg::hash! {
            "current_copy": self.copy_id,
            "capture_time": {"!=": eg::NULL },
            "cancel_time": eg::NULL,
            "fulfillment_time": eg::NULL
        };

        let flesh = eg::hash! {
            "limit": 1,
            "flesh": 1,
            "flesh_fields": {"ahr": ["usr"]}
        };

        let hold = match self.editor().search_with_ops("ahr", query, flesh)?.pop() {
            Some(h) => h,
            None => return Ok(()),
        };

        if hold["usr"].id()? == self.patron_id {
            self.checkout_is_for_hold = Some(hold);
            return Ok(());
        }

        log::info!("{self} item is on holds shelf for another patron");

        // NOTE this is what the Perl does, but ideally patron display
        // info is collected via the patron ID, not this bit of name logic.
        let fname = hold["usr"]["first_given_name"].string()?;
        let lname = hold["usr"]["family_name"].string()?;
        let pid = hold["usr"]["id"].int()?;
        let hid = hold["id"].int()?;

        let payload = eg::hash! {
            "patron_name": format!("{fname} {lname}"),
            "patron_id": pid,
            "hold_id": hid,
        };

        let mut evt = EgEvent::new("ITEM_ON_HOLDS_SHELF");
        evt.set_payload(payload);
        self.add_event(evt);

        self.hold_found_for_alt_patron = Some(hold);

        Ok(())
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
        let query = eg::hash! {
            "target_copy": self.copy_id,
            "stop_fines": "CLAIMSRETURNED",
            "checkin_time": eg::NULL,
        };

        let mut circ = match self.editor().search("circ", query)?.pop() {
            Some(c) => c,
            None => return Ok(()),
        };

        if !self.can_override_event("CIRC_CLAIMS_RETURNED") {
            return self.exit_err_on_event_code("CIRC_CLAIMS_RETURNED");
        }

        circ["checkin_time"] = EgValue::from("now");
        circ["checkin_scan_time"] = EgValue::from("now");
        circ["checkin_lib"] = EgValue::from(self.circ_lib);
        circ["checkin_staff"] = EgValue::from(self.requestor_id()?);

        if let Some(id) = self.editor().requestor_ws_id() {
            circ["checkin_workstation"] = EgValue::from(id);
        }

        self.editor().update(circ).map(|_| ())
    }

    fn check_for_open_circ(&mut self) -> EgResult<()> {
        if self.is_renewal() {
            return Ok(());
        }

        let query = eg::hash! {
            "target_copy":  self.copy_id,
            "checkin_time": eg::NULL,
        };

        let circ = match self.editor().search("circ", query)?.pop() {
            Some(c) => c,
            None => return Ok(()),
        };

        let mut payload = eg::hash! {"copy": self.copy().clone()};

        if self.patron_id == circ["usr"].int()? {
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
                    payload["auto_renew"] = EgValue::from(1);
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

        // We check permit test results before verifying we have a copy,
        // because we need the results for noncat/precat checkouts.
        let copy_id = if self.copy.is_none()
            || self.is_noncat
            || (self.precat_requested() && !self.is_override && !self.is_renewal())
        {
            eg::NULL
        } else {
            EgValue::from(self.copy_id)
        };

        let query = eg::hash! {
            "from": [
                func,
                self.circ_lib,
                copy_id,
                self.patron_id
            ]
        };

        let results = self.editor().json_query(query)?;

        log::debug!("{self} {func} returned: {:?}", results);

        if results.len() == 0 {
            return self.exit_err_on_event_code("NO_POLICY_MATCHPOINT");
        };

        // Pull the policy data from the first one, which will be the
        // success data if we have any.

        let policy = &results[0];

        self.circ_test_success = policy["success"].boolish();

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
            .editor()
            .retrieve("crcd", policy["duration_rule"].clone())?
            .ok_or_else(err)?;

        let mut recurring_fine_rule = self
            .editor()
            .retrieve("crrf", policy["recurring_fine_rule"].clone())?
            .ok_or_else(err)?;

        let max_fine_rule = self
            .editor()
            .retrieve("crmf", policy["max_fine_rule"].clone())?
            .ok_or_else(err)?;

        // optional
        let hard_due_date = self
            .editor()
            .retrieve("chdd", policy["hard_due_date"].clone())?;

        if let Ok(n) = policy["renewals"].int() {
            duration_rule["max_renewals"] = EgValue::from(n);
        }

        if let Some(s) = policy["grace_period"].as_str() {
            recurring_fine_rule["grace_period"] = EgValue::from(s);
        }

        let max_fine = self.calc_max_fine(&max_fine_rule)?;
        let copy = self.copy();

        let copy_duration = copy["loan_duration"].int()?;
        let copy_fine_level = copy["fine_level"].int()?;

        let duration = match copy_duration {
            C::CIRC_DURATION_SHORT => duration_rule["shrt"].string()?,
            C::CIRC_DURATION_EXTENDED => duration_rule["extended"].string()?,
            _ => duration_rule["normal"].string()?,
        };

        let recurring_fine = match copy_fine_level {
            C::CIRC_FINE_LEVEL_LOW => recurring_fine_rule["low"].float()?,
            C::CIRC_FINE_LEVEL_HIGH => recurring_fine_rule["high"].float()?,
            _ => recurring_fine_rule["normal"].float()?,
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

    /// Check for patron or item policy blocks and override where possible.
    fn inspect_policy_failures(&mut self) -> EgResult<()> {
        if self.circ_test_success {
            return Ok(());
        }

        let mut policy_results = match self.circ_policy_results.take() {
            Some(p) => p,
            None => Err(format!("Non-success circ policy has no policy data"))?,
        };

        if self.is_noncat || self.precat_requested() {
            // "no_item" failures are OK for non-cat checkouts and
            // when precat is requested.
            policy_results = policy_results
                .into_iter()
                .filter(|r| {
                    if let Some(fp) = r["fail_part"].as_str() {
                        return fp != "no_item";
                    }
                    true
                })
                .collect();
        }

        if self.checkout_is_for_hold.is_some() {
            // If this checkout will fulfill a hold, ignore CIRC blocks
            // and rely instead on the (later-checked) FULFILL blocks.

            let penalty_codes: Vec<&str> = policy_results
                .iter()
                .filter(|r| r["fail_part"].is_string())
                .map(|r| r.as_str().unwrap())
                .collect();

            let query = eg::hash! {
                "name": penalty_codes,
                "block_list": {"like": "%CIRC%"}
            };

            let block_pens = self.editor().search("csp", query)?;
            let block_pen_names: Vec<&str> = block_pens
                .iter()
                .map(|p| p["name"].as_str().unwrap())
                .collect();

            let mut keepers = Vec::new();

            for pr in policy_results.drain(..) {
                let pr_name = pr["fail_part"].as_str().unwrap_or("");
                if !block_pen_names.contains(&pr_name) {
                    keepers.push(pr);
                }
            }

            policy_results = keepers;
        }

        // Map fail_part values to legacy event codes and add the
        // events to our working list.
        for pr in policy_results.iter() {
            let fail_part = match pr["fail_part"].as_str() {
                Some(fp) => fp,
                None => continue,
            };

            // Use the mapped value if we have one or default to
            // using the fail_part as the event code.
            let evt_code = LEGACY_CIRC_EVENT_MAP
                .iter()
                .filter(|(fp, _)| fp == &fail_part)
                .map(|(_, code)| code)
                .next()
                .unwrap_or(&fail_part);

            self.add_event_code(evt_code);
        }

        self.circ_policy_results = Some(policy_results);

        Ok(())
    }

    fn calc_max_fine(&mut self, max_fine_rule: &EgValue) -> EgResult<f64> {
        let rule_amount = max_fine_rule["amount"].float()?;

        let copy_id = self.copy_id;

        if max_fine_rule["is_percent"].boolish() {
            let copy_price = billing::get_copy_price(self.editor(), copy_id)?;
            return Ok((copy_price * rule_amount) / 100.0);
        }

        if self
            .settings
            .get_value("circ.max_fine.cap_at_price")?
            .boolish()
        {
            let copy_price = billing::get_copy_price(self.editor(), copy_id)?;
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
        let mut circ = eg::hash! {
            "target_copy": self.copy_id,
            "usr": self.patron_id,
            "circ_lib": self.circ_lib,
            "circ_staff": self.requestor_id()?,
        };

        if let Some(ws) = self.editor().requestor_ws_id() {
            circ["workstation"] = EgValue::from(ws);
        };

        if let Some(ct) = self.options.get("checkout_time") {
            circ["xact_start"] = ct.clone();
        }

        if let Some(id) = self.parent_circ {
            circ["parent_circ"] = EgValue::from(id);
        }

        if self.is_renewal() {
            if self
                .options
                .get("opac_renewal")
                .unwrap_or(&eg::NULL)
                .boolish()
            {
                circ["opac_renewal"] = EgValue::from("t");
            }
            if self
                .options
                .get("phone_renewal")
                .unwrap_or(&eg::NULL)
                .boolish()
            {
                circ["phone_renewal"] = EgValue::from("t");
            }
            if self
                .options
                .get("desk_renewal")
                .unwrap_or(&eg::NULL)
                .boolish()
            {
                circ["desk_renewal"] = EgValue::from("t");
            }
            if self
                .options
                .get("auto_renewal")
                .unwrap_or(&eg::NULL)
                .boolish()
            {
                circ["auto_renewal"] = EgValue::from("t");
            }

            circ["renewal_remaining"] = EgValue::from(self.renewal_remaining);
            circ["auto_renewal_remaining"] = EgValue::from(self.auto_renewal_remaining);
        }

        if self.circ_policy_unlimited {
            circ["duration_rule"] = EgValue::from(C::CIRC_POLICY_UNLIMITED);
            circ["recurring_fine_rule"] = EgValue::from(C::CIRC_POLICY_UNLIMITED);
            circ["max_fine_rule"] = EgValue::from(C::CIRC_POLICY_UNLIMITED);
            circ["renewal_remaining"] = EgValue::from(0);
            circ["grace_period"] = EgValue::from(0);
        } else if let Some(policy) = self.circ_policy_rules.as_ref() {
            circ["duration"] = EgValue::from(policy.duration.to_string());
            circ["duration_rule"] = policy.duration_rule["name"].clone();

            circ["recurring_fine"] = EgValue::from(policy.recurring_fine);
            circ["recurring_fine_rule"] = policy.recurring_fine_rule["name"].clone();
            circ["fine_interval"] = policy.recurring_fine_rule["recurrence_interval"].clone();

            circ["max_fine"] = EgValue::from(policy.max_fine);
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
        let due_val = match self.options.get("due_date") {
            Some(d) => d.clone(),
            None => return Ok(false),
        };

        let circ_lib = self.circ_lib;

        if !self
            .editor()
            .allowed_at("CIRC_OVERRIDE_DUE_DATE", circ_lib)?
        {
            return Err(self.editor().die_event());
        }

        self.circ.as_mut().unwrap()["due_date"] = due_val;

        return Ok(true);
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
            let force = hdd["forceto"].boolish();

            if cdate > date::now() && (cdate < due_date || force) {
                due_date = cdate;
            }
        }

        self.circ.as_mut().unwrap()["due_date"] = EgValue::from(date::to_iso(&due_date));

        Ok(())
    }

    /// Check for booking conflicts and shorten the due date if we need
    /// to apply some elbow room.
    fn apply_booking_due_date(&mut self, is_manual: bool) -> EgResult<bool> {
        if !self.is_booking_enabled() {
            return Ok(false);
        }

        let due_date = match self.circ.as_ref().unwrap()["due_date"].as_str() {
            Some(s) => s.to_string(),
            None => return Ok(false),
        };

        let query = eg::hash! {"barcode": self.copy()["barcode"].clone()};
        let flesh = eg::hash! {"flesh": 1, "flesh_fields": {"brsrc": ["type"]}};

        let resource = match self.editor().search_with_ops("brsrc", query, flesh)?.pop() {
            Some(r) => r,
            None => return Ok(false),
        };

        let stop_circ = self
            .settings
            .get_value("circ.booking_reservation.stop_circ")?
            .boolish();

        let query = eg::hash! {
            "resource": resource["id"].clone(),
            "search_start": "now",
            "search_end": due_date.as_str(),
            "fields": {
                "cancel_time": eg::NULL,
                "return_time": eg::NULL,
            }
        };

        let booking_ids_op = self.editor().client_mut().send_recv_one(
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
        let due_date_dt = date::parse_datetime(&due_date)?;
        let now_dt = date::now();
        let mut bookings = Vec::new();

        // First see if we need to block the circulation due to
        // reservation overlap / stop-circ setting.
        for id in booking_ids.members() {
            let booking = self
                .editor()
                .retrieve("bresv", id.clone())?
                .ok_or_else(|| self.editor().die_event())?;

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
        circ["duration"] = EgValue::from(format!("{duration} seconds"));
        circ["due_date"] = EgValue::from(date::to_iso(&due_date_dt));

        // Changes were made.
        Ok(true)
    }

    /// Extend the circ due date to avoid org unit closures.
    fn extend_due_date(&mut self, _shift_to_start: bool) -> EgResult<()> {
        if self.is_renewal() {
            self.extend_renewal_due_date()?;
        }

        let due_date_str = match self.circ.as_ref().unwrap()["due_date"].as_str() {
            Some(s) => s,
            None => return Ok(()),
        };

        let due_date_dt = date::parse_datetime(due_date_str)?;

        let circ_lib = self.circ_lib;
        let org_open_data = org::next_open_date(self.editor(), circ_lib, &due_date_dt)?;

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

        self.circ.as_mut().unwrap()["due_date"] = EgValue::from(date::to_iso(&due_date_dt));

        Ok(())
    }

    /// Optionally extend the due date of a renewal if time was
    /// lost on renewing early.
    fn extend_renewal_due_date(&mut self) -> EgResult<()> {
        let policy = match self.circ_policy_rules.as_ref() {
            Some(p) => p,
            None => return Ok(()),
        };

        // Intervals can in theory be numeric; coerce to string result.
        let renew_extend_min_res = policy.matchpoint["renew_extend_min_interval"].string();

        if !policy.matchpoint["renew_extends_due_date"].boolish() {
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

        let prev_circ = match self.editor().retrieve("circ", EgValue::from(parent_circ))? {
            Some(c) => c,
            None => return Err(self.editor().die_event()),
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
        if let Ok(intvl) = renew_extend_min_res {
            let min_duration = date::interval_to_seconds(&intvl)?;
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

                self.circ.as_mut().unwrap()["due_date"] = EgValue::from(date::to_iso(&due));

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

        self.circ.as_mut().unwrap()["due_date"] = EgValue::from(date::to_iso(&due));

        Ok(())
    }

    fn save_checkout_circ(&mut self) -> EgResult<()> {
        // At this point we know we have a circ.
        // Turn our circ hash into an IDL-classed object.
        let circ = self.circ.as_ref().unwrap().clone();
        let clone = EgValue::create("circ", circ)?;

        log::debug!("{self} creating circulation {}", clone.dump());

        // Put it in the DB
        self.circ = Some(self.editor().create(clone)?);

        // We did it. We checked out a copy.
        self.update_copy(eg::hash! {"status": C::COPY_STATUS_CHECKED_OUT})?;

        Ok(())
    }

    fn apply_limit_groups(&mut self) -> EgResult<()> {
        let limit_groups = match self.circ_policy_rules.as_ref() {
            Some(p) => match p.limit_groups.as_ref() {
                Some(g) => g,
                None => return Ok(()),
            },
            None => return Ok(()),
        };

        let query = eg::hash! {
            "from": [
                "action.link_circ_limit_groups",
                self.circ.as_ref().unwrap()["id"].clone(),
                limit_groups.clone()
            ]
        };

        self.editor().json_query(query)?;

        Ok(())
    }

    fn is_deposit(&self) -> bool {
        if let Some(copy) = self.copy.as_ref() {
            if let Some(amount) = copy["deposit_amount"].as_f64() {
                return amount > 0.0 && copy["deposit"].boolish();
            }
        }
        false
    }

    // True if we have a deposit_amount but the desposit flag is false.
    fn is_rental(&self) -> bool {
        if let Some(copy) = self.copy.as_ref() {
            if let Some(amount) = copy["deposit_amount"].as_f64() {
                return amount > 0.0 && !copy["deposit"].boolish();
            }
        }
        false
    }

    fn apply_deposit_fee(&mut self) -> EgResult<()> {
        let is_deposit = self.is_deposit();
        let is_rental = self.is_rental();

        if !is_deposit && !is_rental {
            return Ok(());
        }

        // confirmed above
        let deposit_amount = self.copy()["deposit_amount"].as_f64().unwrap();

        let skip_deposit_fee = self.settings.get_value("skip_deposit_fee")?.boolish();
        if is_deposit && (skip_deposit_fee || self.is_deposit_exempt()?) {
            return Ok(());
        }

        let skip_rental_fee = self.settings.get_value("skip_rental_fee")?.boolish();
        if is_rental && (skip_rental_fee | self.is_rental_exempt()?) {
            return Ok(());
        }

        let mut btype = C::BTYPE_DEPOSIT;
        let mut btype_label = C::BTYPE_LABEL_DEPOSIT;

        if is_rental {
            btype = C::BTYPE_RENTAL;
            btype_label = C::BTYPE_LABEL_RENTAL;
        }

        let circ_id = self.circ.as_ref().expect("Circ is Set").id()?;

        let bill = billing::create_bill(
            self.editor(),
            deposit_amount,
            btype,
            btype_label,
            circ_id,
            Some(C::BTYPE_NOTE_SYSTEM),
            None,
            None,
        )?;

        if is_deposit {
            self.deposit_billing = Some(bill);
        } else {
            self.rental_billing = Some(bill);
        }

        Ok(())
    }

    fn is_deposit_exempt(&mut self) -> EgResult<bool> {
        let profile = self.patron.as_ref().unwrap()["profile"].id()?;

        let groups = self.settings.get_value("circ.deposit.exempt_groups")?;

        if !groups.is_array() || groups.len() == 0 {
            return Ok(false);
        }

        let mut parent_ids = Vec::new();
        for grp in groups.members() {
            parent_ids.push(grp.id()?);
        }

        self.is_group_descendant(profile, parent_ids.as_slice())
    }

    fn is_rental_exempt(&mut self) -> EgResult<bool> {
        let profile = self.patron.as_ref().unwrap()["profile"].id()?;

        let groups = self.settings.get_value("circ.rental.exempt_groups")?;

        if !groups.is_array() || groups.len() == 0 {
            return Ok(false);
        }

        let mut parent_ids = Vec::new();
        for grp in groups.members() {
            parent_ids.push(grp.id()?);
        }

        self.is_group_descendant(profile, parent_ids.as_slice())
    }

    /// Returns true if the child is a descendant of any of the parent
    /// profile group IDs
    fn is_group_descendant(&mut self, child_id: i64, parent_ids: &[i64]) -> EgResult<bool> {
        let query = eg::hash! {"from": ["permission.grp_ancestors", child_id] };
        let ancestors = self.editor().json_query(query)?;
        for parent_id in parent_ids {
            for grp in &ancestors {
                if grp.id()? == *parent_id {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// See if we can fulfill a hold for this patron with this
    /// checked out item.
    fn handle_checkout_holds(&mut self) -> EgResult<()> {
        if self.is_noncat {
            return Ok(());
        }

        // checkout_is_for_hold will contain the hold we already know
        // to be on the holds shelf for our patron + item.
        let mut maybe_hold = self.checkout_is_for_hold.take();

        if maybe_hold.is_none() {
            maybe_hold = self.handle_targeted_hold()?;
        }

        if maybe_hold.is_none() {
            maybe_hold = self.find_related_user_hold()?;
        }

        let mut hold = match maybe_hold.take() {
            Some(h) => h,
            None => return Ok(()),
        };

        self.check_hold_fulfill_blocks()?;

        let hold_id = hold.id()?;

        log::info!("{self} fulfilling hold {hold_id}");

        hold["hopeless_date"].take();
        hold["current_copy"] = EgValue::from(self.copy_id);
        hold["fulfillment_time"] = EgValue::from("now");
        hold["fulfillment_staff"] = EgValue::from(self.requestor_id()?);
        hold["fulfillment_lib"] = EgValue::from(self.circ_lib);

        if hold["capture_time"].is_null() {
            hold["capture_time"] = EgValue::from("now");
        }

        self.editor().create(hold)?;

        self.fulfilled_hold_ids = Some(vec![hold_id]);

        Ok(())
    }

    /// If we have a hold that targets another patron -- we have already
    /// overridden that event -- then reset the hold so it can go on
    /// to target a different copy.
    fn handle_targeted_hold(&mut self) -> EgResult<Option<EgValue>> {
        let mut hold = match self.hold_found_for_alt_patron.take() {
            Some(h) => h,
            None => return Ok(None),
        };

        hold["clear_prev_check_time"].take();
        hold["clear_current_copy"].take();
        hold["clear_capture_time"].take();
        hold["clear_shelf_time"].take();
        hold["clear_shelf_expire_time"].take();
        hold["clear_current_shelf_lib"].take();

        log::info!(
            "{self} un-targeting hold {} because our copy is checking out",
            hold["id"],
        );

        self.editor().update(hold).map(|_| None)
    }

    /// Find a similar hold to fulfill.
    ///
    /// If the circ.checkout_fill_related_hold setting is turned on
    /// and no hold for the patron directly targets the checked out
    /// item, see if there is another hold for the patron that could be
    /// fulfilled by the checked out item.  Fulfill the oldest hold and
    /// only fulfill 1 of them.
    ///
    /// First, check for one that the copy matches via hold_copy_map,
    /// ensuring that *any* hold type that this copy could fill may end
    /// up filled.
    ///
    /// Then, if circ.checkout_fill_related_hold_exact_match_only is not
    /// enabled, look for a Title (T) or Volume (V) hold that matches
    /// the item. This allows items that are non-requestable to count as
    /// capturing those hold types.
    /// ------------------------------------------------------------------------------
    fn find_related_user_hold(&mut self) -> EgResult<Option<EgValue>> {
        if self.is_precat_copy() {
            return Ok(None);
        }

        if !self
            .settings
            .get_value("circ.checkout_fills_related_hold")?
            .boolish()
        {
            return Ok(None);
        }

        let copy_id = self.copy_id;
        let patron_id = self.patron_id;

        // find the oldest unfulfilled hold that has not yet hit the holds shelf.
        let query = eg::hash! {
            "select": {"ahr": ["id"]},
            "from": {
                "ahr": {
                    "ahcm": {
                        "field": "hold",
                        "fkey": "id"
                    },
                    "acp": {
                        "field": "id",
                        "fkey": "current_copy",
                        "type": "left" // there may be no current_copy
                    }
                }
            },
            "where": {
                "+ahr": {
                    "usr": patron_id,
                    "fulfillment_time": eg::NULL,
                    "cancel_time": eg::NULL,
                   "-or": [
                        {"expire_time": eg::NULL},
                        {"expire_time": {">": "now"}}
                    ]
                },
                "+ahcm": {
                    "target_copy": copy_id,
                },
                "+acp": {
                    "-or": [
                        {"id": eg::NULL}, // left-join copy may be nonexistent
                        {"status": {"!=": C::COPY_STATUS_ON_HOLDS_SHELF}},
                    ]
                }
            },
            "order_by": {"ahr": {"request_time": {"direction": "asc"}}},
            "limit": 1
        };

        if let Some(hold) = self.editor().json_query(query)?.pop() {
            return self.editor().retrieve("ahr", hold["id"].clone());
        }

        if self
            .settings
            .get_value("circ.checkout_fills_related_hold_exact_match_only")?
            .boolish()
        {
            // We only want exact matches and didn't find any.  We're done.
            return Ok(None);
        }

        // Expand our search to more hold types that could be filled
        // by our checked out copy.

        let circ_lib = self.circ_lib;
        let patron_id = self.patron_id;
        let copy_id = self.copy_id;

        let hold_data = holds::related_to_copy(
            self.editor(),
            copy_id,
            Some(circ_lib),
            None, // frozen
            Some(patron_id),
            Some(false), // already on holds shelf
        )?;

        if hold_data.len() == 0 {
            return Ok(None);
        }

        // holds::related_to_copy may return holds that patron does not
        // want filled by this copy, e.g. holds that target different
        // volumes or records.  Apply some additional filtering.

        let record_id = self.copy()["call_number"]["record"].int()?;
        let volume_id = self.copy()["call_number"].id()?;

        for hold in hold_data.iter() {
            let target = hold.target();

            // The Perl only supports T and V holds.  Matching that for now.

            if hold.hold_type() == holds::HoldType::Title && target == record_id {
                return self.editor().retrieve("ahr", hold.id());
            }

            if hold.hold_type() == holds::HoldType::Volume && target == volume_id {
                return self.editor().retrieve("ahr", hold.id());
            }
        }

        Ok(None)
    }

    /// Exits with error if hold blocks are present and we are not
    /// overriding them.
    fn check_hold_fulfill_blocks(&mut self) -> EgResult<()> {
        let home_ou = self.patron.as_ref().unwrap()["home_ou"].int()?;
        let copy_ou = self.copy()["circ_lib"].int()?;

        let copy_prox;
        let circ_lib = self.circ_lib;
        let ou_prox = org::proximity(self.editor(), home_ou, circ_lib)?.unwrap_or(-1);

        if copy_ou == circ_lib {
            copy_prox = ou_prox;
        } else {
            copy_prox = org::proximity(self.editor(), copy_ou, circ_lib)?.unwrap_or(-1);
        }

        let query = eg::hash! {
            "select": {"csp": ["name", "label"]},
            "from": {"ausp": "csp"},
            "where": {
                "+ausp": {
                    "usr": self.patron_id,
                    "org_unit": org::full_path(self.editor(), circ_lib, None)?,
                    "-or": [
                        {"stop_date": eg::NULL},
                        {"stop_date": {">": "now"}}
                    ]
                },
                "+csp": {
                    "block_list": {"like": "%FULFILL%"},
                    "-or": [
                        {"ignore_proximity": eg::NULL},
                        {"ignore_proximity": {"<": ou_prox}},
                        {"ignore_proximity": {"<": copy_prox}}
                    ]
                }
            }
        };

        let penalties = self.editor().json_query(query)?;
        for pen in penalties {
            let mut evt = EgEvent::new(pen["name"].as_str().unwrap());
            if let Some(d) = pen["label"].as_str() {
                evt.set_desc(d);
            }
            self.add_event(evt);
        }

        self.try_override_events()
    }

    fn build_checkout_response(&mut self) -> EgResult<()> {
        let mut record = None;
        if !self.is_precat_copy() {
            let record_id = self.copy()["call_number"]["record"].int()?;
            record = Some(bib::map_to_mvr(self.editor(), record_id)?);
        }

        let mut copy = self.copy().clone();
        let volume = copy["call_number"].take();
        copy.deflesh()?;

        let circ = self.circ.as_ref().unwrap().clone();
        let patron = self.patron.as_ref().unwrap().clone();
        let patron_id = self.patron_id;

        let patron_money = self.editor().retrieve("mus", patron_id)?;

        let mut payload = eg::hash! {
            "copy": copy,
            "volume": volume,
            "record": record,
            "circ": circ,
            "patron": patron,
            "patron_money": patron_money,
        };

        if let Some(list) = self.fulfilled_hold_ids.as_ref() {
            payload["holds_fulfilled"] = EgValue::from(list.clone());
        }

        if let Some(bill) = self.deposit_billing.as_ref() {
            payload["deposit_billing"] = bill.clone();
        }

        if let Some(bill) = self.rental_billing.as_ref() {
            payload["rental_billing"] = bill.clone();
        }

        // Flesh the billing summary for our checked-in circ.
        if let Some(pcirc) = self.parent_circ {
            let flesh = eg::hash! {
                "flesh": 1,
                "flesh_fields": {
                    "circ": ["billable_transaction"],
                    "mbt": ["summary"],
                }
            };

            if let Some(circ) = self.editor().retrieve_with_ops("circ", pcirc, flesh)? {
                payload["parent_circ"] = circ;
            }
        }

        let mut evt = EgEvent::success();
        evt.set_payload(payload);
        self.add_event(evt);

        Ok(())
    }
}
