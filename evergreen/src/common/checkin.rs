use crate::common::billing;
use crate::common::circulator::{CircOp, Circulator};
use crate::common::holds;
use crate::constants as C;
use crate::date;
use crate::error::{EgError, EgResult};
use crate::event::EgEvent;
use crate::util::{json_bool, json_bool_op, json_float, json_int, json_string};
use chrono::{Duration, Local, Timelike};
use json::JsonValue;
use std::collections::HashSet;

const CHECKIN_ORG_SETTINGS: &[&str] = &[
    "circ.transit.min_checkin_interval",
    "circ.transit.suppress_hold",
];

/// Checkin
impl Circulator {
    pub fn checkin(&mut self) -> EgResult<()> {
        self.circ_op = CircOp::Checkin;

        // Pre-cache some setting values.
        self.settings.fetch_values(CHECKIN_ORG_SETTINGS)?;

        self.basic_copy_checks()?;
        self.fix_broken_transit_status()?;
        self.check_transit_checkin_interval()?;
        self.checkin_retarget_holds()?;
        self.cancel_transit_if_circ_exists()?;
        self.set_dont_change_lost_zero()?;
        self.set_can_float()?;
        self.do_inventory_update()?;

        if self.check_is_on_holds_shelf()? {
            // Item is resting cozily on the holds shelf. Leave it be.
            return Ok(());
        }

        self.load_system_copy_alerts()?;
        self.load_runtime_copy_alerts()?;
        self.check_copy_alerts()?;

        self.check_claims_returned();
        self.check_circ_deposit(false)?;
        self.try_override_events()?;

        if self.exit_early {
            return Ok(());
        }

        if self.circ.is_some() {
            self.checkin_handle_circ()?;
        } else if self.transit.is_some() {
            self.checkin_handle_transit()?;
            self.checkin_handle_received_hold()?;
        } else if self.copy_status() == C::COPY_STATUS_IN_TRANSIT {
            log::warn!("{self} copy is in-transit but there is no transit");
            self.reshelve_copy(true)?;
        }

        if self.exit_early {
            return Ok(());
        }

        if self.circ_op == CircOp::Renew {
            //self.finish_fines_and_voiding()?;
            self.add_event_code("SUCCESS");
            return Ok(());
        }

        // Circulations and transits are now closed where necessary.
        // Now see if this copy can fulfill a hold or needs to be
        // routed to a different location.

        let mut item_is_needed = false;
        if self.get_option_bool("noop") {
            if self.get_option_bool("can_float") {
                // As noted in the Perl, it's maybe unexpected that
                // floating items are modified during NO-OP checkins,
                // but the behavior is retained for backwards compat.
                self.update_copy(json::object! {"circ_lib": self.circ_lib})?;
            }
        } else {
            item_is_needed = self.try_to_capture()?;
        }

        Ok(())
    }

    fn basic_copy_checks(&mut self) -> EgResult<()> {
        if self.copy.is_none() {
            self.exit_err_on_event_code("ASSET_COPY_NOT_FOUND")?;
        }

        if json_bool(&self.copy()["deleted"]) {
            // Never attempt to capture holds with a deleted copy.
            self.options
                .insert(String::from("capture"), json::from("nocapture"));
        }

        Ok(())
    }

    /// Load the open transit and make sure our copy is in the right
    /// status if there's a matching transit.
    fn fix_broken_transit_status(&mut self) -> EgResult<()> {
        let query = json::object! {
            target_copy: self.copy()["id"].clone(),
            dest_recv_time: JsonValue::Null,
            cancel_time: JsonValue::Null,
        };

        let results = self.editor.search("atc", query)?;

        let transit = match results.first() {
            Some(t) => t,
            None => return Ok(()),
        };

        if self.copy_status() != C::COPY_STATUS_IN_TRANSIT {
            log::warn!("{self} Copy has an open transit, but incorrect status");
            let changes = json::object! {status: C::COPY_STATUS_IN_TRANSIT};
            self.update_copy(changes)?;
        }

        self.transit = Some(transit.to_owned());

        Ok(())
    }

    /// If a copy goes into transit and is then checked in before the
    /// transit checkin interval has expired, push an event onto the
    /// overridable events list.
    fn check_transit_checkin_interval(&mut self) -> EgResult<()> {
        if self.copy_status() != C::COPY_STATUS_IN_TRANSIT {
            // We only care about in-transit items.
            return Ok(());
        }

        let interval = self
            .settings
            .get_value("circ.transit.min_checkin_interval")?;

        if interval.is_null() {
            // No checkin interval defined.
            return Ok(());
        }

        let transit = match self.transit.as_ref() {
            Some(t) => t,
            None => {
                log::warn!("Copy has in-transit status but no matching transit!");
                return Ok(());
            }
        };

        if transit["source"] == transit["dest"] {
            // Checkin interval does not apply to transits that aren't
            // actually going anywhere.
            return Ok(());
        }

        // Coerce the interval into a string just in case it arrived as a number.
        let interval = json_string(&interval)?;

        let seconds = date::interval_to_seconds(&interval)?;
        // source_send_time is a known non-null string value.
        let send_time_str = transit["source_send_time"].as_str().unwrap();

        let send_time = date::parse_datetime(send_time_str)?;
        let horizon = send_time + Duration::seconds(seconds);

        if horizon > Local::now() {
            self.add_event_code("TRANSIT_CHECKIN_INTERVAL_BLOCK");
        }

        Ok(())
    }

    /// Retarget local holds that might wish to use our copy as
    /// a target.  Useful if the copy is going from a non-holdable
    /// to a holdable status and the hold targeter may not run
    /// until, say, overnight.
    fn checkin_retarget_holds(&mut self) -> EgResult<()> {
        let retarget_mode = match self.options.get("retarget_mode") {
            Some(r) => match r.as_str() {
                Some(s) => s,
                None => "",
            },
            None => "",
        };

        if !retarget_mode.contains("retarget") {
            return Ok(());
        }

        let capture = match self.options.get("capture") {
            Some(c) => match c.as_str() {
                Some(s) => s,
                None => "",
            },
            None => "",
        };

        if capture.eq("nocapture") {
            return Ok(());
        }

        let copy = self.copy();
        let copy_id = self.copy_id.unwrap();

        let is_precat =
            json_bool_op(self.options.get("is_precat")) || json_int(&copy["call_number"])? == -1;

        if is_precat {
            return Ok(());
        }

        if json_int(&copy["circ_lib"])? != self.circ_lib {
            // We only care about "our" copies.
            return Ok(());
        }

        if !json_bool(&copy["holdable"]) {
            return Ok(());
        }

        if json_bool(&copy["deleted"]) {
            return Ok(());
        }

        if !json_bool(&copy["status"]["holdable"]) {
            return Ok(());
        }

        if !json_bool(&copy["location"]["holdable"]) {
            return Ok(());
        }

        // By default, we only care about in-process items.
        if !retarget_mode.contains(".all") && self.copy_status() != C::COPY_STATUS_IN_PROCESS {
            return Ok(());
        }

        let query = json::object! {target_copy: json::from(copy_id)};
        let parts = self.editor.search("acpm", query)?;
        let parts = parts
            .into_iter()
            .map(|p| json_int(&p["id"]).unwrap())
            .collect::<HashSet<_>>();

        // Get the list of potentially retargetable holds
        // TODO reporter.hold_request_record is not currently updated
        // when items/call numbers are transferred to another call
        // number / record.
        let query = json::object! {
            select: {
                ahr: [
                    "id",
                    "target",
                    "hold_type",
                    "cut_in_line",
                    "request_time",
                    "selection_depth"
                ],
                pgt: ["hold_priority"]
            },
            from: {
                ahr: {
                    rhrr: {},
                    au: {
                        pgt: {}
                    }
                }
            },
            where: {
               fulfillment_time: JsonValue::Null,
               cancel_time: JsonValue::Null,
               frozen: "f",
               pickup_lib: self.circ_lib,
            },
            order_by: [
                {class: "pgt", field: "hold_priority"},
                {class: "ahr", field: "cut_in_line",
                    direction: "desc", transform: "coalesce", params: vec!["f"]},
                {class: "ahr", field: "selection_depth", direction: "desc"},
                {class: "ahr", field: "request_time"}
            ]
        };

        let hold_data = self.editor.json_query(query)?;
        for hold in hold_data.iter() {
            let target = json_int(&hold["target"])?;
            let hold_type = hold["hold_type"].as_str().unwrap();

            // Copy-level hold that points to a different copy.
            if hold_type.eq("C") || hold_type.eq("R") || hold_type.eq("F") {
                if target != copy_id {
                    continue;
                }
            }

            // Volume-level hold for a different volume
            if hold_type.eq("V") {
                if target != json_int(&self.copy()["call_number"]["id"])? {
                    continue;
                }
            }

            if parts.len() > 0 {
                // We have parts
                if hold_type.eq("T") {
                    continue;
                } else if hold_type.eq("P") {
                    // Skip part holds for parts that are related to our copy
                    if !parts.contains(&target) {
                        continue;
                    }
                }
            } else if hold_type.eq("P") {
                // We have no parts, skip part-type holds
                continue;
            }

            // We've ruled out a lot of basic scenarios.  Now ask the
            // hold targeter to take over.
            let query = json::object! {
                hold: hold["id"].clone(),
                find_copy: copy_id,
            };

            let results = self.editor.client_mut().send_recv_one(
                "open-ils.hold-targeter",
                "open-ils.hold-targeter.target",
                query,
            )?;

            if let Some(result) = results {
                if json_bool(&result["found_copy"]) {
                    log::info!("checkin_retarget_holds() successfully targeted a hold");
                    break;
                }
            }
        }

        return Ok(());
    }

    /// If have both an open circulation and an open transit,
    /// cancel the transit.
    fn cancel_transit_if_circ_exists(&mut self) -> EgResult<()> {
        if self.circ.is_none() {
            return Ok(());
        }

        if let Some(transit) = self.transit.as_ref() {
            log::info!("{self} copy is both checked out and in transit.  Canceling transit");

            // TODO once transit.abort is migrated to Rust, this call should
            // happen within the same transaction.
            self.editor.client_mut().send_recv_one(
                "open-ils.circ",
                "open-ils.circ.transit.abort",
                json::object! {transitid: transit["id"].clone()},
            )?;

            self.transit = None;
        }

        Ok(())
    }

    /// Decides if we need to avoid certain LOST / LO processing for
    /// transactions that have a zero balance.
    fn set_dont_change_lost_zero(&mut self) -> EgResult<()> {
        match self.copy_status() {
            C::COPY_STATUS_LOST | C::COPY_STATUS_LOST_AND_PAID | C::COPY_STATUS_LONG_OVERDUE => {
                // Found a copy me may want to work on,
            }
            _ => return Ok(()), // copy is not relevant
        }

        // LOST fine settings are controlled by the copy's circ lib,
        // not the circulation's
        let value = self.settings.get_value_at_org(
            "circ.checkin.lost_zero_balance.do_not_change",
            json_int(&self.copy()["circ_lib"])?,
        )?;

        let mut dont_change = json_bool(&value);

        if dont_change {
            // Org setting says not to change.
            // Make sure no balance is owed, or the setting is meaningless.

            if let Some(circ) = self.circ.as_ref() {
                if let Some(mbts) = self.editor.retrieve("mbts", circ["id"].clone())? {
                    dont_change = json_float(&mbts["balance_owed"])? == 0.0;
                }
            }
        }

        if dont_change {
            self.set_option_true("dont_change_lost_zero");
        }

        Ok(())
    }

    /// Determines of our copy is eligible for floating.
    fn set_can_float(&mut self) -> EgResult<()> {
        let float_id = &self.copy()["floating"];

        if float_id.is_null() {
            // Copy is not configured to float
            return Ok(());
        }

        // Copy can float.  Can it float here?

        let float_group = self.editor.retrieve("cfg", float_id.clone())?.unwrap(); // foreign key

        let query = json::object! {
            from: [
                "evergreen.can_float",
                float_group["id"].clone(),
                self.copy()["circ_lib"].clone(),
                self.circ_lib
            ]
        };

        if let Some(resp) = self.editor.json_query(query)?.first() {
            if json_bool(&resp["evergreen.can_float"]) {
                self.set_option_true("can_float");
            }
        }

        Ok(())
    }

    fn do_inventory_update(&mut self) -> EgResult<()> {
        if !self.get_option_bool("do_inventory_update") {
            return Ok(());
        }

        if json_int(&self.copy()["circ_lib"])? != self.circ_lib
            && !self.get_option_bool("can_float")
        {
            // Item is not home and cannot float
            return Ok(());
        }

        // Create a new copy inventory row.
        let aci = json::object! {
            inventory_date: "now",
            inventory_workstation: self.editor.requestor_ws_id(),
            copy: self.copy()["id"].clone(),
        };

        self.editor.create(&aci)?;

        Ok(())
    }

    fn check_is_on_holds_shelf(&mut self) -> EgResult<bool> {
        if self.copy_status() != C::COPY_STATUS_ON_HOLDS_SHELF {
            return Ok(false);
        }

        let copy_id = self.copy_id.unwrap();

        if self.get_option_bool("clear_expired") {
            // Clear shelf-expired holds for this copy.
            // TODO run in the same transaction once ported to Rust.

            let params = json::array![
                self.editor.authtoken(),
                self.circ_lib,
                self.copy()["id"].clone(),
            ];

            self.editor.client_mut().send_recv_one(
                "open-ils.circ",
                "open-ils.circ.hold.clear_shelf.process",
                params,
            )?;
        }

        let hold = match holds::captured_hold_for_copy(&mut self.editor, copy_id)? {
            Some(h) => h,
            None => {
                log::warn!("{self} Copy on holds shelf but there is no hold");
                self.reshelve_copy(false)?;
                return Ok(false);
            }
        };

        let pickup_lib = json_int(&hold["pickup_lib"])?;

        log::info!("{self} we found a captured, un-fulfilled hold");

        if pickup_lib != self.circ_lib && !self.get_option_bool("hold_as_transit") {
            let suppress_here = self.settings.get_value("circ.transit.suppress_hold")?;

            let suppress_here = match json_string(&suppress_here) {
                Ok(s) => s,
                Err(_) => String::from(""),
            };

            let suppress_there = self
                .settings
                .get_value_at_org("circ.transit.suppress_hold", pickup_lib)?;

            let suppress_there = match json_string(&suppress_there) {
                Ok(s) => s,
                Err(_) => String::from(""),
            };

            if suppress_here == suppress_there && suppress_here != "" {
                log::info!("{self} hold is within transit suppress group: {suppress_here}");
                self.set_option_true("fake_hold_dest");
                return Ok(true);
            }
        }

        if pickup_lib == self.circ_lib && !self.get_option_bool("hold_as_transit") {
            log::info!("{self} hold is for here");
            return Ok(true);
        }

        log::info!("{self} hold is not for here");
        self.options.insert(String::from("remote_hold"), hold);

        Ok(false)
    }

    fn reshelve_copy(&mut self, force: bool) -> EgResult<()> {
        let force = force || self.get_option_bool("force");

        let status = self.copy_status();

        let next_status = match self.options.get("next_copy_status") {
            Some(s) => json_int(&s)?,
            None => C::COPY_STATUS_RESHELVING,
        };

        if force
            || (status != C::COPY_STATUS_ON_HOLDS_SHELF
                && status != C::COPY_STATUS_CATALOGING
                && status != C::COPY_STATUS_IN_TRANSIT
                && status != next_status)
        {
            self.update_copy(json::object! {status: json::from(next_status)})?;
        }

        Ok(())
    }

    fn check_claims_returned(&mut self) {
        if let Some(circ) = self.circ.as_ref() {
            if let Some(sf) = circ["stop_fines"].as_str() {
                if sf == "CLAIMSRETURNED" {
                    self.add_event_code("CIRC_CLAIMS_RETURNED");
                }
            }
        }
    }

    fn check_circ_deposit(&mut self, void: bool) -> EgResult<()> {
        let circ_id = match self.circ.as_ref() {
            Some(c) => c["id"].clone(),
            None => return Ok(()),
        };

        let query = json::object! {
            btype: C::BTYPE_DEPOSIT,
            voided: "f",
            xact: circ_id,
        };

        let results = self.editor.search("mb", query)?;
        let deposit = match results.first() {
            Some(d) => d,
            None => return Ok(()),
        };

        if void {
            // Caller suggests we void.  Verify settings allow it.
            if json_bool(self.settings.get_value("circ.void_item_deposit")?) {
                let bill_id = json_int(&deposit["id"])?;
                billing::void_bills(&mut self.editor, &[bill_id], Some("DEPOSIT ITEM RETURNED"))?;
            }
        } else {
            let mut evt = EgEvent::new("ITEM_DEPOSIT_PAID");
            evt.set_payload(deposit.to_owned());
            self.add_event(evt);
        }

        Ok(())
    }

    fn checkin_handle_circ(&mut self) -> EgResult<()> {
        if self.get_option_bool("claims_never_checked_out") {
            let xact_start = &self.circ.as_ref().unwrap()["xact_start"];
            self.options
                .insert("backdate".to_string(), xact_start.clone());
        }

        if self.options.contains_key("backdate") {
            self.checkin_compile_backdate()?;
        }

        let copy_status = self.copy_status();
        let copy_circ_lib = self.copy_circ_lib();

        let circ = self.circ.as_mut().unwrap();
        circ["checkin_time"] = self
            .options
            .get("backdate")
            .map(|bd| bd.clone())
            .unwrap_or(json::from("now"));

        circ["checkin_scan_time"] = json::from("now");
        circ["checkin_staff"] = json::from(self.editor.requestor_id());
        circ["checkin_lib"] = json::from(self.circ_lib);
        circ["checkin_workstation"] = json::from(self.editor.requestor_ws_id());

        match copy_status {
            C::COPY_STATUS_LOST => self.checkin_handle_lost()?,
            C::COPY_STATUS_LOST_AND_PAID => self.checkin_handle_lost()?,
            C::COPY_STATUS_LONG_OVERDUE => self.checkin_handle_long_overdue()?,
            C::COPY_STATUS_MISSING => {
                if copy_circ_lib == self.circ_lib {
                    self.reshelve_copy(true)?
                } else {
                    log::info!("{self} leaving copy in missing status on remote checkin");
                }
            }
            _ => self.reshelve_copy(true)?,
        }

        if !self.get_option_bool("dont_change_lost_zero") {
            // Caller has not requested we leave well enough alone, i.e.
            // if an item was lost and paid, it's eligible to be re-opened
            // for additional billing.

            if self.get_option_bool("claims_never_checked_out") {
                let circ = self.circ.as_mut().unwrap(); // mut borrow conflicts
                circ["stop_fines"] = json::from("CLAIMSNEVERCHECKEDOUT");
            } else if copy_status == C::COPY_STATUS_LOST {
                // Note copy_status refers to the status of the copy
                // before self.checkin_handle_lost() was called.

                if self.get_option_bool("circ.lost.generate_overdue_on_checkin") {
                    // As with Perl, this setting is based on the
                    // runtime circ lib instead of the copy circ lib.

                    // If this circ was LOST and we are configured to
                    // generate overdue fines for lost items on checkin
                    // (to fill the gap between mark lost time and when
                    // the fines would have naturally stopped), then
                    // clear stop_fines so the fine generator can work.
                    let circ = self.circ.as_mut().unwrap(); // mut borrow conflicts
                    circ["stop_fines"] = JsonValue::Null;
                }
            }

            self.handle_checkin_fines()?;
        }

        self.check_circ_deposit(true)?;

        let circ_id = self.circ.as_ref().unwrap()["id"].clone();

        // Set/clear stop_fines as needed.
        billing::check_open_xact(&mut self.editor, json_int(&circ_id)?)?;

        // Get a post-save version of the circ to pick up any in-DB changes.
        if let Some(c) = self.editor.retrieve("circ", circ_id)? {
            self.circ = Some(c);
        }

        Ok(())
    }

    fn checkin_handle_lost(&mut self) -> EgResult<()> {
        log::info!("{self} processing LOST checkin...");

        let billing_options = json::object! {
            ous_void_item_cost: "circ.void_lost_on_checkin",
            ous_void_proc_fee: "circ.void_lost_proc_fee_on_checkin",
            ous_restore_overdue: "circ.restore_overdue_on_lost_return",
            void_cost_btype: C::BTYPE_LOST_MATERIALS,
            void_fee_btype: C::BTYPE_LOST_MATERIALS_PROCESSING_FEE,
        };

        self.options
            .insert("lost_or_lo_billing_options".to_string(), billing_options);

        self.checkin_handle_lost_or_long_overdue(
            "circ.max_accept_return_of_lost",
            "circ.lost_immediately_available",
            None, // ous_use_last_activity not supported for LOST
        )
    }

    fn checkin_handle_long_overdue(&mut self) -> EgResult<()> {
        let billing_options = json::object! {
            is_longoverdue: true,
            ous_void_item_cost: "circ.void_longoverdue_on_checkin",
            ous_void_proc_fee: "circ.void_longoverdue_proc_fee_on_checkin",
            ous_restore_overdue: "circ.restore_overdue_on_longoverdue_return",
            void_cost_btype: C::BTYPE_LONG_OVERDUE_MATERIALS,
            void_fee_btype: C::BTYPE_LONG_OVERDUE_MATERIALS_PROCESSING_FEE,
        };

        self.options
            .insert("lost_or_lo_billing_options".to_string(), billing_options);

        self.checkin_handle_lost_or_long_overdue(
            "circ.max_accept_return_of_longoverdue",
            "circ.longoverdue_immediately_available",
            Some("circ.longoverdue.use_last_activity_date_on_return"),
        )
    }

    fn checkin_handle_lost_or_long_overdue(
        &mut self,
        ous_max_return: &str,
        ous_immediately_available: &str,
        ous_use_last_activity: Option<&str>,
    ) -> EgResult<()> {
        // Lost / Long-Overdue settings are based on the copy circ lib.
        let copy_circ_lib = self.copy_circ_lib();
        let max_return = self
            .settings
            .get_value_at_org(ous_max_return, copy_circ_lib)?;
        let mut too_late = false;

        if let Some(max) = max_return.as_str() {
            let interval = date::interval_to_seconds(&max)?;

            let last_activity = self.circ_last_billing_activity(ous_use_last_activity)?;
            let last_activity = date::parse_datetime(&last_activity)?;

            let last_chance = last_activity + Duration::seconds(interval);
            too_late = last_chance > Local::now();
        }

        if too_late {
            log::info!(
                "{self} check-in of lost/lo item exceeds max
                return interval.  skipping fine/fee voiding, etc."
            );
        } else if self.get_option_bool("dont_change_lost_zero") {
            log::info!(
                "{self} check-in of lost/lo item having a balance
                of zero, skipping fine/fee voiding and reinstatement."
            );
        } else {
            log::info!(
                "{self} check-in of lost/lo item is within the
                max return interval (or no interval is defined).  Proceeding
                with fine/fee voiding, etc."
            );

            self.set_option_true("needs_lost_bill_handling");
        }

        if self.circ_lib == copy_circ_lib {
            // Lost/longoverdue item is home and processed.
            // Treat like a normal checkin from this point on.
            return self.reshelve_copy(true);
        }

        // Item is not home.  Does it go right back into rotation?
        let available_now = json_bool(
            self.settings
                .get_value_at_org(ous_immediately_available, copy_circ_lib)?,
        );

        if available_now {
            // Item status does not need to be retained.
            // Put the item back into gen-pop.
            self.reshelve_copy(true)
        } else {
            log::info!("{self}: leaving lost/longoverdue copy status in place on checkin");
            Ok(())
        }
    }

    /// Last billing activity is last payment time, last billing time, or the
    /// circ due date.
    ///
    /// If the relevant "use last activity" org unit setting is
    /// false/unset, then last billing activity is always the due date.
    ///
    /// Panics if self.circ is None.
    fn circ_last_billing_activity(&mut self, maybe_setting: Option<&str>) -> EgResult<String> {
        let copy_circ_lib = self.copy_circ_lib();
        let circ = self.circ.as_ref().unwrap();

        // due_date is a required string field.
        let due_date = circ["due_date"].as_str().unwrap();

        let setting = match maybe_setting {
            Some(s) => s,
            None => return Ok(due_date.to_string()),
        };

        let use_activity = self.settings.get_value_at_org(setting, copy_circ_lib)?;

        if !json_bool(use_activity) {
            return Ok(due_date.to_string());
        }

        if let Some(mbts) = self.editor.retrieve("mbts", circ["id"].clone())? {
            if let Some(last_payment) = mbts["last_payment_ts"].as_str() {
                return Ok(last_payment.to_string());
            }
            if let Some(last_billing) = mbts["last_billing_ts"].as_str() {
                return Ok(last_billing.to_string());
            }
        }

        // No billing activity.  Fall back to due date.
        Ok(due_date.to_string())
    }

    /// Compiles the exact backdate value.
    ///
    /// Assumes circ and options.backdate are set.
    fn checkin_compile_backdate(&mut self) -> EgResult<()> {
        let duedate = match self.circ.as_ref() {
            Some(circ) => circ["due_date"]
                .as_str()
                .ok_or(format!("{self} circ has no due date?"))?,
            None => return Ok(()),
        };

        let backdate = match self.options.get("backdate") {
            Some(bd) => bd
                .as_str()
                .ok_or(format!("{self} bad backdate value: {bd}"))?,
            None => return Ok(()),
        };

        // Set the backdate hour and minute based on the hour/minute
        // of the original due date.
        let orig_date = date::parse_datetime(duedate)?;
        let mut new_date = date::parse_datetime(backdate)?;

        new_date = new_date
            .with_hour(orig_date.hour())
            .ok_or(format!("Could not set backdate hours"))?;

        new_date = new_date
            .with_minute(orig_date.minute())
            .ok_or(format!("Could not set backdate minutes"))?;

        if new_date > Local::now() {
            log::info!("{self} ignoring future backdate: {new_date}");
            self.options.remove("backdate");
        } else {
            self.options.insert(
                "backdate".to_string(),
                json::from(date::to_iso(&new_date.into())),
            );
        }

        Ok(())
    }

    fn handle_checkin_fines(&mut self) -> EgResult<()> {
        let copy_circ_lib = self.copy_circ_lib();

        if let Some(ops) = self.options.get("lost_or_lo_billing_options") {
            if !self.get_option_bool("void_overdues") {
                if let Some(setting) = ops["ous_restore_overdue"].as_str() {
                    if json_bool(self.settings.get_value_at_org(setting, copy_circ_lib)?) {
                        self.checkin_handle_lost_or_lo_now_found_restore_od(false)?;
                    }
                }
            }
        }

        let mut is_circ = false;
        let xact_id = match self.circ.as_ref() {
            Some(c) => {
                is_circ = true;
                json_int(&c["id"])?
            }
            None => match self.reservation.as_ref() {
                Some(r) => json_int(&r["id"])?,
                None => Err(format!(
                    "{self} we have no transaction to generate fines for"
                ))?,
            },
        };
        if is_circ {
            if self.circ.as_ref().unwrap()["stop_fines"].is_null() {
                billing::generate_fines_for_circ(&mut self.editor, xact_id)?;

                // Update our copy of the circ after billing changes,
                // which may apply a stop_fines value.
                self.circ = self.editor.retrieve("circ", xact_id)?;
            }

            self.set_circ_stop_fines()?;
        } else {
            billing::generate_fines_for_resv(&mut self.editor, xact_id)?;
        }

        if !self.get_option_bool("needs_lost_bill_handling") {
            // No lost/lo billing work required.  All done.
            return Ok(());
        }

        let ops = match self.options.get("lost_or_lo_billing_options") {
            Some(o) => o,
            None => Err(format!("Cannot handle lost/lo billing without options"))?,
        };

        // below was previously called checkin_handle_lost_or_lo_now_found()
        let tag = if json_bool(&ops["is_longoverdue"]) {
            "LONGOVERDUE"
        } else {
            "LOST"
        };
        let note = format!("{tag} ITEM RETURNED");

        let mut void_cost = 0.0;
        if let Some(set) = ops["ous_void_item_cost"].as_str() {
            if let Ok(c) = json_float(self.settings.get_value_at_org(set, copy_circ_lib)?) {
                void_cost = c;
            }
        }

        let mut void_proc_fee = 0.0;
        if let Some(set) = ops["ous_void_proc_fee"].as_str() {
            if let Ok(c) = json_float(self.settings.get_value_at_org(set, copy_circ_lib)?) {
                void_proc_fee = c;
            }
        }

        if void_cost > 0.0 {
            let void_cost_btype = match ops["void_cost_btype"].as_i64() {
                Some(b) => b,
                None => {
                    log::warn!("Cannot zero {tag} circ without a billing type");
                    return Ok(());
                }
            };

            billing::void_or_zero_bills_of_type(
                &mut self.editor,
                xact_id,
                copy_circ_lib,
                void_cost_btype,
                &note,
            )?;
        }

        if void_proc_fee > 0.0 {
            let void_fee_btype = match ops["void_fee_btype"].as_i64() {
                Some(b) => b,
                None => {
                    log::warn!("Cannot zero {tag} circ without a billing type");
                    return Ok(());
                }
            };

            billing::void_or_zero_bills_of_type(
                &mut self.editor,
                xact_id,
                copy_circ_lib,
                void_fee_btype,
                &note,
            )?;
        }

        Ok(())
    }

    fn set_circ_stop_fines(&mut self) -> EgResult<()> {
        let circ = self.circ.as_ref().unwrap();

        if !circ["stop_fines"].is_null() {
            return Ok(());
        }

        // Set stop_fines and stop_fines_time on our open circulation.
        let stop_fines = if self.circ_op == CircOp::Renew {
            "RENEW"
        } else if self.get_option_bool("claims_never_checked_out") {
            "CLAIMSNEVERCHECKEDOUT"
        } else {
            "CHECKIN"
        };

        let stop_fines = json::from(stop_fines);

        let stop_fines_time = match self.options.get("backdate") {
            Some(bd) => bd.clone(),
            None => json::from("now"),
        };

        let circ = self.circ.as_mut().unwrap();

        circ["stop_fines"] = stop_fines;
        circ["stop_fines_time"] = stop_fines_time;

        self.editor.update(circ)?;

        // Update our copy to get in-DB changes.
        self.circ = self.editor.retrieve("circ", circ["id"].clone())?;

        Ok(())
    }

    /// Restore voided/adjusted overdue fines on lost/long-overdue return.
    fn checkin_handle_lost_or_lo_now_found_restore_od(
        &mut self,
        is_longoverdue: bool,
    ) -> EgResult<()> {
        let circ = self.circ.as_ref().unwrap();
        let circ_id = json_int(&circ["id"])?;

        let query = json::object! {xact: circ_id, btype: C::BTYPE_OVERDUE_MATERIALS};
        let ops = json::object! {"order_by": {"mb": "billing_ts desc"}};
        let overdues = self.editor.search_with_ops("mb", query, ops)?;

        if overdues.len() == 0 {
            log::info!("{self} no overdues to reinstate on lost/lo checkin");
            return Ok(());
        }

        let tag = if is_longoverdue {
            "LONGOVERRDUE"
        } else {
            "LOST"
        };
        log::info!("{self} re-instating {} pre-{tag} overdues", overdues.len());

        let void_max = json_float(&circ["max_fine"])?;
        let mut void_amount = 0.0;

        let billing_ids: Vec<JsonValue> = overdues.iter().map(|b| b["id"].clone()).collect();
        let voids = self
            .editor
            .search("maa", json::object! {"billing": billing_ids})?;

        if voids.len() > 0 {
            // Overdues adjusted via account adjustment
            for void in voids.iter() {
                void_amount += json_float(&void["amount"])?;
            }
        } else {
            // Overdues voided the old-fashioned way, i.e. voided.
            for bill in overdues.iter() {
                if json_bool(&bill["voided"]) {
                    void_amount += json_float(&bill["amount"])?;
                }
            }
        }

        if void_amount == 0.0 {
            log::info!("{self} voided overdues amounted to $0.00.  Nothing to restore");
            return Ok(());
        }

        if void_amount > void_max {
            void_amount = void_max;
        }

        // We have at least one overdue
        let first_od = overdues.first().unwrap();
        let last_od = overdues.last().unwrap();

        let btype_label = first_od["billing_type"].as_str().unwrap(); // required field
        let period_start = first_od["period_start"].as_str();
        let period_end = last_od["period_end"].as_str();

        let note = format!("System: {tag} RETURNED - OVERDUES REINSTATED");

        billing::create_bill(
            &mut self.editor,
            void_amount,
            C::BTYPE_OVERDUE_MATERIALS,
            btype_label,
            circ_id,
            Some(&note),
            period_start,
            period_end,
        )?;

        Ok(())
    }

    /// Assumes self.transit is set
    fn checkin_handle_transit(&mut self) -> EgResult<()> {
        log::info!("{self} attempting to receive transit");

        let transit = self.transit.as_ref().unwrap();
        let transit_id = json_int(&transit["id"])?;
        let transit_dest = json_int(&transit["dest"])?;
        let transit_copy_status = json_int(&transit["copy_status"])?;

        let for_hold = transit_copy_status == C::COPY_STATUS_ON_HOLDS_SHELF;
        let suppress_transit = self.should_suppress_transit(transit_dest, for_hold)?;

        if for_hold && suppress_transit {
            self.set_option_true("fake_hold_dest");
        }

        self.hold_transit = self.editor.retrieve("ahtc", transit_id)?;

        if let Some(ht) = self.hold_transit.as_ref() {
            // A hold transit can have a null "hold" value if the linked
            // hold was anonymized while in transit.
            if !ht["hold"].is_null() {
                self.hold = self.editor.retrieve("ahr", ht["hold"].clone())?;
            }
        }

        let hold_as_transit = self.get_option_bool("hold_as_transit")
            && transit_copy_status == C::COPY_STATUS_ON_HOLDS_SHELF;

        if !suppress_transit && (transit_dest != self.circ_lib || hold_as_transit) {
            // Item is in-transit to a different location OR
            // we are captured holds as transits and don't need another one.

            log::info!(
                "{self}: Fowarding transit on copy which is destined
                for a different location. transit={transit_id} destination={transit_dest}"
            );

            let mut evt = EgEvent::new("ROUTE_ITEM");
            evt.set_org(transit_dest);

            return self.exit_ok_on_event(evt);
        }

        // Receive the transit
        let transit = self.transit.as_mut().unwrap();
        transit["dest_recv_time"] = json::from("now");
        self.editor.update(&transit)?;

        // Refresh our copy of the transit.
        self.transit = self.editor.retrieve("atc", transit_id)?;

        // Apply the destination copy status.
        self.update_copy(json::object! {"status": transit_copy_status})?;

        if self.hold.is_some() {
            self.put_hold_on_shelf()?;
        } else {
            self.hold_transit = None;
            self.reshelve_copy(true)?;
            self.clear_option("fake_hold_dest");
        }

        let mut payload = json::object! {
            transit: self.transit.as_ref().unwrap().clone()
        };

        if let Some(ht) = self.hold_transit.as_ref() {
            payload["holdtransit"] = ht.clone();
        }

        let mut evt = EgEvent::success();
        evt.set_payload(payload);
        evt.set_ad_hoc_value("ishold", json::from(self.hold.is_some()));

        self.add_event(evt);

        Ok(())
    }

    /// This handles standard hold transits as well as items
    /// that transited here w/o a hold transit yet are in
    /// fact captured for a hold.
    fn checkin_handle_received_hold(&mut self) -> EgResult<()> {
        let copy = self.copy.as_ref().unwrap();

        if self.hold_transit.is_none()
            && json_int(&copy["status"])? != C::COPY_STATUS_ON_HOLDS_SHELF
        {
            // No hold transit and not headed for the holds shelf.
            return Ok(());
        }

        let mut alt_hold;
        let hold = match self.hold.as_mut() {
            Some(h) => h,
            None => match holds::captured_hold_for_copy(&mut self.editor, self.copy_id.unwrap())? {
                Some(h) => {
                    alt_hold = Some(h);
                    alt_hold.as_mut().unwrap()
                }
                None => {
                    log::warn!("{self} item should be captured, but isn't, skipping");
                    return Ok(());
                }
            },
        };

        if !hold["cancel_time"].is_null() || !hold["fulfillment_time"].is_null() {
            // Hold cancled or filled mid-transit
            self.reshelve_copy(false)?;
            self.clear_option("fake_hold_dest");
            return Ok(());
        }

        if hold["hold_type"].as_str().unwrap() == "R" {
            // hold_type required
            self.update_copy(json::object! {status: C::COPY_STATUS_CATALOGING})?;
            self.clear_option("fake_hold_dest");
            // no further processing needed.
            self.set_option_true("noop");

            let hold = self.hold.as_mut().unwrap();
            hold["fulfillment_time"] = json::from("now");
            self.editor.update(&hold)?;

            return Ok(());
        }

        if self.get_option_bool("fake_hold_dest") {
            let hold = self.hold.as_mut().unwrap();
            // Perl code does not update the hold in the database
            // at this point.  Doing same.
            hold["pickup_lib"] = json::from(self.circ_lib);

            return Ok(());
        }

        Ok(())
    }

    fn should_suppress_transit(&mut self, destination: i64, for_hold: bool) -> EgResult<bool> {
        if destination == self.circ_lib {
            return Ok(false);
        }

        if for_hold && self.get_option_bool("hold_as_transit") {
            return Ok(false);
        }

        let setting = if for_hold {
            "circ.transit.suppress_hold"
        } else {
            "circ.transit.suppress_non_hold"
        };

        // These value for these settings is opaque.  If a value is
        // set (i.e. not null), then we only care of they match.
        // Values are clone()ed to avoid parallel mutable borrows.
        let suppress_for_here = self.settings.get_value(setting)?.clone();
        if suppress_for_here.is_null() {
            return Ok(false);
        }

        let suppress_for_dest = self
            .settings
            .get_value_at_org(setting, self.circ_lib)?
            .clone();
        if suppress_for_dest.is_null() {
            return Ok(false);
        }

        // json::* knows if two JsonValue's are the same.
        if suppress_for_here != suppress_for_dest {
            return Ok(false);
        }

        Ok(true)
    }

    /// Set hold shelf values and update the hold.
    fn put_hold_on_shelf(&mut self) -> EgResult<()> {
        let hold = self.hold.as_mut().unwrap();
        let hold_id = json_int(&hold["id"])?;

        hold["shelf_time"] = json::from("now");
        hold["current_shelf_lib"] = json::from(self.circ_lib);

        if let Some(date) = holds::calc_hold_shelf_expire_time(&mut self.editor, &hold, None)? {
            hold["shelf_expire_time"] = json::from(date);
        }

        self.editor.update(&hold)?;
        self.hold = self.editor.retrieve("ahr", hold_id)?;

        Ok(())
    }

    fn try_to_capture(&mut self) -> EgResult<bool> {
        let mut needed = false;

        if self.get_option_bool("remote_hold") {
            needed = self.attempt_checkin_hold_capture()?;
            return Ok(needed);
        }

        /*
        if (!$self->remote_hold) {
            if ($self->use_booking) {
                my $potential_hold = $self->hold_capture_is_possible;
                my $potential_reservation = $self->reservation_capture_is_possible;

                if ($potential_hold and $potential_reservation) {
                    $logger->info("circulator: item could fulfill either hold or reservation");
                    $self->push_events(new OpenILS::Event(
                        "HOLD_RESERVATION_CONFLICT",
                        "hold" => $potential_hold,
                        "reservation" => $potential_reservation
                    ));
                    return if $self->bail_out;
                } elsif ($potential_hold) {
                    $needed_for_something =
                        $self->attempt_checkin_hold_capture;
                } elsif ($potential_reservation) {
                    $needed_for_something =
                        $self->attempt_checkin_reservation_capture;
                }
            } else {
                $needed_for_something = $self->attempt_checkin_hold_capture;
            }
        }
        */

        Ok(needed)
    }

    fn attempt_checkin_hold_capture(&mut self) -> EgResult<bool> {
        if let Some(value) = self.options.get("capture") {
            if let Some(capture) = value.as_str() {
                if capture == "nocapture" {
                    return Ok(false);
                }
            }
        }

        let maybe_found =
            holds::find_nearest_permitted_hold(&mut self.editor, self.copy_id.unwrap(), false)?;

        let (mut hold, retarget) = match maybe_found {
            Some(info) => info,
            None => {
                log::info!("{self} no permitted holds found for copy");
                return Ok(false);
            }
        };

        if let Some(capture) = self.options.get("capture") {
            if capture != "capture" {
                // See if this item is in a hold-capture-verify location.
                if json_bool(&self.copy()["location"]["hold_verify"]) {
                    let mut evt = EgEvent::new("HOLD_CAPTURE_DELAYED");
                    evt.set_ad_hoc_value("copy_location", self.copy()["location"].clone());
                    self.exit_err_on_event(evt)?;
                }
            }
        }

        if retarget.len() > 0 {
            self.retarget_holds = Some(retarget);
        }

        let pickup_lib = json_int(&hold["pickup_lib"])?;
        let suppress_transit = self.should_suppress_transit(pickup_lib, true)?;

        hold["hopeless_date"] = JsonValue::Null;
        hold["current_copy"] = json::from(self.copy_id.unwrap());
        hold["capture_time"] = json::from("now");

        // Clear some other potential cruft
        hold["fulfillment_time"] = JsonValue::Null;
        hold["fulfillment_staff"] = JsonValue::Null;
        hold["fulfillment_lib"] = JsonValue::Null;
        hold["expire_time"] = JsonValue::Null;
        hold["cancel_time"] = JsonValue::Null;

        if suppress_transit ||
            (pickup_lib == self.circ_lib && !self.get_option_bool("hold_as_transit")) {
            self.hold = Some(hold);
            // This updates and refreshes the hold.
            self.put_hold_on_shelf()?;
        } else {
            self.editor.update(&hold)?;
            self.hold = self.editor.retrieve("ahr", json_int(&hold["id"])?)?;
        }

        Ok(true)
    }
}
