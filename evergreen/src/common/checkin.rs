use crate as eg;
use chrono::Timelike;
use eg::common::billing;
use eg::common::circulator::{CircOp, Circulator};
use eg::common::holds;
use eg::common::penalty;
use eg::common::targeter;
use eg::common::transit;
use eg::constants as C;
use eg::date;
use eg::event::EgEvent;
use eg::result::{EgError, EgResult};
use eg::EgValue;
use std::collections::HashSet;

/// Performs item checkins
impl Circulator<'_> {
    /// Checkin an item.
    ///
    /// Returns Ok(()) if the active transaction should be committed and
    /// Err(EgError) if the active transaction should be rolled backed.
    pub fn checkin(&mut self) -> EgResult<()> {
        if self.circ_op == CircOp::Unset {
            self.circ_op = CircOp::Checkin;
        }

        self.init()?;

        if !self.is_renewal() && !self.editor.allowed_at("COPY_CHECKIN", self.circ_lib)? {
            return Err(self.editor().die_event());
        }

        log::info!("{self} starting checkin");

        self.basic_copy_checks()?;

        self.fix_broken_transit_status()?;
        self.check_transit_checkin_interval()?;
        self.checkin_retarget_holds()?;
        self.cancel_transit_if_circ_exists()?;
        self.hold_revert_sanity_checks()?;
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

        if self.is_renewal() {
            self.finish_fines_and_voiding()?;
            self.add_event_code("SUCCESS");
            return Ok(());
        }

        if self.revert_hold_fulfillment()? {
            return Ok(());
        }

        // Circulations and transits are now closed where necessary.
        // Now see if this copy can fulfill a hold or needs to be
        // routed to a different location.

        let mut item_is_needed = false;
        if self.get_option_bool("noop") {
            if self.get_option_bool("can_float") {
                // As noted in the Perl, it may be unexpected that
                // floating items are modified during NO-OP checkins,
                // but the behavior is retained for backwards compat.
                self.update_copy(eg::hash! {"circ_lib": self.circ_lib})?;
            }
        } else {
            item_is_needed = self.try_to_capture()?;
            if !item_is_needed {
                self.try_to_transit()?;
            }
        }

        if !self.handle_claims_never()? && !item_is_needed {
            self.reshelve_copy(false)?;
        }

        if self.editor().has_pending_changes() {
            if self.events.len() == 0 {
                self.add_event(EgEvent::success());
            }
        } else {
            self.add_event(EgEvent::new("NO_CHANGE"));
        }

        self.finish_fines_and_voiding()?;

        if self.patron.is_some() {
            penalty::calculate_penalties(self.editor, self.patron_id, self.circ_lib, None)?;
        }

        self.cleanup_events();
        self.flesh_checkin_events()?;

        Ok(())
    }

    /// Returns true if claims-never-checked-out handling occurred.
    fn handle_claims_never(&mut self) -> EgResult<bool> {
        if !self.get_option_bool("claims_never_checked_out") {
            return Ok(false);
        }

        let circ = match self.circ.as_ref() {
            Some(c) => c, // should be set at this point
            None => return Ok(false),
        };

        if !self
            .settings
            .get_value_at_org(
                "circ.claim_never_checked_out.mark_missing",
                circ["circ_lib"].int()?,
            )?
            .boolish()
        {
            return Ok(false);
        }

        // Configured to mark claims never checked out as Missing.
        // Note to self: this would presumably be a circ-id based
        // checkin instead of a copy id/barcode checkin.

        let next_status = match self.options.get("next_copy_status") {
            Some(s) => s.int()?,
            None => C::COPY_STATUS_MISSING,
        };

        self.update_copy(eg::hash! {"status": next_status})?;

        Ok(true)
    }

    /// What value did the caller provide for the "capture" option, if any.
    fn capture_state(&self) -> &str {
        match self.options.get("capture") {
            Some(c) => c.as_str().unwrap_or(""),
            None => "",
        }
    }

    /// Load the open transit and make sure our copy is in the right
    /// status if there's a matching transit.
    fn fix_broken_transit_status(&mut self) -> EgResult<()> {
        let query = eg::hash! {
            target_copy: self.copy()["id"].clone(),
            dest_recv_time: EgValue::Null,
            cancel_time: EgValue::Null,
        };

        let mut results = self.editor().search("atc", query)?;

        let transit = match results.pop() {
            Some(t) => t,
            None => return Ok(()),
        };

        if self.copy_status() != C::COPY_STATUS_IN_TRANSIT {
            log::warn!("{self} Copy has an open transit, but incorrect status");
            let changes = eg::hash! {status: C::COPY_STATUS_IN_TRANSIT};
            self.update_copy(changes)?;
        }

        self.transit = Some(transit);

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
        let interval = interval.string()?;

        // source_send_time is a known non-null string value.
        let send_time_str = transit["source_send_time"].as_str().unwrap();
        let send_time = date::parse_datetime(send_time_str)?;

        let horizon = date::add_interval(send_time, &interval)?;

        if horizon > date::now() {
            self.add_event_code("TRANSIT_CHECKIN_INTERVAL_BLOCK");
        }

        Ok(())
    }

    /// Retarget local holds that might wish to use our copy as
    /// a target.  Useful if the copy is going from a non-holdable
    /// to a holdable status and the hold targeter may not run
    /// until, say, overnight.
    fn checkin_retarget_holds(&mut self) -> EgResult<()> {
        let copy = self.copy();

        let retarget_mode = self
            .options
            .get("retarget_mode")
            .map(|v| v.as_str().unwrap_or(""))
            .unwrap_or("");

        // A lot of scenarios can lead to avoiding hold fulfillment checks.
        if !retarget_mode.contains("retarget")
            || self.get_option_bool("revert_hold_fulfillment")
            || self.capture_state() == "nocapture"
            || self.is_precat_copy()
            || copy["circ_lib"].int()? != self.circ_lib
            || copy["deleted"].boolish()
            || !copy["holdable"].boolish()
            || !copy["status"]["holdable"].boolish()
            || !copy["location"]["holdable"].boolish()
        {
            return Ok(());
        }

        // By default, we only care about in-process items.
        if !retarget_mode.contains(".all") && self.copy_status() != C::COPY_STATUS_IN_PROCESS {
            return Ok(());
        }

        let query = eg::hash! {target_copy: EgValue::from(self.copy_id)};
        let parts = self.editor().search("acpm", query)?;
        let parts = parts
            .into_iter()
            .map(|p| p.id().expect("ID Required"))
            .collect::<HashSet<_>>();

        let copy_id = self.copy_id;
        let circ_lib = self.circ_lib;
        let vol_id = self.copy()["call_number"].id()?;

        let hold_data = holds::related_to_copy(
            self.editor(),
            copy_id,
            Some(circ_lib),
            None,
            None,
            Some(false), // already on holds shelf
        )?;

        // Since we're targeting a batch of holds, instead of a single hold,
        // let the targeter manage the transaction.  Otherwise, we could
        // target a large number of holds within a single transaction,
        // which is no bueno.
        let mut editor = self.editor().clone();
        let mut hold_targeter = targeter::HoldTargeter::new(&mut editor);

        for hold in hold_data.iter() {
            let target = hold.target();
            let hold_type: &str = hold.hold_type().into();

            // Copy-level hold that points to a different copy.
            if target != copy_id && (hold_type.eq("C") || hold_type.eq("R") || hold_type.eq("F")) {
                continue;
            }

            // Volume-level hold for a different volume
            if target != vol_id && hold_type.eq("V") {
                continue;
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

            let ctx = hold_targeter.target_hold(hold.id(), Some(copy_id))?;

            if ctx.success() && ctx.found_copy() {
                log::info!("checkin_retarget_holds() successfully targeted a hold");
                break;
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
            let transit_id = transit.id()?;
            log::info!(
                "{self} copy is both checked out and in transit.  Canceling transit {transit_id}"
            );
            transit::cancel_transit(self.editor(), transit_id, false)?;
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
            self.copy()["circ_lib"].int()?,
        )?;

        let mut dont_change = value.boolish();

        if dont_change {
            // Org setting says not to change.
            // Make sure no balance is owed, or the setting is meaningless.

            if let Some(circ) = self.circ.as_ref() {
                let circ_id = circ["id"].clone();
                if let Some(mbts) = self.editor().retrieve("mbts", circ_id)? {
                    dont_change = mbts["balance_owed"].float()? == 0.0;
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

        let float_id = float_id.clone();

        // Copy can float.  Can it float here?

        let float_group = self
            .editor()
            .retrieve("cfg", float_id)?
            .ok_or_else(|| self.editor().die_event())?;

        let query = eg::hash! {
            from: [
                "evergreen.can_float",
                float_group["id"].clone(),
                self.copy()["circ_lib"].clone(),
                self.circ_lib
            ]
        };

        if let Some(resp) = self.editor().json_query(query)?.first() {
            if resp["evergreen.can_float"].boolish() {
                self.set_option_true("can_float");
            }
        }

        Ok(())
    }

    /// Set an inventory date for our item if requested.
    fn do_inventory_update(&mut self) -> EgResult<()> {
        if !self.get_option_bool("do_inventory_update") {
            return Ok(());
        }

        let ws_id = match self.editor().requestor_ws_id() {
            Some(i) => i,
            // Cannot perform inventory without a workstation.
            None => return Ok(()),
        };

        if self.copy()["circ_lib"].int()? != self.circ_lib && !self.get_option_bool("can_float") {
            // Item is not home and cannot float
            return Ok(());
        }

        // Create a new copy inventory row.
        let aci = eg::hash! {
            inventory_date: "now",
            inventory_workstation: ws_id,
            copy: self.copy()["id"].clone(),
        };

        self.editor().create(aci)?;

        Ok(())
    }

    /// True if our item is currently on the local holds shelf or sits
    /// within a hold transit suppression group.
    ///
    /// Shelf-expired holds for our copy may also be cleared if requested.
    fn check_is_on_holds_shelf(&mut self) -> EgResult<bool> {
        if self.copy_status() != C::COPY_STATUS_ON_HOLDS_SHELF {
            return Ok(false);
        }

        let copy_id = self.copy_id;

        if self.get_option_bool("clear_expired") {
            // Clear shelf-expired holds for this copy.
            // TODO run in the same transaction once ported to Rust.

            let params = vec![
                EgValue::from(self.editor().authtoken()),
                EgValue::from(self.circ_lib),
                self.copy()["id"].clone(),
            ];

            self.editor().client_mut().send_recv_one(
                "open-ils.circ",
                "open-ils.circ.hold.clear_shelf.process",
                params,
            )?;
        }

        let hold = match holds::captured_hold_for_copy(self.editor(), copy_id)? {
            Some(h) => h,
            None => {
                log::warn!("{self} Copy on holds shelf but there is no hold");
                self.reshelve_copy(false)?;
                return Ok(false);
            }
        };

        let pickup_lib = hold["pickup_lib"].int()?;

        log::info!("{self} we found a captured, un-fulfilled hold");

        if pickup_lib != self.circ_lib && !self.get_option_bool("hold_as_transit") {
            let suppress_here = self.settings.get_value("circ.transit.suppress_hold")?;

            let suppress_here = match suppress_here.string() {
                Ok(s) => s,
                Err(_) => String::from(""),
            };

            let suppress_there = self
                .settings
                .get_value_at_org("circ.transit.suppress_hold", pickup_lib)?;

            let suppress_there = match suppress_there.string() {
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

    /// Sets our copy's status to the determined next-copy-status,
    /// or to Reshelving, with a few potential execptions.
    fn reshelve_copy(&mut self, force: bool) -> EgResult<()> {
        let force = force || self.get_option_bool("force");

        let status = self.copy_status();

        let next_status = match self.options.get("next_copy_status") {
            Some(s) => s.int()?,
            None => C::COPY_STATUS_RESHELVING,
        };

        if force
            || (status != C::COPY_STATUS_ON_HOLDS_SHELF
                && status != C::COPY_STATUS_CATALOGING
                && status != C::COPY_STATUS_IN_TRANSIT
                && status != next_status)
        {
            self.update_copy(eg::hash! {status: EgValue::from(next_status)})?;
        }

        Ok(())
    }

    /// Returns claims-returned event if our circulation is claims returned.
    fn check_claims_returned(&mut self) {
        if let Some(circ) = self.circ.as_ref() {
            if let Some(sf) = circ["stop_fines"].as_str() {
                if sf == "CLAIMSRETURNED" {
                    self.add_event_code("CIRC_CLAIMS_RETURNED");
                }
            }
        }
    }

    /// Checks for an existing deposit payment and voids the deposit
    /// if configured OR returns a deposit paid event.
    fn check_circ_deposit(&mut self, void: bool) -> EgResult<()> {
        let circ_id = match self.circ.as_ref() {
            Some(c) => c["id"].clone(),
            None => return Ok(()),
        };

        let query = eg::hash! {
            btype: C::BTYPE_DEPOSIT,
            voided: "f",
            xact: circ_id,
        };

        let mut results = self.editor().search("mb", query)?;
        let deposit = match results.pop() {
            Some(d) => d,
            None => return Ok(()),
        };

        if void {
            // Caller suggests we void.  Verify settings allow it.
            if self.settings.get_value("circ.void_item_deposit")?.boolish() {
                let bill_id = deposit.id()?;
                billing::void_bills(self.editor(), &[bill_id], Some("DEPOSIT ITEM RETURNED"))?;
            }
        } else {
            let mut evt = EgEvent::new("ITEM_DEPOSIT_PAID");
            evt.set_payload(deposit);
            self.add_event(evt);
        }

        Ok(())
    }

    /// Checkin our open circulation and potentially kick off
    /// lost/long-overdue item handling, among a few other smaller tasks.
    fn checkin_handle_circ(&mut self) -> EgResult<()> {
        let selfstr: String = self.to_string();

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

        let req_id = self.requestor_id()?;
        let req_ws_id = self.editor().requestor_ws_id();

        let circ = self.circ.as_mut().unwrap();
        let circ_id = circ.id()?;

        circ["checkin_time"] = self
            .options
            .get("backdate")
            .map(|bd| bd.clone())
            .unwrap_or(EgValue::from("now"));

        circ["checkin_scan_time"] = EgValue::from("now");
        circ["checkin_staff"] = EgValue::from(req_id);
        circ["checkin_lib"] = EgValue::from(self.circ_lib);
        if let Some(id) = req_ws_id {
            circ["checkin_workstation"] = EgValue::from(id);
        }

        log::info!(
            "{selfstr} checking item in with checkin_time {}",
            circ["checkin_time"]
        );

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
            _ => {
                if !self.is_renewal() {
                    // DB renew-permit function requires the renewed
                    // copy to be in the checked-out status.
                    self.reshelve_copy(true)?;
                }
            }
        }

        if self.get_option_bool("dont_change_lost_zero") {
            // Caller has requested we leave well enough alone, i.e.
            // if an item was lost and paid, it's not eligible to be
            // re-opened for additional billing.
            let circ = self.circ.as_ref().unwrap().clone();
            self.editor().update(circ)?;
        } else {
            if self.get_option_bool("claims_never_checked_out") {
                let circ = self.circ.as_mut().unwrap();
                circ["stop_fines"] = EgValue::from("CLAIMSNEVERCHECKEDOUT");
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
                    let circ = self.circ.as_mut().unwrap();
                    circ["stop_fines"].take();
                }
            }

            let circ = self.circ.as_ref().unwrap().clone();
            self.editor().update(circ)?;
            self.handle_checkin_fines()?;
        }

        self.check_circ_deposit(true)?;

        log::debug!("{selfstr} checking open transaction state");

        // Set/clear stop_fines as needed.
        billing::check_open_xact(self.editor(), circ_id)?;

        // Get a post-save version of the circ to pick up any in-DB changes.
        self.circ = self.editor().retrieve("circ", circ_id)?;

        Ok(())
    }

    /// Collect params and call checkin_handle_lost_or_long_overdue()
    fn checkin_handle_lost(&mut self) -> EgResult<()> {
        log::info!("{self} processing LOST checkin...");

        let billing_options = eg::hash! {
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

    /// Collect params and call checkin_handle_lost_or_long_overdue()
    fn checkin_handle_long_overdue(&mut self) -> EgResult<()> {
        let billing_options = eg::hash! {
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

    /// Determines if/what additional LOST/LO handling is needed for
    /// our circulation.
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
            .get_value_at_org(ous_max_return, copy_circ_lib)?
            .clone(); // parallel
        let mut too_late = false;

        if let Some(max) = max_return.as_str() {
            let last_activity = self.circ_last_billing_activity(ous_use_last_activity)?;
            let last_activity = date::parse_datetime(&last_activity)?;

            let last_chance = date::add_interval(last_activity, max)?;
            too_late = last_chance > date::now();
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
        let available_now = self
            .settings
            .get_value_at_org(ous_immediately_available, copy_circ_lib)?
            .boolish();

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
        let circ_id = circ["id"].clone();

        // to_string() early to avoid some mutable borrow issues
        let due_date = circ["due_date"].as_str().unwrap().to_string();

        let setting = match maybe_setting {
            Some(s) => s,
            None => return Ok(due_date),
        };

        let use_activity = self.settings.get_value_at_org(setting, copy_circ_lib)?;

        if !use_activity.boolish() {
            return Ok(due_date);
        }

        if let Some(mbts) = self.editor().retrieve("mbts", circ_id)? {
            if let Some(last_payment) = mbts["last_payment_ts"].as_str() {
                return Ok(last_payment.to_string());
            }
            if let Some(last_billing) = mbts["last_billing_ts"].as_str() {
                return Ok(last_billing.to_string());
            }
        }

        // No billing activity.  Fall back to due date.
        Ok(due_date)
    }

    /// Compiles the exact backdate value.
    ///
    /// Assumes circ and options.backdate are set.
    fn checkin_compile_backdate(&mut self) -> EgResult<()> {
        let duedate = match self.circ.as_ref() {
            Some(circ) => circ["due_date"]
                .as_str()
                .ok_or_else(|| format!("{self} circ has no due date?"))?,
            None => return Ok(()),
        };

        let backdate = match self.options.get("backdate") {
            Some(bd) => bd
                .as_str()
                .ok_or_else(|| format!("{self} bad backdate value: {bd}"))?,
            None => return Ok(()),
        };

        // Set the backdate hour and minute based on the hour/minute
        // of the original due date.
        let orig_date = date::parse_datetime(duedate)?;
        let mut new_date = date::parse_datetime(backdate)?;

        new_date = new_date
            .with_hour(orig_date.hour())
            .ok_or_else(|| format!("Could not set backdate hours"))?;

        new_date = new_date
            .with_minute(orig_date.minute())
            .ok_or_else(|| format!("Could not set backdate minutes"))?;

        if new_date > date::now() {
            log::info!("{self} ignoring future backdate: {new_date}");
            self.options.remove("backdate");
        } else {
            self.options.insert(
                "backdate".to_string(),
                EgValue::from(date::to_iso(&new_date.into())),
            );
        }

        Ok(())
    }

    /// Run our circ through fine generation and potentially perform
    /// additional LOST/LO billing/voiding/etc steps.
    fn handle_checkin_fines(&mut self) -> EgResult<()> {
        let copy_circ_lib = self.copy_circ_lib();

        if let Some(ops) = self.options.get("lost_or_lo_billing_options") {
            if !self.get_option_bool("void_overdues") {
                if let Some(setting) = ops["ous_restore_overdue"].as_str() {
                    if self
                        .settings
                        .get_value_at_org(setting, copy_circ_lib)?
                        .boolish()
                    {
                        self.checkin_handle_lost_or_lo_now_found_restore_od(false)?;
                    }
                }
            }
        }

        let mut is_circ = false;
        let xact_id = match self.circ.as_ref() {
            Some(c) => {
                is_circ = true;
                c.id()?
            }
            None => match self.reservation.as_ref() {
                Some(r) => r.id()?,
                None => Err(format!(
                    "{self} we have no transaction to generate fines for"
                ))?,
            },
        };
        if is_circ {
            if self.circ.as_ref().unwrap()["stop_fines"].is_null() {
                billing::generate_fines_for_circ(self.editor(), xact_id)?;

                // Update our copy of the circ after billing changes,
                // which may apply a stop_fines value.
                self.circ = self.editor().retrieve("circ", xact_id)?;
            }

            self.set_circ_stop_fines()?;
        } else {
            billing::generate_fines_for_resv(self.editor(), xact_id)?;
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
        let tag = if ops["is_longoverdue"].boolish() {
            "LONGOVERDUE"
        } else {
            "LOST"
        };
        let note = format!("{tag} ITEM RETURNED");

        let mut void_cost = 0.0;
        if let Some(set) = ops["ous_void_item_cost"].as_str() {
            if let Ok(c) = self.settings.get_value_at_org(set, copy_circ_lib)?.float() {
                void_cost = c;
            }
        }

        let mut void_proc_fee = 0.0;
        if let Some(set) = ops["ous_void_proc_fee"].as_str() {
            if let Ok(c) = self.settings.get_value_at_org(set, copy_circ_lib)?.float() {
                void_proc_fee = c;
            }
        }

        let void_cost_btype = ops["void_cost_btype"].as_i64().unwrap_or(0);
        let void_fee_btype = ops["void_fee_btype"].as_i64().unwrap_or(0);

        if void_cost > 0.0 {
            if void_cost_btype == 0 {
                log::warn!("Cannot zero {tag} circ without a billing type");
                return Ok(());
            }

            billing::void_or_zero_bills_of_type(
                self.editor(),
                xact_id,
                copy_circ_lib,
                void_cost_btype,
                &note,
            )?;
        }

        if void_proc_fee > 0.0 {
            if void_fee_btype == 0 {
                log::warn!("Cannot zero {tag} circ without a billing type");
                return Ok(());
            }

            billing::void_or_zero_bills_of_type(
                self.editor(),
                xact_id,
                copy_circ_lib,
                void_fee_btype,
                &note,
            )?;
        }

        Ok(())
    }

    /// Apply a reasonable stop_fines / time value to our circ.
    ///
    /// Does nothing if the circ already has a stop_fines value.
    fn set_circ_stop_fines(&mut self) -> EgResult<()> {
        let circ = self.circ.as_ref().unwrap();

        if !circ["stop_fines"].is_null() {
            return Ok(());
        }

        // Set stop_fines and stop_fines_time on our open circulation.
        let stop_fines = if self.is_renewal() {
            "RENEW"
        } else if self.get_option_bool("claims_never_checked_out") {
            "CLAIMSNEVERCHECKEDOUT"
        } else {
            "CHECKIN"
        };

        let stop_fines = EgValue::from(stop_fines);

        let stop_fines_time = match self.options.get("backdate") {
            Some(bd) => bd.clone(),
            None => EgValue::from("now"),
        };

        let mut circ = circ.clone();

        let circ_id = circ["id"].clone();

        circ["stop_fines"] = stop_fines;
        circ["stop_fines_time"] = stop_fines_time;

        self.editor().update(circ)?;

        // Update our copy to get in-DB changes.
        self.circ = self.editor().retrieve("circ", circ_id)?;

        Ok(())
    }

    /// Restore voided/adjusted overdue fines on lost/long-overdue return.
    fn checkin_handle_lost_or_lo_now_found_restore_od(
        &mut self,
        is_longoverdue: bool,
    ) -> EgResult<()> {
        let circ = self.circ.as_ref().unwrap();
        let circ_id = circ.id()?;
        let void_max = circ["max_fine"].float()?;

        let query = eg::hash! {xact: circ_id, btype: C::BTYPE_OVERDUE_MATERIALS};
        let ops = eg::hash! {"order_by": {"mb": "billing_ts desc"}};
        let overdues = self.editor().search_with_ops("mb", query, ops)?;

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

        let mut void_amount = 0.0;

        let billing_ids: Vec<EgValue> = overdues.iter().map(|b| b["id"].clone()).collect();
        let voids = self
            .editor()
            .search("maa", eg::hash! {"billing": billing_ids})?;

        if voids.len() > 0 {
            // Overdues adjusted via account adjustment
            for void in voids.iter() {
                void_amount += void["amount"].float()?;
            }
        } else {
            // Overdues voided the old-fashioned way, i.e. voided.
            for bill in overdues.iter() {
                if bill["voided"].boolish() {
                    void_amount += bill["amount"].float()?;
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
            self.editor(),
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

    /// Receive the transit or tell the caller it needs to go elsewhere.
    ///
    /// Assumes self.transit is set
    fn checkin_handle_transit(&mut self) -> EgResult<()> {
        log::info!("{self} attempting to receive transit");

        let transit = self.transit.as_ref().unwrap();
        let transit_id = transit.id()?;
        let transit_dest = transit["dest"].int()?;
        let transit_copy_status = transit["copy_status"].int()?;

        let for_hold = transit_copy_status == C::COPY_STATUS_ON_HOLDS_SHELF;
        let suppress_transit = self.should_suppress_transit(transit_dest, for_hold)?;

        if for_hold && suppress_transit {
            self.set_option_true("fake_hold_dest");
        }

        self.hold_transit = self.editor().retrieve("ahtc", transit_id)?;

        if let Some(ht) = self.hold_transit.as_ref() {
            let hold_id = ht["hold"].clone();
            // A hold transit can have a null "hold" value if the linked
            // hold was anonymized while in transit.
            if !ht["hold"].is_null() {
                self.hold = self.editor().retrieve("ahr", hold_id)?;
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
        let mut transit = self.transit.take().unwrap();
        transit["dest_recv_time"] = EgValue::from("now");
        self.editor().update(transit)?;

        // Refresh our copy of the transit.
        self.transit = self.editor().retrieve("atc", transit_id)?;

        // Apply the destination copy status.
        self.update_copy(eg::hash! {"status": transit_copy_status})?;

        if self.hold.is_some() {
            self.put_hold_on_shelf()?;
        } else {
            self.hold_transit = None;
            self.reshelve_copy(true)?;
            self.clear_option("fake_hold_dest");
        }

        let mut payload = eg::hash! {
            transit: self.transit.as_ref().unwrap().clone()
        };

        if let Some(ht) = self.hold_transit.as_ref() {
            payload["holdtransit"] = ht.clone();
        }

        let mut evt = EgEvent::success();
        evt.set_payload(payload);
        evt.set_ad_hoc_value("ishold", EgValue::from(self.hold.is_some()));

        self.add_event(evt);

        Ok(())
    }

    /// This handles standard hold transits as well as items
    /// that transited here w/o a hold transit yet are in
    /// fact captured for a hold.
    fn checkin_handle_received_hold(&mut self) -> EgResult<()> {
        if self.hold_transit.is_none() && self.copy_status() != C::COPY_STATUS_ON_HOLDS_SHELF {
            // No hold transit and not headed for the holds shelf.
            return Ok(());
        }

        let copy_id = self.copy_id;

        let mut alt_hold;
        let hold = match self.hold.as_mut() {
            Some(h) => h,
            None => match holds::captured_hold_for_copy(self.editor(), copy_id)? {
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
            self.update_copy(eg::hash! {status: C::COPY_STATUS_CATALOGING})?;
            self.clear_option("fake_hold_dest");
            // no further processing needed.
            self.set_option_true("noop");

            let mut hold = self.hold.take().unwrap();
            let hold_id = hold.id()?;
            hold["fulfillment_time"] = EgValue::from("now");
            self.editor().update(hold)?;

            self.hold = self.editor().retrieve("ahr", hold_id)?;

            return Ok(());
        }

        if self.get_option_bool("fake_hold_dest") {
            let hold = self.hold.as_mut().unwrap();
            // Perl code does not update the hold in the database
            // at this point.  Doing same.
            hold["pickup_lib"] = EgValue::from(self.circ_lib);

            return Ok(());
        }

        Ok(())
    }

    /// Returns true if transits should be supressed between "here" and
    /// the provided destination.
    ///
    /// * `for_hold` - true if this would be a hold transit.
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

        // json::* knows if two EgValue's are the same.
        if suppress_for_here != suppress_for_dest {
            return Ok(false);
        }

        Ok(true)
    }

    /// Set hold shelf values and update the hold.
    fn put_hold_on_shelf(&mut self) -> EgResult<()> {
        let mut hold = self.hold.take().unwrap();
        let hold_id = hold.id()?;

        hold["shelf_time"] = EgValue::from("now");
        hold["current_shelf_lib"] = EgValue::from(self.circ_lib);

        if let Some(date) = holds::calc_hold_shelf_expire_time(self.editor(), &hold, None)? {
            hold["shelf_expire_time"] = EgValue::from(date);
        }

        self.editor().update(hold)?;
        self.hold = self.editor().retrieve("ahr", hold_id)?;

        Ok(())
    }

    /// Attempt to capture our item for a hold or reservation.
    fn try_to_capture(&mut self) -> EgResult<bool> {
        if self.get_option_bool("remote_hold") {
            return Ok(false);
        }

        if !self.is_booking_enabled() {
            return Ok(self.attempt_checkin_hold_capture()?);
        }

        // XXX this would be notably faster if we didn't first check
        // for both hold and reservation capturability, i.e. if one
        // automatically took precedence.  As is, the capture logic,
        // which can be slow, has to run at minimum 3 times.
        let maybe_hold = self.hold_capture_is_possible()?;
        let maybe_resv = self.reservation_capture_is_possible()?;

        if let Some(hold) = maybe_hold {
            if let Some(resv) = maybe_resv {
                // Hold and reservation == conflict.
                let mut evt = EgEvent::new("HOLD_RESERVATION_CONFLICT");
                evt.set_ad_hoc_value("hold", hold);
                evt.set_ad_hoc_value("reservation", resv);
                self.exit_err_on_event(evt)?;
                Ok(false)
            } else {
                // Hold but no reservation
                self.attempt_checkin_hold_capture()
            }
        } else if maybe_resv.is_some() {
            // Reservation, but no hold.
            self.attempt_checkin_reservation_capture()
        } else {
            // No nuthin
            Ok(false)
        }
    }

    /// Try to capture our item for a hold.
    fn attempt_checkin_hold_capture(&mut self) -> EgResult<bool> {
        if self.capture_state() == "nocapture" {
            return Ok(false);
        }

        let copy_id = self.copy_id;

        let maybe_found = holds::find_nearest_permitted_hold(self.editor(), copy_id, false)?;

        let (mut hold, retarget) = match maybe_found {
            Some(info) => info,
            None => {
                log::info!("{self} no permitted holds found for copy");
                return Ok(false);
            }
        };

        if self.capture_state() != "capture" {
            // See if this item is in a hold-capture-verify location.
            if self.copy()["location"]["hold_verify"].boolish() {
                let mut evt = EgEvent::new("HOLD_CAPTURE_DELAYED");
                evt.set_ad_hoc_value("copy_location", self.copy()["location"].clone());
                self.exit_err_on_event(evt)?;
            }
        }

        if retarget.len() > 0 {
            self.retarget_holds = Some(retarget);
        }

        let pickup_lib = hold["pickup_lib"].int()?;
        let suppress_transit = self.should_suppress_transit(pickup_lib, true)?;

        hold["hopeless_date"].take();
        hold["current_copy"] = EgValue::from(self.copy_id);
        hold["capture_time"] = EgValue::from("now");

        // Clear some other potential cruft
        hold["fulfillment_time"].take();
        hold["fulfillment_staff"].take();
        hold["fulfillment_lib"].take();
        hold["expire_time"].take();
        hold["cancel_time"].take();

        if suppress_transit
            || (pickup_lib == self.circ_lib && !self.get_option_bool("hold_as_transit"))
        {
            self.hold = Some(hold);
            // This updates and refreshes the hold.
            self.put_hold_on_shelf()?;
        } else {
            let hold_id = hold.id()?;
            self.editor().update(hold)?;
            self.hold = self.editor().retrieve("ahr", hold_id)?;
        }

        Ok(true)
    }

    fn attempt_checkin_reservation_capture(&mut self) -> EgResult<bool> {
        if self.capture_state() == "nocapture" {
            return Ok(false);
        }

        let params = vec![
            EgValue::from(self.editor().authtoken()),
            self.copy()["barcode"].clone(),
            EgValue::from(true), // Avoid updating the copy.
        ];

        let result = self.editor().client_mut().send_recv_one(
            "open-ils.booking",
            "open-ils.booking.resources.capture_for_reservation",
            params,
        )?;

        let resp = result
            .ok_or_else(|| EgError::Debug(format!("Booking capture failed to return event")))?;

        let mut evt = EgEvent::parse(&resp)
            .ok_or_else(|| EgError::Debug(format!("Booking capture failed to return event")))?;

        if evt.textcode() == "RESERVATION_NOT_FOUND" {
            if let Some(cause) = evt.payload()["fail_cause"].as_str() {
                if cause == "not-transferable" {
                    log::warn!(
                        "{self} reservation capture attempted against non-transferable item"
                    );
                    self.add_event(evt);
                    return Ok(false);
                }
            }
        }

        if !evt.is_success() {
            // Other non-success events are simply treated as non-captures.
            return Ok(false);
        }

        log::info!("{self} booking capture succeeded");

        if let Ok(stat) = evt.payload()["new_copy_status"].int() {
            self.update_copy(eg::hash! {"status": stat})?;
        }

        let reservation = evt.payload_mut()["reservation"].take();
        if reservation.is_object() {
            self.reservation = Some(reservation);
        }

        let transit = evt.payload_mut()["transit"].take();
        if transit.is_object() {
            let mut e = EgEvent::new("ROUTE_ITEM");
            e.set_org(transit["dest"].int()?);
            self.add_event(e);
        }

        Ok(true)
    }

    /// Returns a hold object if one is found which may be suitable
    /// for capturing our item.
    fn hold_capture_is_possible(&mut self) -> EgResult<Option<EgValue>> {
        if self.capture_state() == "nocapture" {
            return Ok(None);
        }

        let copy_id = self.copy_id;
        let maybe_found =
            holds::find_nearest_permitted_hold(self.editor(), copy_id, true /* check only */)?;

        let (hold, retarget) = match maybe_found {
            Some(info) => info,
            None => {
                log::info!("{self} no permitted holds found for copy");
                return Ok(None);
            }
        };

        if retarget.len() > 0 {
            self.retarget_holds = Some(retarget);
        }

        Ok(Some(hold))
    }

    /// Returns a reservation object if one is found which may be suitable
    /// for capturing our item.
    fn reservation_capture_is_possible(&mut self) -> EgResult<Option<EgValue>> {
        if self.capture_state() == "nocapture" {
            return Ok(None);
        }

        let params = vec![
            EgValue::from(self.editor().authtoken()),
            self.copy()["barcode"].clone(),
        ];

        let result = self.editor().client_mut().send_recv_one(
            "open-ils.booking",
            "open-ils.booking.reservations.could_capture",
            params,
        )?;

        if let Some(resp) = result {
            if let Some(evt) = EgEvent::parse(&resp) {
                self.exit_err_on_event(evt)?;
            } else {
                return Ok(Some(resp));
            }
        }
        return Ok(None);
    }

    /// Determines if our item needs to transit somewhere else and
    /// builds the needed transit.
    fn try_to_transit(&mut self) -> EgResult<()> {
        let mut dest_lib = self.copy_circ_lib();

        let mut has_remote_hold = false;
        if let Some(hold) = self.options.get("remote_hold") {
            has_remote_hold = true;
            if let Ok(pl) = hold["pickup_lib"].int() {
                dest_lib = pl;
            }
        }

        let suppress_transit = self.should_suppress_transit(dest_lib, false)?;
        let hold_as_transit = self.get_option_bool("hold_as_transit");

        if suppress_transit || (dest_lib == self.circ_lib && !(has_remote_hold && hold_as_transit))
        {
            // Copy is where it needs to be, either for hold or reshelving.
            return self.checkin_handle_precat();
        }

        let can_float = self.get_option_bool("can_float");
        let manual_float =
            self.get_option_bool("manual_float") || self.copy()["floating"]["manual"].boolish();

        if can_float && manual_float && !has_remote_hold {
            // Copy is floating -- make it stick here
            self.update_copy(eg::hash! {"circ_lib": self.circ_lib})?;
            return Ok(());
        }

        // Copy needs to transit home
        self.checkin_build_copy_transit(dest_lib)?;
        let mut evt = EgEvent::new("ROUTE_ITEM");
        evt.set_org(dest_lib);
        self.add_event(evt);

        Ok(())
    }

    /// Set the item status to Cataloging and let the caller know
    /// it's a pre-cat item.
    fn checkin_handle_precat(&mut self) -> EgResult<()> {
        if !self.is_precat_copy() {
            return Ok(());
        }

        if self.copy_status() != C::COPY_STATUS_CATALOGING {
            return Ok(());
        }

        self.add_event_code("ITEM_NOT_CATALOGED");

        self.update_copy(eg::hash! {"status": C::COPY_STATUS_CATALOGING})
            .map(|_| ())
    }

    /// Create the actual transit object dn set our item as in-transit.
    fn checkin_build_copy_transit(&mut self, dest_lib: i64) -> EgResult<()> {
        let mut transit = eg::hash! {
            "source": self.circ_lib,
            "dest": dest_lib,
            "target_copy": self.copy_id,
            "source_send_time": "now",
            "copy_status": self.copy_status(),
        };

        // If we are "transiting" an item to the holds shelf,
        // it's a hold transit.
        let maybe_remote_hold = self.options.get("remote_hold");
        let has_remote_hold = maybe_remote_hold.is_some();

        if let Some(hold) = maybe_remote_hold.as_ref() {
            transit["hold"] = hold["id"].clone();

            // Hold is transiting, clear any shelf-iness.
            if !hold["current_shelf_lib"].is_null() || !hold["shelf_time"].is_null() {
                let mut h = (*hold).clone();
                h["current_shelf_lib"].take();
                h["shelf_time"].take();
                self.editor().update(h)?;
            }
        }

        log::info!("{self} transiting copy to {dest_lib}");

        if has_remote_hold {
            let t = EgValue::create("ahtc", transit)?;
            let t = self.editor().create(t)?;
            self.hold_transit = self.editor().retrieve("ahtc", t["id"].clone())?;
        } else {
            let t = EgValue::create("atc", transit)?;
            let t = self.editor().create(t)?;
            self.transit = self.editor().retrieve("ahtc", t["id"].clone())?;
        }

        self.update_copy(eg::hash! {"status": C::COPY_STATUS_IN_TRANSIT})?;
        Ok(())
    }

    /// Maybe void overdues and verify the transaction has the correct
    /// open/closed state.
    fn finish_fines_and_voiding(&mut self) -> EgResult<()> {
        let void_overdues = self.get_option_bool("void_overdues");
        let mut backdate_maybe = match self.options.get("backate") {
            Some(bd) => bd.as_str().map(|d| d.to_string()),
            None => None,
        };

        let circ_id = match self.circ.as_ref() {
            Some(c) => c.id()?,
            None => return Ok(()),
        };

        if !void_overdues && backdate_maybe.is_none() {
            return Ok(());
        }

        let mut note_maybe = None;

        if void_overdues {
            note_maybe = Some("System: Amnesty Checkin");
            backdate_maybe = None;
        }

        billing::void_or_zero_overdues(
            self.editor(),
            circ_id,
            backdate_maybe.as_deref(),
            note_maybe,
            false,
            false,
        )?;

        billing::check_open_xact(self.editor(), circ_id)
    }

    /// This assumes the caller is finished with all processing and makes
    /// changes to local copies if data (e.g. setting copy = None for editing).
    fn flesh_checkin_events(&mut self) -> EgResult<()> {
        let mut copy = self.copy.take().unwrap().take(); // assumes copy
        let copy_id = self.copy_id;
        let record_id = copy["call_number"]["record"].int()?;

        // Grab the volume before it's de-fleshed.
        let volume = copy["call_number"].take();
        copy["call_number"] = volume["id"].clone();

        // De-flesh the copy
        copy.deflesh()?;

        let mut payload = eg::hash! {
            "copy": copy,
            "volume": volume,
        };

        if !self.is_precat_copy() {
            if let Some(rec) = self.editor().retrieve("rmsr", record_id)? {
                payload["title"] = rec;
            }
        }

        if let Some(mut hold) = self.hold.take() {
            if hold["cancel_time"].is_null() {
                hold["notes"] = EgValue::from(
                    self.editor()
                        .search("ahrn", eg::hash! {hold: hold["id"].clone()})?,
                );
                payload["hold"] = hold;
            }
        }

        if let Some(circ) = self.circ.as_ref() {
            let flesh = eg::hash! {
                "flesh": 1,
                "flesh_fields": {
                    "circ": ["billable_transaction"],
                    "mbt": ["summary"]
                }
            };

            let circ_id = circ["id"].clone();

            if let Some(fcirc) = self.editor().retrieve_with_ops("circ", circ_id, flesh)? {
                payload["circ"] = fcirc;
            }
        }

        if let Some(patron) = self.patron.as_ref() {
            let flesh = eg::hash! {
                "flesh": 1,
                "flesh_fields": {
                    "au": ["card", "billing_address", "mailing_address"]
                }
            };

            let patron_id = patron["id"].clone();

            if let Some(fpatron) = self.editor().retrieve_with_ops("au", patron_id, flesh)? {
                payload["patron"] = fpatron;
            }
        }

        if let Some(reservation) = self.reservation.take() {
            payload["reservation"] = reservation;
        }

        if let Some(transit) = self.hold_transit.take().or(self.transit.take()) {
            payload["transit"] = transit;
        }

        let query = eg::hash! {"copy": copy_id};
        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": {
                "alci": ["inventory_workstation"]
            }
        };

        if let Some(inventory) = self.editor().search_with_ops("alci", query, flesh)?.pop() {
            payload["copy"]["latest_inventory"] = inventory;
        }

        // Should never happen, but to be safe:
        if self.events.len() == 0 {
            self.events.push(EgEvent::new("NO_CHANGE"));
        }

        // Clone the payload into any additional events for full coverage.
        for (idx, evt) in self.events.iter_mut().enumerate() {
            if idx > 0 {
                evt.set_payload(payload.clone());
            }
        }

        // Capture the uncloned payload into the first event (which will
        // always be present).
        self.events[0].set_payload(payload);

        Ok(())
    }

    /// Returns true if a hold revert was requested but it does
    /// not make sense with the data we have.
    pub fn hold_revert_sanity_checks(&mut self) -> EgResult<()> {
        if !self.get_option_bool("revert_hold_fulfillment") {
            return Ok(());
        }

        if self.circ.is_some()
            && self.copy.is_some()
            && self.copy_status() == C::COPY_STATUS_CHECKED_OUT
            && self.patron.is_some()
            && !self.is_renewal()
        {
            return Ok(());
        }

        log::warn!("{self} hold-revert requested but makes no sense");

        // Return an inocuous event response to avoid spooking
        // SIP clients -- also, it's true.
        Err(EgEvent::new("NO_CHANGE").into())
    }

    /// Returns true if a hold fulfillment was reverted.
    fn revert_hold_fulfillment(&mut self) -> EgResult<bool> {
        if !self.get_option_bool("revert_hold_fulfillment") {
            return Ok(false);
        }

        let query = eg::hash! {
            "usr": self.patron.as_ref().unwrap()["id"].clone(),
            "cancel_time": EgValue::Null,
            "fulfillment_time": {"!=": EgValue::Null},
            "current_copy": self.copy()["id"].clone(),
        };

        let ops = eg::hash! {
            "order_by": {
                "ahr": "fulfillment_time desc"
            },
            "limit": 1
        };

        let mut hold = match self.editor().search_with_ops("ahr", query, ops)?.pop() {
            Some(h) => h,
            None => return Ok(false),
        };

        // The hold fulfillment time will match the xact_start time of
        // its companion circulation.
        let xact_date =
            date::parse_datetime(self.circ.as_ref().unwrap()["xact_start"].as_str().unwrap())?;

        let ff_date = date::parse_datetime(
            self.hold.as_ref().unwrap()["fulfillment_time"]
                .as_str()
                .unwrap(),
        )?;

        // In some cases the date stored in PG contains milliseconds and
        // in other cases not. To make an accurate comparison, truncate
        // to seconds.
        if xact_date.timestamp() != ff_date.timestamp() {
            return Ok(false);
        }

        log::info!("{self} undoing fulfillment for hold {}", hold["id"]);

        hold["fulfillment_time"].take();
        hold["fulfillment_staff"].take();
        hold["fulfillment_lib"].take();

        self.editor().update(hold)?;

        self.update_copy(eg::hash! {"status": C::COPY_STATUS_ON_HOLDS_SHELF})?;

        Ok(true)
    }
}
