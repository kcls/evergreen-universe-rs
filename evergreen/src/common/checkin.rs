use crate::common::circulator::Circulator;
use crate::constants as C;
use crate::date;
use crate::util::{json_bool, json_bool_op, json_float, json_int, json_string};
use chrono::{Duration, Local};
use json::JsonValue;
use std::collections::HashSet;

const CHECKIN_ORG_SETTINGS: &[&str] = &[
    "circ.transit.min_checkin_interval",
    "circ.transit.suppress_hold",
];

impl Circulator {
    pub fn checkin(&mut self) -> Result<(), String> {
        self.action = Some(String::from("checkin"));

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

        self.load_copy_alerts(&["CHECKOUT"])?;

        Ok(())
    }

    fn basic_copy_checks(&mut self) -> Result<(), String> {
        if self.copy.is_none() {
            self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
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
    fn fix_broken_transit_status(&mut self) -> Result<(), String> {
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

        if json_int(&self.copy()["status"]["id"])? != C::EG_COPY_STATUS_IN_TRANSIT {
            log::warn!("{self} Copy has an open transit, but incorrect status");
            let changes = json::object! {status: C::EG_COPY_STATUS_IN_TRANSIT};
            self.update_copy(changes)?;
        }

        self.transit = Some(transit.to_owned());

        Ok(())
    }

    /// If a copy goes into transit and is then checked in before the
    /// transit checkin interval has expired, push an event onto the
    /// overridable events list.
    fn check_transit_checkin_interval(&mut self) -> Result<(), String> {
        if json_int(&self.copy()["status"]["id"])? != C::EG_COPY_STATUS_IN_TRANSIT {
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
    fn checkin_retarget_holds(&mut self) -> Result<(), String> {
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
        if !retarget_mode.contains(".all")
            && json_int(&copy["status"]["id"])? != C::EG_COPY_STATUS_IN_PROCESS
        {
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
    fn cancel_transit_if_circ_exists(&mut self) -> Result<(), String> {
        if self.open_circ.is_none() {
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
    fn set_dont_change_lost_zero(&mut self) -> Result<(), String> {
        let copy_status = json_int(&self.copy()["status"]["id"])?;

        match copy_status {
            C::EG_COPY_STATUS_LOST
            | C::EG_COPY_STATUS_LOST_AND_PAID
            | C::EG_COPY_STATUS_LONG_OVERDUE => {
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

            if let Some(circ) = self.open_circ.as_ref() {
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
    fn set_can_float(&mut self) -> Result<(), String> {
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

    fn do_inventory_update(&mut self) -> Result<(), String> {
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

        self.changes_applied = true;

        self.editor.create(&aci).map(|_| ()) // don't need the result.
    }

    fn check_is_on_holds_shelf(&mut self) -> Result<bool, String> {
        if json_int(&self.copy()["status"]["id"])? != C::EG_COPY_STATUS_ON_HOLDS_SHELF {
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

        // What hold are we on the shelf for?
        let query = json::object! {
            current_copy: copy_id,
            capture_time: {"!=": JsonValue::Null},
            fulfillment_time: JsonValue::Null,
            cancel_time: JsonValue::Null,
        };

        let holds = self.editor.search("ahr", query)?;
        if holds.len() == 0 {
            log::warn!("{self} Copy on holds shelf but there is no hold");
            self.reshelve_copy(false)?;
            return Ok(false);
        }

        let hold = holds[0].to_owned();
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

    fn reshelve_copy(&mut self, force: bool) -> Result<(), String> {
        let force = force || self.get_option_bool("force");

        let status = json_int(&self.copy()["status"]["id"])?;

        let next_status = match self.options.get("next_copy_status") {
            Some(s) => json_int(&s)?,
            None => C::EG_COPY_STATUS_RESHELVING,
        };

        if force
            || (status != C::EG_COPY_STATUS_ON_HOLDS_SHELF
                && status != C::EG_COPY_STATUS_CATALOGING
                && status != C::EG_COPY_STATUS_IN_TRANSIT
                && status != next_status)
        {
            self.update_copy(json::object! {status: json::from(next_status)})?;
            self.changes_applied = true;
        }

        Ok(())
    }
}
