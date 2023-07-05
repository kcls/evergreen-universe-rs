use crate::constants as C;
use crate::util::{json_int, json_bool, json_bool_op, json_string};
use crate::common::circulator::Circulator;
use crate::event::EgEvent;
use crate::date;
use chrono::{Local, Duration};
use json::JsonValue;
use std::collections::HashSet;

const CHECKIN_ORG_SETTINGS: &[&str] = &[
   "circ.transit.min_checkin_interval"
];


impl Circulator {

    pub fn checkin(&mut self) -> Result<(), String> {
        self.action = Some(String::from("checkin"));

        if self.copy.is_none() {
            self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
        }

        if json_bool(&self.copy()["deleted"]) {
            // Never attempt to capture holds with a deleted copy.
            // TODO maybe move this closer to where it matters and/or
            // avoid needing to set the option?
            self.options.insert(String::from("capture"), json::from("nocapture"));
        }

        // Pre-cache some setting values.
        self.settings.fetch_values(CHECKIN_ORG_SETTINGS)?;

        self.fix_broken_transit_status()?;
        self.check_transit_checkin_interval()?;
        self.checkin_retarget_holds()?;

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

        if json_int(&self.copy()["status"]["id"])? != C::EG_COPY_STATUS_IN_TRANSIT as i64 {
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

        if json_int(&self.copy()["status"]["id"])? != C::EG_COPY_STATUS_IN_TRANSIT as i64 {
            // We only care about in-transit items.
            return Ok(());
        }

        let interval = self.settings.get_value("circ.transit.min_checkin_interval")?;

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
            Some(r) => match r.as_str() {Some(s) => s, None => ""},
            None => "",
        };

        if !retarget_mode.contains("retarget") {
            return Ok(());
        }

        let capture = match self.options.get("capture") {
            Some(c) => match c.as_str() {Some(s) => s, None => ""}
            None => "",
        };

        if capture.eq("nocapture") {
            return Ok(());
        }

        let copy = self.copy();
        let copy_id = json_int(&copy["id"])?;

        let is_precat =
            json_bool_op(self.options.get("is_precat")) ||
            json_int(&copy["call_number"])? == -1;

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
        if !retarget_mode.contains(".all") &&
            json_int(&copy["status"]["id"])? != C::EG_COPY_STATUS_IN_PROCESS as i64 {
            return Ok(());
        }

        let query = json::object! {target_copy: json::from(copy_id)};
        let parts = self.editor.search("acpm", query)?;
        let parts = parts.into_iter().map(|p| json_int(&p["id"]).unwrap()).collect::<HashSet<_>>();

        // Get the list of potentially retargetable holds
        // NOTE reporter.hold_request_record does not get updated
        // when items/call numbers are transferred to another call number / record.
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
                query
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

}
