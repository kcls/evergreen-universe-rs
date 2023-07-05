use crate::constants as C;
use crate::util::{json_int, json_bool};
use crate::common::circulator::Circulator;
use crate::event::EgEvent;
use json::JsonValue;

const CHECKIN_ORG_SETTINGS: &[&str] = &[
   "circ.transit.min_checkin_interval"
];


impl Circulator {

    pub fn checkin(&mut self) -> Result<(), String> {
        if self.copy.is_none() {
            self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
        }

        if json_bool(&self.copy()["deleted"]) {
            // Never attempt to capture holds with a deleted copy.
            self.options.insert(String::from("capture"), json::from("nocapture"));
        }

        // Pre-cache some setting values.
        self.settings.fetch_values(CHECKIN_ORG_SETTINGS)?;

        self.fix_broken_transit_status()?;
        self.check_transit_checkin_interval()?;

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

        if json_int(&self.copy()["status"])? != C::EG_COPY_STATUS_IN_TRANSIT as i64 {
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

        if json_int(&self.copy()["status"])? != C::EG_COPY_STATUS_IN_TRANSIT as i64 {
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

        Ok(())
    }
}
