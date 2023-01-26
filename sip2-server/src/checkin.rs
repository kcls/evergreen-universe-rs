use super::session::Session;
use super::item;
use chrono::prelude::*;
use chrono::DateTime;
use evergreen as eg;

pub enum AlertType {
    Unknown,
    LocalHold,
    RemoteHold,
    Ill,
    Transit,
    Other,
}

impl From<&str> for AlertType {
    fn from(v: &str) -> AlertType {
        match v {
            "01" => Self::LocalHold,
            "02" => Self::RemoteHold,
            "03" => Self::Ill,
            "04" => Self::Transit,
            "99" => Self::Other,
            _ => Self::Unknown
        }
    }
}

pub struct CheckinResult {
    ok: bool,
    alert: bool,
    current_loc: String,
    permanent_loc: String,
    destination_loc: Option<String>,
    patron_barcode: Option<String>,
    alert_type: Option<AlertType>,
    hold_patron_name: Option<String>,
    hold_patron_barcode: Option<String>,
}

impl Session {

    pub fn handle_checkin(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        self.set_authtoken()?;

        let barcode = msg
            .get_field_value("AB")
            .ok_or(format!("handle_item_info() missing item barcode"))?;

        let current_loc_op = msg.get_field_value("AP");
        let return_date = &msg.fixed_fields()[2];

        // KCLS only
        // cancel == un-fulfill hold this copy currently fulfills
        let cancel_op = msg.get_field_value("BI");

        log::info!("Checking in item {barcode}");

        let item = match self.get_item_details(&barcode)? {
            Some(c) => c,
            None => {
                return Ok(self.return_checkin_item_not_found(&barcode));
            }
        };

        let result = self.checkin(
            &item,
            &current_loc_op,
            return_date.value(),
            cancel_op.is_some(),
            self.account().settings().checkin_override_all(),
        )?;

        todo!()
    }

    fn return_checkin_item_not_found(&self, barcode: &str) -> sip2::Message {

        let mut resp = sip2::Message::new(
            &sip2::spec::M_CHECKIN_RESP,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_CHECKIN_OK, "0").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_RESENSITIZE, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_MAGNETIC_MEDIA, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_ALERT, "N").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("AB", &barcode);
        resp.add_field("AO", self.account().settings().institution());
        resp.add_field("CV", "00"); // unkown alert type

        resp
    }

    fn checkin(
        &mut self,
        item: &item::Item,
        current_loc_op: &Option<String>,
        return_date: &str,
        cancel: bool,
        ovride: bool
    ) -> Result<CheckinResult, String> {

        let mut args = json::object! {
            copy_barcode: item.barcode.as_str(),
            hold_as_transit: self.account().settings().checkin_holds_as_transits(),
        };

        if cancel {
            args["revert_hold_fulfillment"] = json::from(cancel);
        }

        if return_date.trim().len() > 0 {
            // SIP return date is YYYYMMDD

            if let Some(sip_date) = DateTime::parse_from_str(return_date, "%Y%m%d").ok() {
                let iso_date = sip_date.format("%Y-%m-%d").to_string();
                log::info!("Checking in with backdate: {iso_date}");

                args["backdate"] = json::from(iso_date);

            } else {
                log::warn!("Invalid checkin return date: {return_date}");
            }
        }

        if let Some(sn) = current_loc_op {
            if let Some(org_id) = self.org_id_from_sn(sn)? {
                args["circ_lib"] = json::from(org_id);
            }
        }

        if !args.has_key("circ_lib") {
            args["circ_lib"] = json::from(self.get_ws_org_id()?);
        }

        let method = match ovride {
            true => "open-ils.circ.checkin.override",
            false => "open-ils.circ.checkin",
        };

        let params = vec![json::from(self.authtoken()?), args];

        let resp = match
            self.osrf_client_mut().sendrecvone("open-ils.circ", method, params)? {
            Some(r) => r,
            None => Err(format!("API call {method} failed to return a response"))?,
        };

        let evt_json = match resp {
            json::JsonValue::Array(list) => list[0].to_owned(),
            _ => resp
        };

        let evt = eg::event::EgEvent::parse(&evt_json)
            .ok_or(format!("API call {method} failed to return an event"))?;

        if !ovride &&
            self.account().settings().checkin_override().contains(&evt.textcode().to_string()) {
            return self.checkin(item, current_loc_op, return_date, cancel, true);
        }

        let mut current_loc = item.current_loc.to_string();     // item.circ_lib
        let mut permanent_loc = item.permanent_loc.to_string(); // item.circ_lib
        let mut destination_loc = None;
        if let Some(org_id) = evt.org() {
            destination_loc = self.org_sn_from_id(*org_id)?;
        }

        let copy = &evt.payload()["copy"];
        if copy.is_object() {
            // If the API returned a copy, collect data about the copy
            // for our response.  It could mean the copy's circ lib
            // changed because it floats.

            if let Ok(circ_lib) = self.parse_id(&copy["circ_lib"]) {
                if circ_lib != item.circ_lib {
                    if let Some(loc) = self.org_sn_from_id(circ_lib)? {
                        current_loc = loc.to_string();
                        permanent_loc = loc.to_string();
                    }
                }
            }
        }

        let mut result = CheckinResult {
            ok: false,
            alert: false,
            current_loc,
            permanent_loc,
            destination_loc,
            patron_barcode: None,
            alert_type: None,
            hold_patron_name: None,
            hold_patron_barcode: None,
        };

        let circ = &evt.payload()["circ"];
        if circ.is_object() {
            // If we checked in a circ, collect some data about it.

            let ops = json::object! {
                flesh: 1,
                flesh_fields: {au: ["card"]}
            };

            let user_id = self.parse_id(&circ["usr"])?;
            let user = self.editor_mut().retrieve_with_ops("au", user_id, ops)?
                .expect("Circ user should be defined");

            // Technically possible for barcode value to be null.
            if let Some(bc) = user["card"]["barcode"].as_str() {
                result.patron_barcode = Some(bc.to_string());
            }
        }

        self.handle_hold(&evt, &mut result)?;

        // MORE TODO HERE

        Ok(result)
    }

    fn handle_hold(
        &mut self,
        evt: &eg::event::EgEvent,
        result: &mut CheckinResult
    ) -> Result<(), String> {

        todo!()

    }
}


