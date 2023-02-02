use super::item;
use super::session::Session;
use chrono::NaiveDateTime;
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
            "00" => Self::Unknown,
            "01" => Self::LocalHold,
            "02" => Self::RemoteHold,
            "03" => Self::Ill,
            "04" => Self::Transit,
            "99" => Self::Other,
            _ => panic!("Unknown alert type: {}", v),
        }
    }
}

impl From<AlertType> for &str {
    fn from(a: AlertType) -> &'static str {
        match a {
            AlertType::Unknown => "00",
            AlertType::LocalHold => "01",
            AlertType::RemoteHold => "02",
            AlertType::Ill => "03",
            AlertType::Transit => "04",
            AlertType::Other => "99",
        }
    }
}

pub struct CheckinResult {
    ok: bool,
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

        log::info!("{self} Checking in item {barcode}");

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

        let mut resp = sip2::Message::from_values(
            &sip2::spec::M_LOGIN_RESP,
            &[
                sip2::util::num_bool(result.ok),                   // checkin ok
                sip2::util::sip_bool(!item.magnetic_media),        // resensitize
                sip2::util::sip_bool(item.magnetic_media),         // magnetic
                sip2::util::sip_bool(result.alert_type.is_some()), // alert
                &sip2::util::sip_date_now(),
            ],
            &[
                ("AB", &barcode),
                ("AO", self.account().settings().institution()),
                ("AJ", &item.title),
                ("AP", &result.current_loc),
                ("AQ", &result.permanent_loc),
                ("BG", &item.owning_loc),
                ("BT", &item.fee_type),
                ("CI", sip2::util::num_bool(false)), // security inhibit
            ],
        )
        .unwrap();

        if let Some(ref bc) = result.patron_barcode {
            resp.add_field("AA", bc);
        }
        if let Some(at) = result.alert_type {
            resp.add_field("CV", at.into());
        }
        if let Some(ref loc) = result.destination_loc {
            resp.add_field("CT", loc);
        }
        if let Some(ref bc) = result.hold_patron_barcode {
            resp.add_field("CY", bc);
        }
        if let Some(ref n) = result.hold_patron_name {
            resp.add_field("DA", n);
        }

        Ok(resp)
    }

    fn return_checkin_item_not_found(&self, barcode: &str) -> sip2::Message {
        sip2::Message::from_values(
            &sip2::spec::M_CHECKIN_RESP,
            &[
                sip2::util::num_bool(false), // checkin ok
                sip2::util::sip_bool(false), // resensitize
                sip2::util::sip_bool(false), // magnetic
                sip2::util::sip_bool(false), // alert
                &sip2::util::sip_date_now(),
            ],
            &[
                ("AB", &barcode),
                ("AO", self.account().settings().institution()),
                ("CV", AlertType::Unknown.into()),
            ],
        )
        .unwrap()
    }

    fn checkin(
        &mut self,
        item: &item::Item,
        current_loc_op: &Option<String>,
        return_date: &str,
        cancel: bool,
        ovride: bool,
    ) -> Result<CheckinResult, String> {
        let mut args = json::object! {
            copy_barcode: item.barcode.as_str(),
            hold_as_transit: self.account().settings().checkin_holds_as_transits(),
        };

        if cancel {
            args["revert_hold_fulfillment"] = json::from(cancel);
        }

        if return_date.trim().len() == 18 {
            let fmt = sip2::spec::SIP_DATE_FORMAT;

            // Use NaiveDate since SIP dates don't typically include a
            // time zone value.
            if let Some(sip_date) = NaiveDateTime::parse_from_str(return_date, fmt).ok() {
                let iso_date = sip_date.format("%Y-%m-%d").to_string();
                log::info!("{self} Checking in with backdate: {iso_date}");

                args["backdate"] = json::from(iso_date);
            } else {
                log::warn!("{self} Invalid checkin return date: {return_date}");
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

        let resp = match self
            .osrf_client_mut()
            .sendrecvone("open-ils.circ", method, params)?
        {
            Some(r) => r,
            None => Err(format!("API call {method} failed to return a response"))?,
        };

        log::debug!("{self} Checkin of {} returned: {resp}", item.barcode);

        let evt_json = match resp {
            json::JsonValue::Array(list) => {
                if list.len() > 0 {
                    list[0].to_owned()
                } else {
                    json::JsonValue::Null
                }
            }
            _ => resp,
        };

        let evt = eg::event::EgEvent::parse(&evt_json)
            .ok_or(format!("API call {method} failed to return an event"))?;

        if !ovride
            && self
                .account()
                .settings()
                .checkin_override()
                .contains(&evt.textcode().to_string())
        {
            return self.checkin(item, current_loc_op, return_date, cancel, true);
        }

        let mut current_loc = item.current_loc.to_string(); // item.circ_lib
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

            log::debug!("{self} Checkin of {} returned a copy object", item.barcode);

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
            log::debug!(
                "{self} Checkin of {} returned a circulation object",
                item.barcode
            );

            if let Some(user) = self.get_user_and_card(self.parse_id(&circ["usr"])?)? {
                if let Some(bc) = user["card"]["barcode"].as_str() {
                    result.patron_barcode = Some(bc.to_string());
                }
            }
        }

        self.handle_hold(&evt, &mut result)?;

        if evt.textcode().eq("SUCCESS") || evt.textcode().eq("NO_CHANGE") {
            result.ok = true;
        } else if evt.textcode().eq("ROUTE_ITEM") {
            result.ok = true;
            if result.alert_type.is_none() {
                result.alert_type = Some(AlertType::Transit);
            }
        } else {
            result.ok = false;
            if result.alert_type.is_none() {
                result.alert_type = Some(AlertType::Unknown);
            }
        }

        Ok(result)
    }

    /// See if checkin resulted in a hold capture and collect
    /// related info.
    fn handle_hold(
        &mut self,
        evt: &eg::event::EgEvent,
        result: &mut CheckinResult,
    ) -> Result<(), String> {
        let rh = &evt.payload()["remote_hold"];
        let lh = &evt.payload()["hold"];

        let hold = if rh.is_object() {
            rh
        } else if lh.is_object() {
            lh
        } else {
            return Ok(());
        };

        log::debug!("{self} Checkin returned a hold object id={}", hold["id"]);

        if let Some(user) = self.get_user_and_card(self.parse_id(&hold["usr"])?)? {
            result.hold_patron_name = Some(self.format_user_name(&user));
            if let Some(bc) = user["card"]["barcode"].as_str() {
                result.hold_patron_barcode = Some(bc.to_string());
            }
        }

        let pickup_lib_id;
        let pickup_lib = &hold["pickup_lib"];

        // hold pickup lib may or may not be fleshed here.
        if pickup_lib.is_object() {
            result.destination_loc = Some(pickup_lib["shortname"].as_str().unwrap().to_string());
            pickup_lib_id = self.parse_id(&pickup_lib["id"])?;
        } else {
            pickup_lib_id = self.parse_id(&pickup_lib)?;
            result.destination_loc = self.org_sn_from_id(pickup_lib_id)?;
        }

        if pickup_lib_id == self.get_ws_org_id()? {
            result.alert_type = Some(AlertType::LocalHold);
        } else {
            result.alert_type = Some(AlertType::RemoteHold);
        }

        Ok(())
    }
}
