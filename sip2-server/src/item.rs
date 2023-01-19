use super::session::Session;

impl Session {
    pub fn handle_item_info(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        let barcode = msg
            .get_field_value("AB")
            .ok_or(format!("handle_item_info() missing item barcode"))?;

        log::info!("Item Information {barcode}");

        let search = json::object! {
            barcode: barcode.to_string(),
            deleted: "f",
        };

        let flesh = json::object! {
            flesh: 3,
            flesh_fields: {
                acp: ["circ_lib", "call_number",
                    "status", "stat_cat_entry_copy_maps", "circ_modifier"],
                acn: ["owning_lib", "record"],
                bre: ["flat_display_entries"],
                ascecm: ["stat_cat", "stat_cat_entry"],
            }
        };

        let copies = self.editor_mut().search_with_ops("acp", search, flesh)?;

        // Will be zero or one.
        if copies.len() == 0 {
            return Ok(self.return_not_found(&barcode));
        }

        let copy = &copies[0]; // should only be one

        let mut due_date: Option<String> = None;

        if let Ok(Some(circ)) = self.get_copy_circ(copy["id"].as_i64().unwrap()) {
            if let Some(iso_date) = circ["due_date"].as_str() {
                if self
                    .account()
                    .unwrap()
                    .settings()
                    .due_date_use_sip_date_format()
                {
                    due_date = match sip2::util::sip_date(iso_date) {
                        Ok(d) => Some(d),
                        Err(e) => Err(format!("Cannot parse due date: {iso_date} {e}"))?,
                    }
                } else {
                    due_date = Some(iso_date.to_string());
                }
            }
        }

        let circ_lib = copy["circ_lib"]["shortname"].as_str().unwrap(); // required
        let owning_lib = copy["call_number"]["owning_lib"]["shortname"]
            .as_str()
            .unwrap(); // required

        let mut dest_location = circ_lib.to_string();
        let transit_op = self.get_copy_transit(copy)?;

        if let Some(transit) = &transit_op {
            dest_location = transit["dest"]["shortname"].as_str().unwrap().to_string();
        }

        let mut hold_pickup_date_op: Option<String> = None;
        let mut hold_patron_barcode_op: Option<String> = None;
        let mut hold_queue_len = 0;
        if let Some(hold) = self.get_copy_hold(copy, &transit_op)? {
            hold_queue_len = 1; // copying SIPServer

            dest_location = hold["pickup_lib"]["shortname"]
                .as_str()
                .unwrap()
                .to_string();

            if let Some(date) = hold["shelf_expire_time"].as_str() {
                if let Ok(date2) = sip2::util::sip_date(date) {
                    hold_pickup_date_op = Some(date2);
                }
            }

            if let Some(bc) = hold["usr"]["card"]["barcode"].as_str() {
                hold_patron_barcode_op = Some(bc.to_string());
            }
        }

        let deposit_amount = match copy["deposit_amount"].as_f64() {
            Some(a) => a,
            None => match copy["deposit_amount"].as_str() {
                Some(s) => match s.parse::<f64>() {
                    Ok(v) => v,
                    Err(e) => Err(format!(
                        "Invalid deposit amount: {} {e}",
                        copy["deposit_amount"]
                    ))?,
                },
                None => Err(format!(
                    "Unexpected deposit amount: {}",
                    copy["deposit_amount"]
                ))?,
            },
        };

        let mut fee_type = "01";
        if copy["deposit"].as_str().unwrap().eq("f") {
            if deposit_amount > 0.0 {
                fee_type = "06";
            }
        }

        let circ_status = self.circ_status(copy);

        let mut resp = sip2::Message::new(
            &sip2::spec::M_ITEM_INFO_RESP,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_CIRCULATION_STATUS, circ_status).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_SECURITY_MARKER, "02").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_FEE_TYPE, fee_type).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        let media_type = match copy["circ_modifier"]["sip2_media_type"].as_str() {
            Some(t) => t,
            None => "001",
        };

        resp.add_field("AB", &barcode);
        resp.add_field("AJ", self.get_title(copy));
        resp.add_field("AP", circ_lib); // current location
        resp.add_field("AQ", circ_lib); // permanent location
        resp.add_field("BG", owning_lib); // owning location
        resp.add_field("BH", self.sip_config().currency());
        resp.add_field("BV", &format!("{deposit_amount}"));
        resp.add_field("CF", &format!("{hold_queue_len}"));
        resp.add_field("CK", media_type);
        resp.add_field("CT", &dest_location);
        resp.maybe_add_field("CM", hold_pickup_date_op.as_deref());
        resp.maybe_add_field("CY", hold_patron_barcode_op.as_deref());

        if let Some(ref d) = due_date {
            resp.add_field("AH", d);
        }

        Ok(resp)
    }

    fn get_copy_hold(
        &mut self,
        copy: &json::JsonValue,
        transit: &Option<json::JsonValue>,
    ) -> Result<Option<json::JsonValue>, String> {
        let copy_status = copy["status"]["id"].as_i64().unwrap();

        if copy_status != 8 {
            // On Holds Shelf
            if let Some(t) = transit {
                if t["copy_status"].as_i64().unwrap() != 8 {
                    // Copy in transit for non-hold reasons
                    return Ok(None);
                }
            } else {
                // Copy not currently captured / transiting for a hold.
                return Ok(None);
            }
        }

        let copy_id = copy["id"].as_i64().unwrap();

        let search = json::object! {
            current_copy: copy_id,
            capture_time: {"!=": json::JsonValue::Null},
            cancel_time: json::JsonValue::Null,
            fulfillment_time: json::JsonValue::Null,
        };

        let flesh = json::object! {
            limit: 1,
            flesh: 2,
            flesh_fields: {ahr: ["pickup_lib", "usr"], au: ["card"]},
        };

        let holds = self.editor_mut().search_with_ops("ahr", search, flesh)?;

        if holds.len() > 0 {
            Ok(Some(holds[0].to_owned()))
        } else {
            Ok(None)
        }
    }

    fn get_copy_transit(
        &mut self,
        copy: &json::JsonValue,
    ) -> Result<Option<json::JsonValue>, String> {
        let copy_status = copy["status"]["id"].as_i64().unwrap();

        if copy_status != 6 {
            return Ok(None);
        }

        let copy_id = copy["id"].as_i64().unwrap();

        let search = json::object! {
            target_copy: copy_id,
            dest_recv_time: json::JsonValue::Null,
            cancel_time: json::JsonValue::Null,
        };

        let flesh = json::object! {
            flesh: 1,
            flesh_fields: {atc: ["dest"]},
        };

        let transits = self.editor_mut().search_with_ops("atc", search, flesh)?;

        if transits.len() > 0 {
            Ok(Some(transits[0].to_owned()))
        } else {
            Ok(None)
        }
    }

    fn get_title<'a>(&'a self, copy: &'a json::JsonValue) -> &str {
        for entry in copy["call_number"]["record"]["flat_display_entries"].members() {
            if entry["name"].as_str().unwrap().eq("title") {
                return entry["value"].as_str().unwrap();
            }
        }

        ""
    }

    fn circ_status(&self, copy: &json::JsonValue) -> &str {
        // status if fleshed.  status and its id are required.
        let copy_status = copy["status"]["id"].as_i64().unwrap();

        match copy_status {
            9 => "02",      // on order
            0 => "03",      // available
            1 => "04",      // checked out
            5 => "06",      // in process
            8 => "08",      // holds shelf
            7 => "09",      // reshelving
            6 => "10",      // in transit
            3 | 17 => "12", // lost, lost-and-paid
            4 => "13",      // mising
            _ => "01",      // unknown
        }
    }

    /// Returns a basic response with an empty title, which indicates
    /// (to some SIP clients, at least) that the item was not found.
    fn return_not_found(&self, barcode: &str) -> sip2::Message {
        log::debug!("No copy found with barcode: {barcode}");

        let mut resp = sip2::Message::new(
            &sip2::spec::M_ITEM_INFO_RESP,
            vec![
                // circ status unknown
                sip2::FixedField::new(&sip2::spec::FF_CIRCULATION_STATUS, "01").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_SECURITY_MARKER, "01").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_FEE_TYPE, "01").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("AB", &barcode);
        resp.add_field("AJ", "");

        resp
    }

    fn get_copy_circ(&mut self, copy_id: i64) -> Result<Option<json::JsonValue>, String> {
        let search = json::object! {
            target_copy: copy_id,
            checkin_time: json::JsonValue::Null,
            "-or": [
              {stop_fines: json::JsonValue::Null},
              {stop_fines: ["MAXFINES", "LONGOVERDUE"]}
            ]
        };

        let flesh = json::object! {
            flesh: 2,
            flesh_fields: {
                circ: ["usr"],
                au: ["card"],
            }
        };

        let circs = self.editor_mut().search_with_ops("circ", search, flesh)?;

        if circs.len() > 0 {
            Ok(Some(circs[0].to_owned()))
        } else {
            Ok(None)
        }
    }
}
