use super::session::Session;
use eg::constants as C;
use eg::date;
use eg::result::EgResult;
use eg::EgValue;
use evergreen as eg;

/// A copy object with SIP-related data collected and attached.
pub struct Item {
    pub barcode: String,
    pub circ_lib: i64,
    pub due_date: Option<String>,
    pub copy_status: i64,
    pub circ_status: &'static str,
    pub fee_type: &'static str,
    pub title: String,
    pub current_loc: String,
    pub permanent_loc: String,
    pub destination_loc: String,
    pub owning_loc: String,
    pub deposit_amount: f64,
    pub magnetic_media: bool,
    pub hold_queue_length: usize,
    pub media_type: String,
    pub hold_pickup_date: Option<String>,
    pub hold_patron_barcode: Option<String>,
    pub circ_patron_id: Option<i64>,
}

impl Session {
    /// Collect a pile of data for a copy by barcode
    pub fn get_item_details(&mut self, barcode: &str) -> EgResult<Option<Item>> {
        let search = eg::hash! {
            barcode: barcode,
            deleted: "f",
        };

        let flesh = eg::hash! {
            flesh: 3,
            flesh_fields: {
                acp: ["circ_lib", "call_number",
                    "stat_cat_entry_copy_maps", "circ_modifier"],
                acn: ["owning_lib", "record"],
                bre: ["simple_record"],
                ascecm: ["stat_cat", "stat_cat_entry"],
            }
        };

        let copies = self.editor_mut().search_with_ops("acp", search, flesh)?;

        // Will be zero or one.
        if copies.len() == 0 {
            return Ok(None);
        }

        let copy = &copies[0]; // should only be one
        let copy_status = copy["status"].int()?;

        let mut circ_patron_id: Option<i64> = None;
        let mut due_date: Option<String> = None;

        if let Some(circ) = self.get_copy_circ(&copy, copy_status)? {
            circ_patron_id = Some(circ["usr"].int()?);

            if let Some(iso_date) = circ["due_date"].as_str() {
                if self.account().settings().due_date_use_sip_date_format() {
                    let due_dt = date::parse_datetime(iso_date)?;
                    due_date = Some(sip2::util::sip_date_from_dt(&due_dt));
                } else {
                    due_date = Some(iso_date.to_string());
                }
            }
        }

        let circ_lib_id = copy["circ_lib"].id()?;
        let circ_lib = copy["circ_lib"]["shortname"].as_str().unwrap(); // required
        let owning_lib = copy["call_number"]["owning_lib"]["shortname"]
            .as_str()
            .unwrap(); // required

        let mut dest_location = circ_lib.to_string();
        let transit_op = self.get_copy_transit(copy, copy_status)?;

        if let Some(transit) = &transit_op {
            dest_location = transit["dest"]["shortname"].as_str().unwrap().to_string();
        }

        let mut hold_pickup_date_op: Option<String> = None;
        let mut hold_patron_barcode_op: Option<String> = None;
        let mut hold_queue_length = 0;

        if let Some(hold) = self.get_copy_hold(copy, &transit_op, copy_status)? {
            hold_queue_length = 1; // copying SIPServer

            dest_location = hold["pickup_lib"]["shortname"]
                .as_str()
                .unwrap()
                .to_string();

            if let Some(date) = hold["shelf_expire_time"].as_str() {
                let pu_date = date::parse_datetime(date)?;
                hold_pickup_date_op = Some(sip2::util::sip_date_from_dt(&pu_date));
            }

            if let Some(bc) = hold["usr"]["card"]["barcode"].as_str() {
                hold_patron_barcode_op = Some(bc.to_string());
            }
        }

        let deposit_amount = copy["deposit_amount"].float()?;

        let mut fee_type = "01";
        if copy["deposit"].as_str().unwrap().eq("f") {
            if deposit_amount > 0.0 {
                fee_type = "06";
            }
        }

        let circ_status = self.circ_status(copy_status);
        let media_type = copy["circ_modifier"]["sip2_media_type"]
            .as_str()
            .unwrap_or("001");
        let magnetic_media = copy["circ_modifier"]["magnetic_media"].boolish();

        let (title, _) = self.get_copy_title_author(&copy)?;
        let title = title.unwrap_or(String::new());

        Ok(Some(Item {
            barcode: barcode.to_string(),
            due_date,
            title,
            copy_status: copy_status,
            circ_lib: circ_lib_id,
            deposit_amount,
            hold_queue_length,
            magnetic_media,
            fee_type: fee_type,
            circ_status: circ_status,
            current_loc: circ_lib.to_string(),
            permanent_loc: circ_lib.to_string(),
            destination_loc: dest_location,
            owning_loc: owning_lib.to_string(),
            media_type: media_type.to_string(),
            hold_pickup_date: hold_pickup_date_op,
            hold_patron_barcode: hold_patron_barcode_op,
            circ_patron_id,
        }))
    }

    pub fn handle_item_info(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        let barcode = match msg.get_field_value("AB") {
            Some(b) => b,
            None => return Ok(self.return_item_not_found("")),
        };

        log::info!("{self} Item Information {barcode}");

        let item = match self.get_item_details(&barcode)? {
            Some(c) => c,
            None => {
                return Ok(self.return_item_not_found(&barcode));
            }
        };

        let mut resp = sip2::Message::from_values(
            &sip2::spec::M_ITEM_INFO_RESP,
            &[
                item.circ_status,
                "02", // security marker
                &item.fee_type,
                &sip2::util::sip_date_now(),
            ],
            &[
                ("AB", &item.barcode),
                ("AJ", &item.title),
                ("AP", &item.current_loc),
                ("AQ", &item.permanent_loc),
                ("BG", &item.owning_loc),
                ("CT", &item.destination_loc),
                ("BH", self.sip_config().currency()),
                ("BV", &format!("{:.2}", item.deposit_amount)),
                ("CF", &format!("{}", item.hold_queue_length)),
                ("CK", &item.media_type),
            ],
        )
        .unwrap();

        resp.maybe_add_field("CM", item.hold_pickup_date.as_deref());
        resp.maybe_add_field("CY", item.hold_patron_barcode.as_deref());
        resp.maybe_add_field("AH", item.due_date.as_deref());

        Ok(resp)
    }

    /// Find an active hold linked to the copy.  The copy must be on
    /// the holds shelf or in transit to the holds shelf.
    fn get_copy_hold(
        &mut self,
        copy: &EgValue,
        transit: &Option<EgValue>,
        copy_status: i64,
    ) -> EgResult<Option<EgValue>> {
        if copy_status != C::COPY_STATUS_ON_HOLDS_SHELF {
            // On Holds Shelf
            if let Some(t) = transit {
                if t["copy_status"].int()? != C::COPY_STATUS_ON_HOLDS_SHELF {
                    // Copy in transit for non-hold reasons
                    return Ok(None);
                }
            } else {
                // Copy not currently on shelf or transiting for a hold.
                return Ok(None);
            }
        }

        let copy_id = copy.id()?;

        let search = eg::hash! {
            current_copy: copy_id,
            capture_time: {"!=": EgValue::Null},
            cancel_time: EgValue::Null,
            fulfillment_time: EgValue::Null,
        };

        let flesh = eg::hash! {
            limit: 1,
            flesh: 2,
            flesh_fields: {ahr: ["pickup_lib", "usr"], au: ["card"]},
        };

        let mut holds = self.editor_mut().search_with_ops("ahr", search, flesh)?;

        Ok(holds.pop())
    }

    /// Find the active transit for a copy if one exists.
    fn get_copy_transit(&mut self, copy: &EgValue, copy_status: i64) -> EgResult<Option<EgValue>> {
        if copy_status != C::COPY_STATUS_IN_TRANSIT {
            return Ok(None);
        }

        let copy_id = copy.id()?;

        let search = eg::hash! {
            target_copy: copy_id,
            dest_recv_time: EgValue::Null,
            cancel_time: EgValue::Null,
        };

        let flesh = eg::hash! {
            flesh: 1,
            flesh_fields: {atc: ["dest"]},
        };

        let mut transits = self.editor_mut().search_with_ops("atc", search, flesh)?;

        Ok(transits.pop())
    }

    fn circ_status(&self, copy_status: i64) -> &'static str {
        match copy_status {
            C::COPY_STATUS_ON_ORDER => "02",
            C::COPY_STATUS_AVAILABLE => "03",
            C::COPY_STATUS_CHECKED_OUT => "04",
            C::COPY_STATUS_IN_PROCESS => "06",
            C::COPY_STATUS_ON_HOLDS_SHELF => "08",
            C::COPY_STATUS_RESHELVING => "09",
            C::COPY_STATUS_IN_TRANSIT => "10",
            C::COPY_STATUS_LOST | C::COPY_STATUS_LOST_AND_PAID => "12",
            C::COPY_STATUS_MISSING => "13",
            _ => "01", // unknown
        }
    }

    /// Returns a basic response with an empty title, which indicates
    /// (to some SIP clients, at least) that the item was not found.
    fn return_item_not_found(&self, barcode: &str) -> sip2::Message {
        log::debug!("{self} No copy found with barcode: {barcode}");

        let resp = sip2::Message::from_values(
            &sip2::spec::M_ITEM_INFO_RESP,
            &[
                "01", // circ status
                "01", // security marker
                "01", // fee type
                &sip2::util::sip_date_now(),
            ],
            &[
                ("AB", &barcode),
                // For some SIP clients, an empty title is how we
                // know an item does not exist.
                ("AJ", ""),
            ],
        )
        .unwrap();

        resp
    }

    /// Find an open circulation linked to the copy.
    fn get_copy_circ(&mut self, copy: &EgValue, copy_status: i64) -> EgResult<Option<EgValue>> {
        if copy_status != C::COPY_STATUS_CHECKED_OUT {
            // Checked Out
            return Ok(None);
        }

        let copy_id = copy.id()?;

        let search = eg::hash! {
            target_copy: copy_id,
            checkin_time: EgValue::Null,
            "-or": [
              {stop_fines: EgValue::Null},
              {stop_fines: ["MAXFINES", "LONGOVERDUE"]}
            ]
        };

        let mut circs = self.editor_mut().search("circ", search)?;

        Ok(circs.pop())
    }
}
