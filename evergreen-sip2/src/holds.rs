use crate::session::Session;
use eg::result::EgResult;
use eg::EgEvent;
use eg::EgValue;
use evergreen as eg;

use crate::item::Item;
use crate::patron::Patron;

impl Session {
    pub fn handle_hold(&mut self, sip_msg: sip2::Message) -> EgResult<sip2::Message> {
        let patron_barcode = sip_msg.get_field_value("AA").unwrap_or("");
        let item_barcode = sip_msg.get_field_value("AB").unwrap_or("");

        let mut response = sip2::Message::from_values(
            "16",
            &[
                "0", // OK
                "N", // Available
            ],
            &[
                ("AA", patron_barcode),
                ("AB", item_barcode),
                ("AO", self.config().institution()),
            ],
        )
        .unwrap();

        // At present, hold cancelation is the only supported operation.
        if sip_msg.fixed_fields().first().map(|f| f.value()) != Some("-") {
            log::warn!("{self} unsupported hold operation");
            return Ok(response);
        }

        let patron = match self.get_patron_details(patron_barcode, None, None)? {
            Some(p) => p,
            None => return Ok(response),
        };

        let item = match self.get_item_details(item_barcode)? {
            Some(i) => i,
            None => return Ok(response),
        };

        let hold = match self.hold_from_copy(&patron, &item)? {
            Some(v) => v,
            None => return Ok(response),
        };

        if !self.cancel_hold(hold.id()?)? {
            return Ok(response);
        }

        // Set the "OK" flag
        response.fixed_fields_mut()[0].set_value("1").unwrap();

        response.add_field("AJ", &format!("{}", item.record_id));

        // Use the targeted copy
        if let Some(bc) = hold["current_copy"]["barcode"].as_str() {
            for field in response.fields_mut().iter_mut() {
                if field.code() == "AB" {
                    field.set_value(bc);
                }
            }
        }

        Ok(response)
    }

    fn cancel_hold(&mut self, hold_id: i64) -> EgResult<bool> {
        let params = vec![
            EgValue::from(self.editor().authtoken().unwrap()),
            EgValue::from(hold_id),
            EgValue::from(7), // cancel via SIP
        ];

        let resp = self.editor().client_mut().send_recv_one(
            "open-ils.circ",
            "open-ils.circ.hold.cancel",
            params,
        )?;

        if resp.is_none() || EgEvent::parse(&resp.unwrap()).is_some() {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn search_one_hold(&mut self, patron: &Patron, filters: EgValue) -> EgResult<Option<EgValue>> {
        let mut query = eg::hash! {
            "usr": patron.id,
            "cancel_time": EgValue::Null,
            "fulfillment_time": EgValue::Null,
        };

        let flesh = eg::hash! {
            "flesh": 2,
            "flesh_fields": {
                "ahr": ["current_copy"],
                "acp": ["call_number"],
            },
            "order_by": {"ahr": "request_time DESC"},
            "limit": 1
        };

        // Absorb the query-specific filters
        for (key, val) in filters.entries() {
            query[key] = val.clone();
        }

        self.editor()
            .search_with_ops("ahr", query, flesh)
            .map(|mut v| v.pop())
    }

    // Given a "representative" copy, finds a matching hold owned by
    // the patron in question.
    fn hold_from_copy(&mut self, patron: &Patron, item: &Item) -> EgResult<Option<EgValue>> {
        // first see if there is a direct match on current_copy
        let filters = eg::hash! {"current_copy": item.id};

        if let Some(hold) = self.search_one_hold(patron, filters)? {
            return Ok(Some(hold));
        }

        // next, assume bib-level holds are the most common
        let filters = eg::hash! {"target": item.record_id, "hold_type": "T"};

        if let Some(hold) = self.search_one_hold(patron, filters)? {
            return Ok(Some(hold));
        }

        // next try metarecord holds
        let query = eg::hash! {"source": item.record_id};
        if let Some(map) = self.editor().search("mmrsm", query)?.pop() {
            let filters = eg::hash! {
                "target": map["metarecord"].int()?,
                "hold_type": "M"
            };

            if let Some(hold) = self.search_one_hold(patron, filters)? {
                return Ok(Some(hold));
            }
        }

        // Volume holds
        let filters = eg::hash! {
            "target": item.call_number_id,
            "hold_type": "V",
        };

        if let Some(hold) = self.search_one_hold(patron, filters)? {
            return Ok(Some(hold));
        }

        // Copy holds
        let filters = eg::hash! {
            "target": item.id,
            "hold_type": ["C", "F", "R"],
        };

        if let Some(hold) = self.search_one_hold(patron, filters)? {
            return Ok(Some(hold));
        }

        Ok(None)
    }
}
