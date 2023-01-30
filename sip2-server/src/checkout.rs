use super::session::Session;
use super::patron;
use super::item;
use chrono::NaiveDateTime;
use evergreen as eg;


const RENEW_METHOD: &str = "open-ils.circ.renew";
const RENEW_OVERRIDE_METHOD: &str = "open-ils.circ.renew.override";
const CHECKOUT_METHOD: &str = "open-ils.circ.checkout.full";
const CHECKOUT_OVERRIDE_METHOD: &str = "open-ils.circ.checkout.full.override";


pub struct CheckoutResult {
    circ_id: Option<i64>,
    due_date: Option<String>,
    renewal_remaining: i64,
}

impl CheckoutResult {
    pub fn new() -> CheckoutResult {
        CheckoutResult {
            circ_id: None,
            due_date: None,
            renewal_remaining: 0,
        }
    }
}

impl Session {

    pub fn handle_checkout(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        self.set_authtoken()?;

        let item_barcode = msg
            .get_field_value("AB")
            .ok_or(format!("checkout() missing item barcode"))?;

        let patron_barcode = msg
            .get_field_value("AA")
            .ok_or(format!("checkout() missing patron barcode"))?;

        let fee_ack_op = msg.get_field_value("BO");

        let item = match self.get_item_details(&item_barcode)? {
            Some(c) => c,
            None => return Ok(self.checkout_item_not_found(&item_barcode, &patron_barcode)),
        };

        let patron = match self.get_patron_details(&patron_barcode, None, None)? {
            Some(c) => c,
            None => return Ok(self.checkout_item_not_found(&item_barcode, &patron_barcode)),
        };

        let result = self.checkout(
            &item_barcode,
            &patron_barcode,
            fee_ack_op.is_some(),
            false, // is renewal
            self.account().settings().checkout_override_all(),
        )?;

        todo!()
    }

    pub fn checkout_item_not_found(&self, item_barcode: &str, patron_barcode: &str) -> sip2::Message {
        sip2::Message::from_values(
            "12",                              // checkout response
            &[
                sip2::util::num_bool(false),   // checkin ok
                sip2::util::sip_bool(false),   // renew ok
                sip2::util::sip_bool(false),   // magnetic
                sip2::util::sip_bool(false),   // desensitize
                &sip2::util::sip_date_now(),   // timestamp
            ], &[
                ("AA", &patron_barcode),
                ("AB", &item_barcode),
            ]
        ).unwrap()
    }


    fn checkout(
        &mut self,
        item_barcode: &str,
        patron_barcode: &str,
        fee_ack: bool,
        is_renewal: bool,
        ovride: bool,
    ) -> Result<CheckoutResult, String> {

        let params = json::object! {
            copy_barcode: item_barcode,
            patron_barcode: patron_barcode,
        };

        let method = match is_renewal {
            true => match ovride {
                true => RENEW_OVERRIDE_METHOD,
                false => RENEW_METHOD,
            },
            false => match ovride {
                true => CHECKOUT_OVERRIDE_METHOD,
                false => CHECKOUT_METHOD,
            }
        };

        let resp = match
            self.osrf_client_mut().sendrecvone("open-ils.circ", method, params)? {
            Some(r) => r,
            None => Err(format!("API call {method} failed to return a response"))?,
        };

        log::debug!("Checkout of {item_barcode} returned: {resp}");

        let events = if let json::JsonValue::Array(list) = resp {
            list
        } else {
            vec![resp]
        };

        let mut result = CheckoutResult::new();

        // Some checkout events are ignored.
        // Loop through them until we encounter an event we care about.
        for event in events {

            let evt = eg::event::EgEvent::parse(&event)
                .ok_or(format!("API call {method} failed to return an event"))?;

            if !ovride &&
                self.account().settings().checkout_override().contains(&evt.textcode().to_string()) {
                return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
            }


            if evt.success() {
                let circ = &evt.payload()["circ"];

                if circ.is_object() {
                    result.circ_id = Some(self.parse_id(&circ["id"])?);
                    result.renewal_remaining = self.parse_id(&circ["renewal_remaining"])?;

                    let iso_date = circ["due_date"].as_str().unwrap(); // required
                    if self.account().settings().due_date_use_sip_date_format() {
                        let due_dt = self.parse_pg_date(iso_date)?;
                        result.due_date = Some(sip2::util::sip_date_from_dt(&due_dt));
                    } else {
                        result.due_date = Some(iso_date.to_string());
                    }
                }

                return Ok(result);
            }
        }

        Ok(result)
    }
}




