use super::session::Session;
use evergreen as eg;
use gettextrs::*;

const RENEW_METHOD: &str = "open-ils.circ.renew";
const RENEW_OVERRIDE_METHOD: &str = "open-ils.circ.renew.override";
const CHECKOUT_METHOD: &str = "open-ils.circ.checkout.full";
const CHECKOUT_OVERRIDE_METHOD: &str = "open-ils.circ.checkout.full.override";

pub struct CheckoutResult {
    /// Presence of a circ_id implies success.
    circ_id: Option<i64>,
    due_date: Option<String>,
    renewal_remaining: i64,
    screen_msg: Option<String>,
}

impl CheckoutResult {
    pub fn new() -> CheckoutResult {
        CheckoutResult {
            circ_id: None,
            due_date: None,
            renewal_remaining: 0,
            screen_msg: None,
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

        log::info!("{self} Checking out item {item_barcode} to patron {patron_barcode}");

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

        let renew_ok = result.renewal_remaining > 0 && !patron.renew_denied;
        let magnetic = item.magnetic_media;

        let mut resp = sip2::Message::from_values(
            "12", // checkout response
            &[
                sip2::util::num_bool(result.circ_id.is_some()), // checkin ok
                sip2::util::sip_bool(renew_ok),                 // renew ok
                sip2::util::sip_bool(magnetic),                 // magnetic
                sip2::util::sip_bool(!magnetic),                // desensitize
                &sip2::util::sip_date_now(),                    // timestamp
            ],
            &[
                ("AA", &patron_barcode),
                ("AB", &item_barcode),
                ("AJ", &item.title),
                ("AO", self.account().settings().institution()),
                ("BT", &item.fee_type),
                ("CI", sip2::util::num_bool(false)), // security inhibit
                ("CK", &item.media_type),
            ],
        )
        .unwrap();

        resp.maybe_add_field("AF", result.screen_msg.as_deref());
        resp.maybe_add_field("AH", result.due_date.as_deref());

        if let Some(id) = result.circ_id {
            resp.add_field("BK", &format!("{id}"));
        }

        if item.deposit_amount > 0.0 {
            resp.add_field("BV", &format!("{:.2}", item.deposit_amount));
        }

        Ok(resp)
    }

    pub fn checkout_item_not_found(
        &self,
        item_barcode: &str,
        patron_barcode: &str,
    ) -> sip2::Message {
        sip2::Message::from_values(
            "12", // checkout response
            &[
                sip2::util::num_bool(false), // checkin ok
                sip2::util::sip_bool(false), // renew ok
                sip2::util::sip_bool(false), // magnetic
                sip2::util::sip_bool(false), // desensitize
                &sip2::util::sip_date_now(), // timestamp
            ],
            &[("AA", &patron_barcode), ("AB", &item_barcode)],
        )
        .unwrap()
    }

    fn checkout(
        &mut self,
        item_barcode: &str,
        patron_barcode: &str,
        fee_ack: bool,
        is_renewal: bool,
        ovride: bool,
    ) -> Result<CheckoutResult, String> {
        let params = vec![
            json::from(self.authtoken()?),
            json::object! {
                copy_barcode: item_barcode,
                patron_barcode: patron_barcode,
            },
        ];

        let method = match is_renewal {
            true => match ovride {
                true => RENEW_OVERRIDE_METHOD,
                false => RENEW_METHOD,
            },
            false => match ovride {
                true => CHECKOUT_OVERRIDE_METHOD,
                false => CHECKOUT_METHOD,
            },
        };

        let resp = match self
            .osrf_client_mut()
            .sendrecvone("open-ils.circ", method, params)?
        {
            Some(r) => r,
            None => Err(format!("API call {method} failed to return a response"))?,
        };

        log::debug!("{self} Checkout of {item_barcode} returned: {resp}");

        let event = if let json::JsonValue::Array(list) = resp {
            list[0].to_owned()
        } else {
            resp
        };

        let mut result = CheckoutResult::new();

        let evt = eg::event::EgEvent::parse(&event)
            .ok_or(format!("API call {method} failed to return an event"))?;

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

                return Ok(result);
            }
        }

        if !ovride {
            if self
                .account()
                .settings()
                .checkout_override()
                .contains(&evt.textcode().to_string())
            {
                // Event is configured for override

                return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
            } else if fee_ack {
                // Caller acknowledges a fee is required.

                if evt.textcode().eq("ITEM_DEPOSIT_FEE_REQUIRED")
                    || evt.textcode().eq("ITEM_RENTAL_FEE_REQUIRED")
                {
                    return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
                }
            }
        }

        if evt.textcode().eq("OPEN_CIRCULATION_EXISTS") {
            result.screen_msg = Some(gettext("This item is already checked out"));
        } else {
            result.screen_msg = Some(gettext(
                "Patron is not allowed to checkout the selected item",
            ));
        }

        Ok(result)
    }
}
