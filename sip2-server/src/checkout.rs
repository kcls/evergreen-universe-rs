use super::item::Item;
use super::patron::Patron;
use super::session::Session;
use eg::common::circulator::Circulator;
use eg::date;
use eg::result::EgResult;
use evergreen as eg;
use std::collections::HashMap;

const RENEW_METHOD: &str = "open-ils.circ.renew";
const RENEW_OVERRIDE_METHOD: &str = "open-ils.circ.renew.override";
const CHECKOUT_METHOD: &str = "open-ils.circ.checkout.full";
const CHECKOUT_OVERRIDE_METHOD: &str = "open-ils.circ.checkout.full.override";

pub struct CheckoutResult {
    /// Presence of a circ_id implies success.
    circ_id: Option<i64>,
    due_date: Option<String>,
    renewal_remaining: i64,
    screen_msg: Option<&'static str>,
    was_renewal: bool,
}

impl CheckoutResult {
    pub fn new() -> CheckoutResult {
        CheckoutResult {
            circ_id: None,
            due_date: None,
            renewal_remaining: 0,
            screen_msg: None,
            was_renewal: false,
        }
    }
}

impl Session {
    pub fn handle_checkout(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        self.set_authtoken()?;

        let item_barcode = match msg.get_field_value("AB") {
            Some(v) => v,
            None => {
                log::error!("checkout() missing item barcode");
                return Ok(self.checkout_item_not_found("", ""));
            }
        };

        let patron_barcode = match msg.get_field_value("AA") {
            Some(v) => v,
            None => {
                log::error!("checkout() missing patron barcode");
                return Ok(self.checkout_item_not_found(&item_barcode, ""));
            }
        };

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

        let renew_ok = msg.fixed_fields()[0].value().eq("Y");
        let same_patron = item.circ_patron_id.unwrap_or(-1) == patron.id;

        let result = self.checkout(
            &item_barcode,
            &patron_barcode,
            fee_ack_op.is_some(),
            renew_ok && same_patron, // is_renewal
            self.account().settings().checkout_override_all(),
        )?;

        self.compile_checkout_response(&item, &patron, &result)
    }

    fn compile_checkout_response(
        &self,
        item: &Item,
        patron: &Patron,
        result: &CheckoutResult,
    ) -> EgResult<sip2::Message> {
        let magnetic = item.magnetic_media;

        let mut resp = sip2::Message::from_values(
            &sip2::spec::M_CHECKOUT_RESP,
            &[
                sip2::util::num_bool(result.circ_id.is_some()), // checkin ok
                sip2::util::sip_bool(result.was_renewal),       // renew ok
                sip2::util::sip_bool(magnetic),                 // magnetic
                sip2::util::sip_bool(!magnetic),                // desensitize
                &sip2::util::sip_date_now(),                    // timestamp
            ],
            &[
                ("AA", &patron.barcode),
                ("AB", &item.barcode),
                ("AJ", &item.title),
                ("AO", self.account().settings().institution()),
                ("BT", &item.fee_type),
                ("CI", "N"), // security inhibit
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
            &sip2::spec::M_CHECKOUT_RESP,
            &[
                "0",                         // checkin ok
                "N",                         // renew ok
                "N",                         // magnetic
                "N",                         // desensitize
                &sip2::util::sip_date_now(), // timestamp
            ],
            &[
                ("AA", &patron_barcode),
                ("AB", &item_barcode),
                ("AO", self.account().settings().institution()),
            ],
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
    ) -> EgResult<CheckoutResult> {
        if self.account().settings().use_native_checkout() {
            self.checkout_native(item_barcode, patron_barcode, fee_ack, is_renewal, ovride)
        } else {
            self.checkout_api(item_barcode, patron_barcode, fee_ack, is_renewal, ovride)
        }
    }

    /// Checkout variant that calls the traditional open-ils.circ APIs.
    fn checkout_api(
        &mut self,
        item_barcode: &str,
        patron_barcode: &str,
        fee_ack: bool,
        is_renewal: bool,
        ovride: bool,
    ) -> EgResult<CheckoutResult> {
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

        let mut resp =
            match self
                .osrf_client_mut()
                .send_recv_one("open-ils.circ", method, params)?
            {
                Some(r) => r,
                None => Err(format!("API call {method} failed to return a response"))?,
            };

        log::debug!("{self} Checkout of {item_barcode} returned: {resp}");

        let event = if resp.is_array() {
            resp[0].take()
        } else {
            resp
        };

        let mut result = CheckoutResult::new();
        result.was_renewal = is_renewal;

        let evt = eg::event::EgEvent::parse(&event)
            .ok_or_else(|| format!("API call {method} failed to return an event"))?;

        if evt.is_success() {
            let circ = &evt.payload()["circ"];

            if circ.is_object() {
                result.circ_id = Some(eg::util::json_int(&circ["id"])?);
                result.renewal_remaining = eg::util::json_int(&circ["renewal_remaining"])?;

                let iso_date = circ["due_date"].as_str().unwrap(); // required
                if self.account().settings().due_date_use_sip_date_format() {
                    let due_dt = date::parse_datetime(iso_date)?;
                    result.due_date = Some(sip2::util::sip_date_from_dt(&due_dt));
                } else {
                    result.due_date = Some(iso_date.to_string());
                }

                return Ok(result);
            } else {
                log::error!("{self} checked out, but did not receive a circ object");
            }
        }

        if !ovride
            && self
                .account()
                .settings()
                .checkout_override()
                .contains(&evt.textcode().to_string())
        {
            return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
        }

        if !ovride && fee_ack {
            // Caller acknowledges a fee is required.
            if evt.textcode().eq("ITEM_DEPOSIT_FEE_REQUIRED")
                || evt.textcode().eq("ITEM_RENTAL_FEE_REQUIRED")
            {
                return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
            }
        }

        // TODO gettext() can be used for these string literals below, but
        // it's a massive dependency for just a couple of sentences.
        // There's likely a better approach.
        if evt.textcode().eq("OPEN_CIRCULATION_EXISTS") {
            result.screen_msg = Some("This item is already checked out");
        } else {
            result.screen_msg = Some("Patron is not allowed to checkout the selected item");
        }

        Ok(result)
    }

    /// Checkout that runs within the current thread as a direct
    /// Rust call.
    fn checkout_native(
        &mut self,
        item_barcode: &str,
        patron_barcode: &str,
        fee_ack: bool,
        is_renewal: bool,
        ovride: bool,
    ) -> EgResult<CheckoutResult> {
        let mut options: HashMap<String, json::JsonValue> = HashMap::new();

        options.insert("copy_barcode".to_string(), item_barcode.into());
        options.insert("patron_barcode".to_string(), patron_barcode.into());

        let editor = self.editor().clone();

        let mut circulator = Circulator::new(editor, options)?;
        circulator.is_override = ovride;
        circulator.begin()?;

        // Collect needed data then kickoff the checkin process.
        let api_result = if is_renewal {
            circulator.renew()
        } else {
            circulator.checkout()
        };

        let err_bind;
        let evt = match api_result {
            Ok(()) => {
                circulator.commit()?;
                circulator
                    .events()
                    .get(0)
                    .ok_or_else(|| format!("API call failed to return an event"))?
            }
            Err(err) => {
                circulator.rollback()?;
                err_bind = Some(err.event_or_default());
                err_bind.as_ref().unwrap()
            }
        };

        log::debug!(
            "{self} Checkout of {item_barcode} returned: {}",
            evt.to_json_value().dump()
        );

        let mut result = CheckoutResult::new();
        result.was_renewal = is_renewal;

        if evt.is_success() {
            let circ = &evt.payload()["circ"];

            if circ.is_object() {
                result.circ_id = Some(eg::util::json_int(&circ["id"])?);
                result.renewal_remaining = eg::util::json_int(&circ["renewal_remaining"])?;

                let iso_date = circ["due_date"].as_str().unwrap(); // required
                if self.account().settings().due_date_use_sip_date_format() {
                    let due_dt = date::parse_datetime(iso_date)?;
                    result.due_date = Some(sip2::util::sip_date_from_dt(&due_dt));
                } else {
                    result.due_date = Some(iso_date.to_string());
                }

                return Ok(result);
            } else {
                log::error!("{self} checked out, but did not receive a circ object");
            }
        }

        if !ovride
            && self
                .account()
                .settings()
                .checkout_override()
                .contains(&evt.textcode().to_string())
        {
            return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
        }

        if !ovride && fee_ack {
            // Caller acknowledges a fee is required.
            if evt.textcode().eq("ITEM_DEPOSIT_FEE_REQUIRED")
                || evt.textcode().eq("ITEM_RENTAL_FEE_REQUIRED")
            {
                return self.checkout(item_barcode, patron_barcode, fee_ack, is_renewal, true);
            }
        }

        // TODO gettext() can be used for these string literals below, but
        // it's a massive dependency for just a couple of sentances.
        // There's likely a better approach.
        if evt.textcode().eq("OPEN_CIRCULATION_EXISTS") {
            result.screen_msg = Some("This item is already checked out");
        } else {
            result.screen_msg = Some("Patron is not allowed to checkout the selected item");
        }

        Ok(result)
    }
}
