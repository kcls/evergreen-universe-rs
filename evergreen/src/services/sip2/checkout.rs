use crate::item::Item;
use crate::patron::Patron;
use crate::session::Session;
use eg::common::circulator::Circulator;
use eg::date;
use eg::result::EgResult;
use eg::EgValue;
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
    screen_msg: Option<String>,
    was_renewal: bool,
}

impl Default for CheckoutResult {
    fn default() -> Self {
        Self::new()
    }
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
    pub fn handle_renew_all(&mut self, sip_msg: &sip2::Message) -> EgResult<sip2::Message> {
        let patron_barcode = sip_msg.get_field_value("AA").unwrap_or("");
        let password_op = sip_msg.get_field_value("AD"); // optional
        let fee_ack_op = sip_msg.get_field_value("BO");

        let patron = match self.get_patron_details(patron_barcode, password_op, None)? {
            Some(c) => c,
            None => {
                // Stub response
                let response = sip2::Message::from_values(
                    "66",
                    &[
                        "0",
                        &sip2::util::sip_count4(0), // renewed count
                        &sip2::util::sip_count4(0), // unrenewed count
                        &sip2::util::sip_date_now(),
                    ],
                    &[("AA", patron_barcode), ("AO", self.config().institution())],
                )
                .unwrap();

                return Ok(response);
            }
        };

        let mut items_renewed = Vec::new();
        let mut items_unrenewed = Vec::new();

        for item_id in patron
            .items_overdue_ids
            .iter()
            .chain(patron.items_out_ids.iter())
        {
            let item = self
                .editor()
                .retrieve("acp", *item_id)?
                .ok_or_else(|| self.editor().die_event())?;

            let item_barcode = item["barcode"].str()?;

            let result = self.checkout(
                item_barcode,
                patron_barcode,
                fee_ack_op.is_some(),
                true, // is_explicit_renewal
                self.config().setting_is_true("checkout_override_all"),
            )?;

            // Presence of circ id indicates success
            if result.circ_id.is_some() {
                items_renewed.push(item_barcode.to_string());
            } else {
                items_unrenewed.push(item_barcode.to_string());
            }
        }

        let mut response = sip2::Message::from_values(
            "66",
            &[
                "1", // success
                &sip2::util::sip_count4(items_renewed.len()),
                &sip2::util::sip_count4(items_unrenewed.len()),
                &sip2::util::sip_date_now(),
            ],
            &[("AA", patron_barcode), ("AO", self.config().institution())],
        )
        .unwrap();

        for barcode in items_renewed {
            response.add_field("BM", &barcode);
        }

        for barcode in items_unrenewed {
            response.add_field("BN", &barcode);
        }

        Ok(response)
    }

    fn checkout_renew_common(
        &mut self,
        msg: &sip2::Message,
        is_explicit_renewal: bool,
    ) -> EgResult<sip2::Message> {
        let item_barcode = match msg.get_field_value("AB") {
            Some(v) => v,
            None => {
                log::error!("checkout() missing item barcode");
                return Ok(self.checkout_item_not_found("", "", is_explicit_renewal));
            }
        };

        let patron_barcode = match msg.get_field_value("AA") {
            Some(v) => v,
            None => {
                log::error!("checkout() missing patron barcode");
                return Ok(self.checkout_item_not_found(item_barcode, "", is_explicit_renewal));
            }
        };

        log::info!("{self} Checking out item {item_barcode} to patron {patron_barcode}");

        let fee_ack_op = msg.get_field_value("BO");

        let item = match self.get_item_details(item_barcode)? {
            Some(c) => c,
            None => {
                return Ok(self.checkout_item_not_found(
                    item_barcode,
                    patron_barcode,
                    is_explicit_renewal,
                ))
            }
        };

        let patron = match self.get_patron_details(patron_barcode, None, None)? {
            Some(c) => c,
            None => {
                return Ok(self.checkout_item_not_found(
                    item_barcode,
                    patron_barcode,
                    is_explicit_renewal,
                ))
            }
        };

        let same_patron = item.circ_patron_id == Some(patron.id);
        let renew_ok = msg.fixed_fields()[0].value().eq("Y");

        let result = self.checkout(
            item_barcode,
            patron_barcode,
            fee_ack_op.is_some(),
            is_explicit_renewal || (renew_ok && same_patron), // is_renewal
            self.config().setting_is_true("checkout_override_all"),
        )?;

        self.compile_checkout_response(&item, &patron, &result, is_explicit_renewal)
    }

    pub fn handle_checkout(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        self.checkout_renew_common(msg, false)
    }

    pub fn handle_renew(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        self.checkout_renew_common(msg, true)
    }

    fn compile_checkout_response(
        &self,
        item: &Item,
        patron: &Patron,
        result: &CheckoutResult,
        is_explicit_renewal: bool,
    ) -> EgResult<sip2::Message> {
        let magnetic = item.magnetic_media;
        let msg_code = if is_explicit_renewal { "30" } else { "12" };

        let mut resp = sip2::Message::from_values(
            msg_code,
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
                ("AO", self.config().institution()),
                ("BT", (item.fee_type)),
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
        is_explicit_renewal: bool,
    ) -> sip2::Message {
        let msg_code = if is_explicit_renewal { "30" } else { "12" };

        sip2::Message::from_values(
            msg_code,
            &[
                "0",                         // checkin ok
                "N",                         // renew ok
                "N",                         // magnetic
                "N",                         // desensitize
                &sip2::util::sip_date_now(), // timestamp
            ],
            &[
                ("AA", patron_barcode),
                ("AB", item_barcode),
                ("AO", self.config().institution()),
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
        if self.config().setting_is_true("use_native_checkout") {
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
            EgValue::from(self.editor().authtoken().unwrap()),
            eg::hash! {
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
                .editor()
                .client_mut()
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
                result.circ_id = Some(circ.id()?);
                result.renewal_remaining = circ["renewal_remaining"].int()?;

                let iso_date = circ["due_date"].as_str().unwrap(); // required
                if self
                    .config()
                    .setting_is_true("due_date_use_sip_date_format")
                {
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

        let can_override = self
            .config()
            .setting_is_true(&format!("checkout.override.{}", evt.textcode()));

        if !ovride && can_override {
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

        if evt.textcode().eq("OPEN_CIRCULATION_EXISTS") {
            let msg = self
                .editor()
                .retrieve("sipsm", "checkout.open_circ_exists")?
                .ok_or_else(|| self.editor().die_event())?;

            result.screen_msg = Some(msg["message"].string()?)
        } else {
            let msg = self
                .editor()
                .retrieve("sipsm", "checkout.patron_not_allowed")?
                .ok_or_else(|| self.editor().die_event())?;

            result.screen_msg = Some(msg["message"].string()?);
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
        let mut options: HashMap<String, EgValue> = HashMap::new();

        options.insert("copy_barcode".to_string(), item_barcode.into());
        options.insert("patron_barcode".to_string(), patron_barcode.into());

        // Standalone transaction; cloning is just easier here.
        let mut editor = self.editor().clone();

        let mut circulator = Circulator::new(&mut editor, options)?;
        circulator.begin()?;
        circulator.is_override = ovride;

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
                    .events().first()
                    .ok_or_else(|| "API call failed to return an event".to_string())?
            }
            Err(err) => {
                circulator.rollback()?;
                err_bind = Some(err.event_or_default());
                err_bind.as_ref().unwrap()
            }
        };

        log::debug!(
            "{self} Checkout of {item_barcode} returned: {}",
            evt.to_value().dump()
        );

        let mut result = CheckoutResult::new();
        result.was_renewal = is_renewal;

        if evt.is_success() {
            let circ = &evt.payload()["circ"];

            if circ.is_object() {
                result.circ_id = Some(circ.id()?);
                result.renewal_remaining = circ["renewal_remaining"].int()?;

                let iso_date = circ["due_date"].as_str().unwrap(); // required
                if self
                    .config()
                    .setting_is_true("due_date_use_sip_date_format")
                {
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

        let can_override = self
            .config()
            .setting_is_true(&format!("checkout.override.{}", evt.textcode()));

        if !ovride && can_override {
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

        if evt.textcode().eq("OPEN_CIRCULATION_EXISTS") {
            let msg = self
                .editor()
                .retrieve("sipsm", "checkout.open_circ_exists")?
                .ok_or_else(|| self.editor().die_event())?;

            result.screen_msg = Some(msg["message"].string()?)
        } else {
            let msg = self
                .editor()
                .retrieve("sipsm", "checkout.patron_not_allowed")?
                .ok_or_else(|| self.editor().die_event())?;

            result.screen_msg = Some(msg["message"].string()?);
        }

        Ok(result)
    }
}
