use super::patron::Patron;
use super::session::Session;
use evergreen as eg;

pub struct PaymentResult {
    success: bool,
    patron_barcode: String,
    screen_msg: Option<String>,
}

impl PaymentResult {
    pub fn new(patron_barcode: &str) -> Self {
        PaymentResult {
            success: false,
            screen_msg: None,
            patron_barcode: patron_barcode.to_string(),
        }
    }
}

impl Session {
    pub fn handle_payment(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        self.set_authtoken()?;

        let patron_barcode = match msg.get_field_value("AA") {
            Some(v) => v,
            None => {
                log::error!("handle_payment() missing patron barcode field");
                return Ok(self.compile_payment_response(&PaymentResult::new("")));
            }
        };

        let mut result = PaymentResult::new(&patron_barcode);

        let pay_amount_str = match msg.get_field_value("BV") {
            Some(v) => v,
            None => {
                log::error!("Payment requires amount field (BV)");
                return Ok(self.compile_payment_response(&result));
            }
        };

        let pay_amount: f64 = match pay_amount_str.parse() {
            Ok(v) => v,
            Err(_) => {
                log::error!("Invalid payment amount: '{pay_amount_str}'");
                return Ok(self.compile_payment_response(&result));
            }
        };

        // credit card, cash, etc.
        let pay_type = msg.fixed_fields()[2].value();

        let terminal_xact_op = msg.get_field_value("BK"); // optional

        // Envisionware extensions for relaying information about
        // payments made via credit card kiosk or cash register.
        let register_login_op = msg.get_field_value("OR");
        let check_number_op = msg.get_field_value("RN");

        let search = json::object! { barcode: patron_barcode };
        let ops = json::object! { flesh: 1u8, flesh_fields: {ac: ["usr"]} };
        let mut cards = self.editor_mut().search_with_ops("ac", search, ops)?;

        if cards.len() == 0 {
            return Ok(self.compile_payment_response(&result));
        }

        // Swap the fleshing to favor usr->card over card->usr
        let mut user = cards[0]["usr"].take();
        user["card"] = cards[0].to_owned();

        let payments: Vec<(i64, f64)>;

        // Caller can request to pay toward a specific transaction or have
        // the back-end select transactions to pay.
        if let Some(xact_id_str) = msg.get_field_value("CG") {
            if let Ok(xact_id) = xact_id_str.parse::<i64>() {
                payments = self.compile_one_xact(&user, xact_id, pay_amount, &mut result)?;
            } else {
                log::warn!("{self} Invalid transaction ID in payment: {xact_id_str}");
                return Ok(self.compile_payment_response(&result));
            }
        } else {
            // No transaction is specified.  Pay whatever we can.
            payments = self.compile_multi_xacts(&user, pay_amount, &mut result)?;
        }

        if payments.len() == 0 {
            return Ok(self.compile_payment_response(&result));
        }

        self.apply_payments(
            &user,
            &mut result,
            &pay_type,
            &terminal_xact_op,
            &check_number_op,
            &register_login_op,
            payments,
        )?;

        Ok(self.compile_payment_response(&result))
    }

    /// Create the SIP response message
    fn compile_payment_response(&self, result: &PaymentResult) -> sip2::Message {
        let mut resp = sip2::Message::from_values(
            &sip2::spec::M_FEE_PAID_RESP,
            &[
                sip2::util::sip_bool(result.success),
                &sip2::util::sip_date_now(),
            ],
            &[
                ("AA", &result.patron_barcode),
                ("AO", self.account().settings().institution()),
            ],
        )
        .unwrap();

        resp.maybe_add_field("AF", result.screen_msg.as_deref());

        resp
    }

    /// Caller wants to pay a specific transaction by ID.  Make sure that's
    /// a viable choice.
    fn compile_one_xact(
        &mut self,
        user: &json::JsonValue,
        xact_id: i64,
        pay_amount: f64,
        result: &mut PaymentResult,
    ) -> Result<Vec<(i64, f64)>, String> {
        let sum = match self.editor_mut().retrieve("mbts", xact_id)? {
            Some(s) => s,
            None => {
                log::warn!("{self} No such transaction with ID {xact_id}");
                return Ok(Vec::new()); // non-success, but not a kickable offense
            }
        };

        if eg::util::json_int(&sum["usr"]) != eg::util::json_int(&user["id"]) {
            log::warn!("{self} Payment transaction {xact_id} does not link to provided user");
            return Ok(Vec::new());
        }

        if pay_amount > eg::util::json_float(&sum["balance_owed"])? {
            result.screen_msg = Some("Overpayment not allowed".to_string());
            return Ok(Vec::new());
        }

        Ok(vec![(xact_id, pay_amount)])
    }

    /// Find transactions to pay
    fn compile_multi_xacts(
        &mut self,
        user: &json::JsonValue,
        pay_amount: f64,
        result: &mut PaymentResult,
    ) -> Result<Vec<(i64, f64)>, String> {
        let mut payments: Vec<(i64, f64)> = Vec::new();
        let mut patron = Patron::new(&result.patron_barcode, self.format_user_name(&user));

        patron.id = eg::util::json_int(&user["id"])?;

        let xacts = self.get_patron_xacts(&patron, None)?; // see patron mod

        if xacts.len() == 0 {
            result.screen_msg = Some("No transactions to pay".to_string());
            return Ok(payments);
        }

        let mut amount_remaining = pay_amount;
        for xact in xacts {
            let xact_id = eg::util::json_int(&xact["id"])?;
            let balance_owed = eg::util::json_float(&xact["balance_owed"])?;

            if balance_owed < 0.0 {
                continue;
            }

            let payment;

            if balance_owed >= amount_remaining {
                // We owe as much or more than the amount of money
                // we have left to distribute.  Pay what we can.
                payment = amount_remaining;
                amount_remaining = 0.0;
            } else {
                // Less is owed on this transaction than we have to
                // distribute, so pay the full amount on this one.
                payment = balance_owed;
                amount_remaining = (amount_remaining * 100.00 - balance_owed + 100.00) / 100.00;
            }

            log::info!(
                "{self} applying payment of {:.2} for xact {} with a
                transaction balance of {:.2} and amount remaining {:.2}",
                payment,
                xact_id,
                balance_owed,
                amount_remaining
            );

            payments.push((xact_id, payment));

            if amount_remaining == 0.0 {
                break;
            }
        }

        if amount_remaining > 0.0 {
            result.screen_msg = Some("Overpayment not allowed".to_string());
            return Ok(payments);
        }

        Ok(payments)
    }

    /// Send payment data to the server for processing.
    fn apply_payments(
        &mut self,
        user: &json::JsonValue,
        result: &mut PaymentResult,
        pay_type: &str,
        terminal_xact_op: &Option<String>,
        check_number_op: &Option<String>,
        register_login_op: &Option<String>,
        payments: Vec<(i64, f64)>,
    ) -> Result<(), String> {
        log::info!("{self} applying payments: {payments:?}");

        // Add the register login to the payment note if present.
        let note = if let Some(rl) = register_login_op {
            log::info!("{self} SIP sent register login string as {rl}");

            // Scrub the Windows domain if present ("DOMAIN\user")
            let mut parts = rl.split("\\");
            let p0 = parts.next();

            let login = if let Some(l) = parts.next() {
                l
            } else {
                p0.unwrap()
            };

            format!("Via SIP2: Register login '{}'", login)
        } else {
            String::from("VIA SIP2")
        };

        let mut pay_array = json::array![];
        for p in payments {
            let sub_array = json::array![p.0, p.1];
            pay_array.push(sub_array).ok();
        }

        let mut args = json::object! {
            userid: eg::util::json_int(&user["id"])?,
            note: note,
            payments: pay_array,
        };

        match pay_type {
            "01" | "02" => {
                // '01' is "VISA"; '02' is "credit card"

                args["cc_args"]["terminal_xact"] = match terminal_xact_op {
                    Some(tx) => json::from(tx.as_str()),
                    None => json::from("Not provided by SIP client"),
                };

                args["payment_type"] = json::from("credit_card_payment");
            }

            "05" => {
                // Check payment
                args["payment_type"] = json::from("check_payment");
                args["check_number"] = match check_number_op {
                    Some(s) => json::from(s.as_str()),
                    None => json::from("Not provided by SIP client"),
                };
            }
            _ => {
                args["payment_type"] = json::from("cash_payment");
            }
        }

        let authtoken = json::from(self.authtoken()?);
        let last_xact_id = user["last_xact_id"].as_str().unwrap(); // required

        let resp = self.osrf_client_mut().sendrecvone(
            "open-ils.circ",
            "open-ils.circ.money.payment",
            vec![authtoken, args, json::from(last_xact_id)],
        )?;

        let resp = resp.ok_or(format!("Payment API returned no response"))?;

        if let Some(evt) = eg::event::EgEvent::parse(&resp) {
            if let Some(d) = evt.desc() {
                result.screen_msg = Some(d.to_string());
            } else {
                result.screen_msg = Some(evt.textcode().to_string());
            }
        } else {
            result.success = true;
        }

        Ok(())
    }
}
