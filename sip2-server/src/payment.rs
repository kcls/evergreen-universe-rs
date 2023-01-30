use super::session::Session;
use super::patron;
use super::item;
use evergreen as eg;

impl Session {

    pub fn handle_payment(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        self.set_authtoken()?;

        let fee_type = msg.fixed_fields()[1].value();
        let pay_type = msg.fixed_fields()[2].value();

        let patron_barcode = msg
            .get_field_value("AA")
            .ok_or(format!("handle_payment() missing patron barcode field"))?;

        let pay_amount = msg
            .get_field_value("BV")
            .ok_or(format!("handle_payment() missing pay amount field"))?;

        let fee_id = msg
            .get_field_value("CG")
            .ok_or(format!("handle_payment() missing transaction ID field"))?;

        let terminal_xact_op = msg.get_field_value("BK"); // optional

        // Envisionware extensions for relaying information about
        // payments made via credit card kiosk or cash register.
        let register_login_op = msg.get_field_value("OR");
        let check_number_op = msg.get_field_value("RN");

        todo!()
    }
}

