use super::session::Session;
use super::patron;
use super::item;
use chrono::NaiveDateTime;
use evergreen as eg;

impl Session {

    pub fn handle_checkout(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        self.set_authtoken()?;

        let item_barcode = msg
            .get_field_value("AB")
            .ok_or(format!("checkout() missing item barcode"))?;

        let patron_barcode = msg
            .get_field_value("AA")
            .ok_or(format!("checkout() missing patron barcode"))?;

        let item = match self.get_item_details(&item_barcode)? {
            Some(c) => c,
            None => return Ok(self.checkout_item_not_found(&item_barcode, &patron_barcode)),
        };

        let patron = match self.get_patron_details(&patron_barcode, None, None)? {
            Some(c) => c,
            None => return Ok(self.checkout_item_not_found(&item_barcode, &patron_barcode)),
        };


        todo!()
    }

    pub fn checkout_item_not_found(&self, item_barcode: &str, patron_barcode: &str) -> sip2::Message {

        let mut resp = sip2::Message::new(
            &sip2::spec::M_CHECKOUT_RESP,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_CHECKIN_OK, sip2::util::num_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_RENEW_OK, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_MAGNETIC_MEDIA, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DESENSITIZE, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("AA", &patron_barcode);
        resp.add_field("AB", &item_barcode);

        resp
    }
}




