use super::session::Session;

impl Session {

    pub fn handle_checkin(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {

        let barcode = msg
            .get_field_value("AB")
            .ok_or(format!("handle_item_info() missing item barcode"))?;

        let current_loc_op = msg.get_field_value("AP");
        let return_date = &msg.fixed_fields()[2];

        // cancel == un-fulfill hold this copy currently fulfills
        // KCLS only
        let cancel_op = msg.get_field_value("BI");

        log::info!("Checking in item {barcode}");

        let item = match self.get_item_details(&barcode)? {
            Some(c) => c,
            None => {
                return Ok(self.return_checkin_item_not_found(&barcode));
            }
        };

        todo!()
    }

    fn return_checkin_item_not_found(&self, barcode: &str) -> sip2::Message {

        let mut resp = sip2::Message::new(
            &sip2::spec::M_CHECKIN_RESP,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_CHECKIN_OK, "0").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_RESENSITIZE, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_MAGNETIC_MEDIA, sip2::util::sip_bool(false)).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_ALERT, "N").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("AB", &barcode);
        resp.add_field("AO", self.account().unwrap().settings().institution());
        resp.add_field("CV", "00"); // unkown alert type

        resp
    }
}


