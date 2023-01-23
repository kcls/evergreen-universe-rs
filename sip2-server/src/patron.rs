use super::session::Session;
use chrono::prelude::*;
use chrono::DateTime;
use json::JsonValue;

const JSON_NULL: JsonValue = JsonValue::Null;

#[derive(Debug)]
pub struct Patron {
    pub id: i64,
    pub barcode: String,
    pub charge_denied: bool,
    pub renew_denied: bool,
    pub recall_denied: bool,
    pub holds_denied: bool,
    pub card_lost: bool,
    pub max_charged: bool,
    pub max_overdue: bool,
    pub max_renewals: bool,
    pub max_claims_returned: bool,
    pub max_lost: bool,
    pub max_fines: bool,
    pub max_fees: bool,
    pub recall_overdue: bool,
    pub max_bills: bool,
    pub valid: bool,
    pub balance_owed: f64,
    pub password_verified: bool,
    pub recall_count: i64,
    pub hold_ids: Vec<i64>,
    pub unavail_hold_ids: Vec<i64>,
    pub holds_count: usize,
    pub unavail_holds_count: usize,
    pub items_overdue_count: usize,
    pub items_out_count: usize,
    pub items_overdue_ids: Vec<i64>,
    pub items_out_ids: Vec<i64>,
    pub fine_count: usize,
}

impl Patron {
    pub fn new(barcode: &str) -> Patron {
        Patron {
            id: 0,
            barcode: barcode.to_string(),
            charge_denied: false,
            renew_denied: false,
            recall_denied: false,
            holds_denied: false,
            card_lost: false,
            max_charged: false,
            max_overdue: false,
            max_renewals: false,
            max_claims_returned: false,
            max_lost: false,
            max_fines: false,
            max_fees: false,
            recall_overdue: false,
            max_bills: false,
            valid: false,
            balance_owed: 0.0,
            password_verified: false,
            recall_count: 0,
            holds_count: 0,
            unavail_holds_count: 0,
            items_overdue_count: 0,
            items_out_count: 0,
            hold_ids: Vec::new(),
            unavail_hold_ids: Vec::new(),
            items_overdue_ids: Vec::new(),
            items_out_ids: Vec::new(),
            fine_count: 0,
        }
    }
}

impl Session {

    pub fn get_patron_details(
        &mut self,
        barcode: &str,
        password_op: Option<&str>,
    ) -> Result<Option<Patron>, String> {

        // Make sure we have an authtoken here so we don't have to
        // keep checking for expired sessions during our data collection.
        self.authtoken(true)?;

        let user = match self.get_user(barcode)? {
            Some(u) => u,
            None => return Ok(None),
        };

        let username = user["usrname"].as_str().unwrap(); // required

        let mut patron = Patron::new(barcode);

        patron.id = self.parse_id(&user["id"])?;
        patron.password_verified = self.check_password(&username, password_op)?;

        if let Some(summary) = self.editor_mut().retrieve("mous", patron.id)? {
            patron.balance_owed = self.parse_float(&summary["balance_owed"])?;
        }

        self.set_patron_privileges(&user, &mut patron)?;
        self.set_patron_summary_items(&user, &mut patron)?;

        // TODO
        // self.set_patron_summary_list_items(&user, &mut patron)?;
        // self.log_activity

        Ok(Some(patron))
    }

    fn set_patron_summary_items(
        &mut self,
        user: &JsonValue,
        patron: &mut Patron,
    ) -> Result<(), String> {

        self.set_patron_hold_ids(patron, false, None, None)?;
        self.set_patron_hold_ids(patron, true, None, None)?;

        if let Some(summary) = self.editor_mut().retrieve("ocirclist", patron.id)? {

            // overdue and out are packaged as comma-separated ID values.
            let overdue: Vec<i64> = summary["overdue"]
                .as_str()
                .unwrap()
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .filter(|id| id > &0)
                .collect();

            let outs: Vec<i64> = summary["out"]
                .as_str()
                .unwrap()
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .filter(|id| id > &0)
                .collect();

            patron.items_overdue_count = overdue.len();
            patron.items_out_count = outs.len();
            patron.items_overdue_ids = overdue;
            patron.items_out_ids = outs;
        }

        let search = json::object! {
            usr: patron.id,
            balance_owed: {"<>": 0},
            total_owed: {">": 0},
        };

        let summaries = self.editor_mut().search("mbts", search)?;
        patron.fine_count = summaries.len();

        Ok(())
    }

    fn set_patron_hold_ids(
        &mut self,
        patron: &mut Patron,
        unavail: bool,
        limit: Option<usize>,
        offset: Option<usize>
    ) -> Result<(), String> {

        let mut search = json::object! {
            usr: patron.id,
            fulfillment_time: JSON_NULL,
            cancel_time: JSON_NULL,
        };

        if unavail {
            search["-or"] = json::array! [
              {current_shelf_lib: JSON_NULL},
              {current_shelf_lib: {"!=": {"+ahr": "pickup_lib"}}}
            ];
        } else if self.account().unwrap().settings().msg64_hold_items_available() {
            search["current_shelf_lib"] = json::object! {"=": {"+ahr": "pickup_lib"}};

        }

        let mut query = json::object! {
            select: {ahr: ["id"]},
            from: "ahr",
            where: {"+ahr": search},
        };

        if let Some(l) = limit { query["limit"] = json::from(l); }
        if let Some(o) = offset { query["offset"] = json::from(o); }

        let id_hash_list = self.editor_mut().json_query(query)?;

        for hash in id_hash_list {
            let hold_id = self.parse_id(&hash["id"])?;
            if unavail {
                patron.unavail_hold_ids.push(hold_id);
            } else {
                patron.hold_ids.push(hold_id);
            }
        }

        if unavail {
            patron.unavail_holds_count = patron.unavail_hold_ids.len();
        } else {
            patron.holds_count = patron.hold_ids.len();
        }

        Ok(())
    }

    fn set_patron_privileges(
        &mut self,
        user: &JsonValue,
        patron: &mut Patron,
    ) -> Result<(), String> {
        let expire_date_str = user["expire_date"].as_str().unwrap(); // required

        // chrono has a parse_from_rfc3339() function, but it does
        // not precisely match the format returned by PG, which uses
        // timezone without colons.
        let expire_date = DateTime::parse_from_str(&expire_date_str, "%Y-%m-%dT%H:%M:%S%z")
            .or_else(|e| Err(format!("Invalid expire date: {e} {expire_date_str}")))?;

        if expire_date < Local::now() {
            // Patron is expired.  Don't bother checking other penalties, etc.

            patron.charge_denied = true;
            patron.renew_denied = true;
            patron.recall_denied = true;
            patron.holds_denied = true;

            return Ok(());
        }

        if self
            .account()
            .unwrap()
            .settings()
            .patron_status_permit_all()
        {
            // This setting group allows all patron actions regardless
            // of penalties, fines, etc.
            return Ok(());
        }

        let penalties = self.get_patron_penalties(patron.id)?;

        patron.max_fines = self.penalties_contain(1, &penalties)?; // PATRON_EXCEEDS_FINES
        patron.max_overdue = self.penalties_contain(2, &penalties)?; // PATRON_EXCEEDS_OVERDUE_COUNT

        let blocked =
             self.parse_bool(&user["barred"]) ||
            !self.parse_bool(&user["active"]) ||
            !self.parse_bool(&user["card"]["active"]);

        let mut block_tags = String::new();
        for pen in penalties.iter() {
            if let Some(tag) = pen["block_tag"].as_str() {
                block_tags += tag;
            }
        }

        if !blocked && block_tags.len() == 0 {
            // No blocks, etc. left to inspect.  All done.
            return Ok(())
        }

        patron.holds_denied = blocked || block_tags.contains("HOLDS");

        if self.account().unwrap().settings().patron_status_permit_loans() {
            // We're going to ignore checkout, renewals blocks for now.
            return Ok(())
        }

        patron.charge_denied = blocked || block_tags.contains("CIRC");
        patron.renew_denied = blocked || block_tags.contains("RENEW");

		// In evergreen, patrons cannot create Recall holds directly, but that
		// doesn't mean they would not have said privilege if the functionality
		// existed.  Base the ability to perform recalls on whether they have
		// checkout and holds privilege, since both would be needed for recalls.
		patron.recall_denied = patron.charge_denied || patron.renew_denied;

        Ok(())
    }

    fn penalties_contain(&self, penalty_id: i64, penalties: &Vec<JsonValue>) -> Result<bool, String> {
        for pen in penalties.iter() {
            let pen_id = self.parse_id(pen)?;
            if pen_id == penalty_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_patron_penalties(&mut self, user_id: i64) -> Result<Vec<JsonValue>, String> {

        let requestor = self.editor().requestor().unwrap();

        let mut field = &requestor["ws_ou"];
        if field.is_null() {
            field = &requestor["home_ou"];
        };
        let ws_org = self.parse_id(field)?;

        let search = json::object! {
            select: {csp: ["id", "block_list"]},
            from: {ausp: "csp"},
            where: {
                "+ausp": {
                    usr: user_id,
                    "-or": [
                      {stop_date: JSON_NULL},
                      {stop_date: {">": "now"}},
                    ],
                    org_unit: {
                        in: {
                            select: {
                                aou: [{
                                    transform: "actor.org_unit_full_path",
                                    column: "id",
                                    result_field: "id",
                                }]
                            },
                            from: "aou",
                            where: {id: ws_org}
                        }
                    }
                }
            }
        };

        self.editor_mut().json_query(search)
    }

    fn get_user(&mut self, barcode: &str) -> Result<Option<JsonValue>, String> {
        let search = json::object! { barcode: barcode };

        let flesh = json::object! {
            flesh: 3,
            flesh_fields: {
                ac: ["usr"],
                au: ["billing_address", "mailing_address", "profile", "stat_cat_entries"],
                actscecm: ["stat_cat"]
            }
        };

        let mut cards = self.editor_mut().search_with_ops("ac", search, flesh)?;

        if cards.len() == 0 {
            return Ok(None);
        }

        let mut user = cards[0]["usr"].take();
        user["card"] = cards[0].to_owned();

        Ok(Some(user))
    }

    fn check_password(
        &mut self,
        username: &str,
        password_op: Option<&str>,
    ) -> Result<bool, String> {
        let password = match password_op {
            Some(p) => p,
            None => return Ok(false),
        };

        let authtoken = self.authtoken(false)?;
        let mut ses = self.osrf_client_mut().session("open-ils.actor");

        let mut req = ses.request(
            "open-ils.actor.verify_user_password",
            vec![
                authtoken,
                JSON_NULL,
                json::from(username),
                JSON_NULL,
                json::from(password),
            ],
        )?;

        if let Some(resp) = req.recv(60)? {
            Ok(self.parse_bool(&resp))
        } else {
            Err(format!("API call timed out"))
        }
    }

    pub fn handle_patron_status(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        let barcode = msg
            .get_field_value("AA")
            .ok_or(format!("handle_patron_status() missing patron barcode"))?;

        let password_op = msg.get_field_value("AD"); // optional

        let patron_op = self.get_patron_details(&barcode, password_op.as_deref())?;
        self.patron_response_common("24", &barcode, patron_op.as_ref())
    }

    fn patron_response_common(&self, msg_code: &str,
        barcode: &str, patron_op: Option<&Patron>) -> Result<sip2::Message, String> {

        let msg_spec = sip2::spec::Message::from_code(msg_code)
            .ok_or(format!("Invalid SIP message code: {msg_code}"))?;

        if patron_op.is_none() {

            let status = format!("{}{}{}{}          ",
                sip2::util::space_bool(false),
                sip2::util::space_bool(false),
                sip2::util::space_bool(false),
                sip2::util::space_bool(false));

            let mut resp = sip2::Message::new(
                msg_spec,
                vec![
                    sip2::FixedField::new(&sip2::spec::FF_PATRON_STATUS, &status).unwrap(),
                    sip2::FixedField::new(&sip2::spec::FF_LANGUAGE, "000").unwrap(),
                    sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
                ],
                Vec::new(),
            );

            resp.add_field("AO", self.account().unwrap().settings().institution());
            resp.add_field("AA", barcode);
            resp.add_field("BL", sip2::util::sip_bool(false)); // valid patron
            resp.add_field("CQ", sip2::util::sip_bool(false)); // valid patron password

            return Ok(resp)
        }

        let patron = patron_op.unwrap();

        log::debug!("PATRON: {patron:?}");

        let status = format!("{}{}{}{}          ",
            sip2::util::space_bool(patron.charge_denied),
            sip2::util::space_bool(patron.renew_denied),
            sip2::util::space_bool(patron.recall_denied),
            sip2::util::space_bool(patron.holds_denied));

        let mut resp = sip2::Message::new(
            msg_spec,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_PATRON_STATUS, &status).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_LANGUAGE, "000").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("AO", self.account().unwrap().settings().institution());
        resp.add_field("AA", barcode);
        resp.add_field("BL", sip2::util::sip_bool(true)); // valid patron
        resp.add_field("CQ", sip2::util::sip_bool(patron.password_verified));

        // TODO more stuff

        Ok(resp)
    }
}
