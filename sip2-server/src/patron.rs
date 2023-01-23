use super::session::Session;
use chrono::prelude::*;
use chrono::DateTime;

pub struct Patron {
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
}

impl Patron {
    pub fn new() -> Patron {
        Patron {
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
        }
    }
}

impl Session {

    pub fn get_patron_details(&mut self,
        barcode: &str, password_op: Option<&str>) -> Result<Option<Patron>, String> {

        let user = match self.get_user(barcode)? {
            Some(u) => u,
            None => return Ok(None),
        };

        let username = user["username"].as_str().unwrap(); // required

        let mut patron = Patron::new();
        patron.password_verified = self.check_password(&username, password_op)?;

        self.set_patron_privileges(&user, &mut patron)?;

        Ok(Some(patron))
    }

    fn set_patron_privileges(&mut self,
        user: &json::JsonValue, patron: &mut Patron) -> Result<(), String> {

        let user_id = self.parse_id(&user["id"])?;

        let expire_date_str = user["expire_date"].as_str().unwrap(); // required
        let expire_date = DateTime::parse_from_rfc3339(&expire_date_str)
            .or_else(|e| Err(format!("Invalid expire date: {e} {expire_date_str}")))?;

        if expire_date < Local::now() {
            // Patron is expired.  Don't bother checking other penalties, etc.

            patron.charge_denied = true;
            patron.renew_denied = true;
            patron.recall_denied = true;
            patron.holds_denied = true;

            return Ok(());
        }

        if self.account().unwrap().settings().patron_status_permit_all() {
            // This setting group allows all patron actions regardless
            // of penalties, fines, etc.
            return Ok(());
        }

        let penalties = self.get_patron_penalties(user_id)?;

        todo!()
    }

    fn get_patron_penalties(&mut self, user_id: i64) -> Result<Vec<json::JsonValue>, String> {
        let ws_org = self.parse_id(&self.editor().requestor().unwrap()["ws_ou"])?;

        let search = json::object! {
            select: {csp: ["id", "block_list"]},
            from: {ausp: "csp"},
            where: {
                "+ausp": {
                    usr: user_id,
                    "-or": [
                      {stop_date: json::JsonValue::Null},
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

    fn get_user(&mut self, barcode: &str) -> Result<Option<json::JsonValue>, String> {
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

    fn check_password(&mut self,
        username: &str, password_op: Option<&str>) -> Result<bool, String> {

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
                json::JsonValue::Null,
                json::from(username),
                json::JsonValue::Null,
                json::from(password),
            ]
        )?;

        if let Some(resp) = req.recv(60)? {
            if let Some(evt) = self.unpack_response_event(&resp)? {
                Err(format!("Unexpected response in password check: {evt}"))
            } else {
                Ok(self.parse_bool(&resp))
            }
        } else {
            Err(format!("API call timed out"))
        }
    }

    pub fn handle_patron_status(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {

        let barcode = msg
            .get_field_value("AA")
            .ok_or(format!("handle_patron_status() missing patron barcode"))?;

        let password_op = msg.get_field_value("AD"); // optional

        let patron = match self.get_patron_details(&barcode, password_op.as_deref())? {
            Some(p) => p,
            None => Err(format!("No such patron: {barcode}"))?,
        };

        Err(format!("TODO"))
    }

}

