use super::connection::Connection;
use super::error::Error;
use super::params::*;
use super::{spec, util, Field, FixedField, Message};
use std::str;

/// Wrapper for Connection which provides a simpler interface for some
/// common SIP2 actions.
///
/// This is not a complete set of friendly-ified requests.  Just a start.
///
/// ```no_run
/// use sip2::{Client, ParamSet};
/// let mut client = Client::new("127.0.0.1:6001").expect("Cannot Connect");
///
/// let mut params = ParamSet::new();
/// params.set_sip_user("sip-server-login");
/// params.set_sip_pass("sip-server-password");
///
/// // Login to the SIP server
/// match client.login(&params).expect("Login Error").ok() {
///     true => println!("Login OK"),
///     false => eprintln!("Login Failed"),
/// }
/// ```
pub struct Client {
    connection: Connection,
}

impl Client {
    /// Creates a new SIP client and opens the TCP connection to the server.
    pub fn new(host: &str) -> Result<Self, Error> {
        Ok(Client {
            connection: Connection::new(host)?,
        })
    }

    /// Shutdown the TCP connection with the SIP server.
    pub fn disconnect(&self) -> Result<(), Error> {
        self.connection.disconnect()
    }

    /// Login to the SIP server
    ///
    /// Sets ok=true if the OK fixed field is true.
    pub fn login(&mut self, params: &ParamSet) -> Result<SipResponse, Error> {
        let user = match params.sip_user() {
            Some(u) => u,
            _ => return Err(Error::MissingParamsError),
        };

        let pass = match params.sip_pass() {
            Some(u) => u,
            _ => return Err(Error::MissingParamsError),
        };

        let mut req = Message::new(
            &spec::M_LOGIN,
            vec![
                FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
                FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
            ],
            vec![
                Field::new(spec::F_LOGIN_UID.code, user),
                Field::new(spec::F_LOGIN_PWD.code, pass),
            ],
        );

        req.maybe_add_field(spec::F_LOCATION_CODE.code, params.location().as_deref());

        let resp = self.connection.sendrecv(&req)?;

        if resp.spec().code == spec::M_LOGIN_RESP.code
            && resp.fixed_fields().len() == 1
            && resp.fixed_fields()[0].value() == "1"
        {
            Ok(SipResponse::new(resp, true))
        } else {
            Ok(SipResponse::new(resp, false))
        }
    }

    /// Send the SC status message
    ///
    /// Sets ok=true if the server reports that it's online.
    pub fn sc_status(&mut self) -> Result<SipResponse, Error> {
        let req = Message::new(
            &spec::M_SC_STATUS,
            vec![
                FixedField::new(&spec::FF_STATUS_CODE, "0").unwrap(),
                FixedField::new(&spec::FF_MAX_PRINT_WIDTH, "999").unwrap(),
                FixedField::new(&spec::FF_PROTOCOL_VERSION, &spec::SIP_PROTOCOL_VERSION).unwrap(),
            ],
            vec![],
        );

        let resp = self.connection.sendrecv(&req)?;

        if resp.fixed_fields().len() > 0 && resp.fixed_fields()[0].value() == "Y" {
            Ok(SipResponse::new(resp, true))
        } else {
            Ok(SipResponse::new(resp, false))
        }
    }

    /// Send a patron status request
    ///
    /// Sets ok=true if the "valid patron" (BL) field is "Y"
    pub fn patron_status(&mut self, params: &ParamSet) -> Result<SipResponse, Error> {
        let patron_id = match params.patron_id() {
            Some(p) => p,
            _ => return Err(Error::MissingParamsError),
        };

        let mut req = Message::new(
            &spec::M_PATRON_STATUS,
            vec![
                FixedField::new(&spec::FF_LANGUAGE, "000").unwrap(),
                FixedField::new(&spec::FF_DATE, &util::sip_date_now()).unwrap(),
            ],
            vec![Field::new(spec::F_PATRON_ID.code, patron_id)],
        );

        req.maybe_add_field(spec::F_INSTITUTION_ID.code, params.institution().as_deref());
        req.maybe_add_field(spec::F_PATRON_PWD.code, params.patron_pwd().as_deref());
        req.maybe_add_field(spec::F_TERMINAL_PWD.code, params.terminal_pwd().as_deref());

        let resp = self.connection.sendrecv(&req)?;

        if let Some(bl_val) = resp.get_field_value(spec::F_VALID_PATRON.code) {
            if bl_val == "Y" {
                return Ok(SipResponse::new(resp, true));
            }
        }

        Ok(SipResponse::new(resp, false))
    }

    /// Send a patron information request
    ///
    /// Sets ok=true if the "valid patron" (BL) field is "Y"
    pub fn patron_info(&mut self, params: &ParamSet) -> Result<SipResponse, Error> {
        let patron_id = match params.patron_id() {
            Some(p) => p,
            None => return Err(Error::MissingParamsError),
        };

        let mut summary: [char; 10] = [' '; 10];

        if let Some(idx) = *params.summary() {
            if idx < 10 {
                summary[idx] = 'Y';
            }
        }

        let sum_str: String = summary.iter().collect::<String>();

        let mut req = Message::new(
            &spec::M_PATRON_INFO,
            vec![
                FixedField::new(&spec::FF_LANGUAGE, "000").unwrap(),
                FixedField::new(&spec::FF_DATE, &util::sip_date_now()).unwrap(),
                FixedField::new(&spec::FF_SUMMARY, &sum_str).unwrap(),
            ],
            vec![Field::new(spec::F_PATRON_ID.code, patron_id)],
        );

        req.maybe_add_field(spec::F_INSTITUTION_ID.code, params.institution().as_deref());
        req.maybe_add_field(spec::F_PATRON_PWD.code, params.patron_pwd().as_deref());
        req.maybe_add_field(spec::F_TERMINAL_PWD.code, params.terminal_pwd().as_deref());

        if let Some(v) = params.start_item() {
            req.add_field(spec::F_START_ITEM.code, &v.to_string());
        }

        if let Some(v) = params.end_item() {
            req.add_field(spec::F_END_ITEM.code, &v.to_string());
        }

        let resp = self.connection.sendrecv(&req)?;

        if let Some(bl_val) = resp.get_field_value(spec::F_VALID_PATRON.code) {
            if bl_val == "Y" {
                return Ok(SipResponse::new(resp, true));
            }
        }

        Ok(SipResponse::new(resp, false))
    }

    /// Send a item information request
    ///
    /// Sets ok=true if a title (AJ) value is present.  Oddly, there's no
    /// specific "item does not exist" value in the Item Info Response.
    pub fn item_info(&mut self, params: &ParamSet) -> Result<SipResponse, Error> {
        let item_id = match params.item_id() {
            Some(id) => id,
            None => return Err(Error::MissingParamsError),
        };

        let mut req = Message::new(
            &spec::M_ITEM_INFO,
            vec![FixedField::new(&spec::FF_DATE, &util::sip_date_now()).unwrap()],
            vec![Field::new(spec::F_ITEM_IDENT.code, &item_id)],
        );

        req.maybe_add_field(spec::F_INSTITUTION_ID.code, params.institution().as_deref());
        req.maybe_add_field(spec::F_TERMINAL_PWD.code, params.terminal_pwd().as_deref());

        let resp = self.connection.sendrecv(&req)?;

        if let Some(title_val) = resp.get_field_value(spec::F_TITLE_IDENT.code) {
            if title_val != "" {
                return Ok(SipResponse::new(resp, true));
            }
        }

        Ok(SipResponse::new(resp, false))
    }
}

/// Wrapper for holding the SIP response message and a simplistic
/// "OK" flag.
pub struct SipResponse {
    /// The response message.
    msg: Message,

    /// True if the message response indicates a success.
    ///
    /// The definition of success varies per request type and may not
    /// match the caller's requirements.  See the full message in
    /// 'msg' to inspect the entire response.
    ok: bool,
}

impl SipResponse {
    pub fn new(msg: Message, ok: bool) -> Self {
        SipResponse { msg, ok }
    }

    pub fn ok(&self) -> bool {
        self.ok
    }
    pub fn msg(&self) -> &Message {
        &self.msg
    }

    /// Shortcut for this.resp.msg().get_field_value(code)
    pub fn value(&self, code: &str) -> Option<String> {
        self.msg().get_field_value(code)
    }
}
