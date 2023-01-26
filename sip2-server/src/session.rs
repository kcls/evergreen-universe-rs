use super::conf;
use eg::auth;
use evergreen as eg;
use opensrf as osrf;
use sip2;
use std::fmt;
use std::net;
use std::sync::Arc;
use std::collections::HashMap;

// Block this many seconds before waking to see if we need
// to perform any maintenance / shutdown.
const SIP_RECV_TIMEOUT: u64 = 5;

const INSTITUTION_SUPPORTS: &[&str] = &[
    "Y", // patron status request,
    "Y", // checkout,
    "Y", // checkin,
    "N", // block patron,
    "Y", // acs status,
    "N", // request sc/acs resend,
    "Y", // login,
    "Y", // patron information,
    "N", // end patron session,
    "Y", // fee paid,
    "Y", // item information,
    "N", // item status update,
    "N", // patron enable,
    "N", // hold,
    "Y", // renew,
    "N", // renew all,
];

/// Manages the connection between a SIP client and the HTTP backend.
pub struct Session {
    sesid: usize,
    sip_connection: sip2::Connection,
    shutdown: bool,
    sip_config: conf::Config,
    osrf_client: osrf::Client,
    editor: eg::editor::Editor,

    // We won't have some values until the SIP client logs in.
    account: Option<conf::SipAccount>,

    org_sn_cache: HashMap<String, i64>,
}

impl Session {
    /// Our thread starts here.  If anything fails, we just log and exit
    pub fn run(
        sip_config: conf::Config,
        osrf_config: Arc<osrf::Config>,
        idl: Arc<eg::idl::Parser>,
        stream: net::TcpStream,
        sesid: usize,
    ) {
        match stream.peer_addr() {
            Ok(a) => log::info!("New SIP connection from {}", a),
            Err(e) => {
                log::error!("SIP connection has no peer addr? {}", e);
                return;
            }
        }

        let mut con = sip2::Connection::new_from_stream(stream);
        con.set_ascii(sip_config.ascii());

        let osrf_client = match osrf::Client::connect(osrf_config.clone()) {
            Ok(c) => c,
            Err(e) => {
                log::error!("Cannot connect to OpenSRF: {e}");
                return;
            }
        };

        osrf_client.set_serializer(eg::idl::Parser::as_serializer(&idl));

        let editor = eg::Editor::new(&osrf_client, &idl);

        let mut ses = Session {
            sesid,
            editor,
            sip_config,
            osrf_client,
            account: None,
            shutdown: false,
            sip_connection: con,
            org_sn_cache: HashMap::new(),
        };

        if let Err(e) = ses.start() {
            log::error!("{ses} exited on error: {e}");
        }
    }

    pub fn org_sn_cache(&self) -> &HashMap<String, i64> {
        &self.org_sn_cache
    }

    pub fn org_sn_cache_mut(&mut self) -> &mut HashMap<String, i64> {
        &mut self.org_sn_cache
    }

    pub fn has_account(&self) -> bool {
        self.account.is_some()
    }

    /// Panics if no account has been set
    pub fn account(&self) -> &conf::SipAccount {
        self.account.as_ref().expect("No account set")
    }

    /// Panics if no account has been set
    pub fn account_mut(&mut self) -> &mut conf::SipAccount {
        self.account.as_mut().expect("No account set")
    }

    pub fn sip_config(&self) -> &conf::Config {
        &self.sip_config
    }

    pub fn osrf_client_mut(&mut self) -> &mut osrf::Client {
        &mut self.osrf_client
    }

    pub fn editor_mut(&mut self) -> &mut eg::editor::Editor {
        &mut self.editor
    }

    pub fn editor(&self) -> &eg::editor::Editor {
        &self.editor
    }

    /// Return the authtoken wrapped as a JSON string for easier use in API calls.
    ///
    /// Returns Err if we fail to verify the token or login as needed.
    pub fn set_authtoken(&mut self) -> Result<(), String> {
        if self.editor.authtoken().is_some() {
            if self.editor.checkauth()? {
                return Ok(());
            }
        }

        self.login()
    }

    pub fn authtoken(&self) -> Result<&str, String> {
        match self.editor().authtoken() {
            Some(a) => Ok(a),
            None => Err(format!("Authtoken is unset")),
        }
    }

    /// Cache the user id after the first lookup
    fn get_ils_user_id(&mut self) -> Result<i64, String> {
        if let Some(id) = self.account().ils_user_id() {
            return Ok(id);
        }

        let ils_username = self.account().ils_username().to_string();

        let search = json::object! {
            usrname: ils_username.as_str(),
            deleted: "f",
        };

        let users = self.editor_mut().search("au", search)?;

        let user_id = match users.len() > 0 {
            true => self.parse_id(&users[0]["id"])?,
            false => Err(format!("No such user: {ils_username}"))?,
        };

        self.account_mut().set_ils_user_id(user_id);

        Ok(user_id)
    }

    fn login(&mut self) -> Result<(), String> {
        let ils_user_id = self.get_ils_user_id()?;
        let mut args = auth::AuthInternalLoginArgs::new(ils_user_id, "staff");

        if self.has_account() {
            if let Some(w) = self.account().workstation() {
                args.workstation = Some(w.to_string());
            }
        }

        let auth_ses = match auth::AuthSession::internal_session(&self.osrf_client, &args)? {
            Some(s) => s,
            None => panic!("Internal Login failed"),
        };

        self.editor.set_authtoken(auth_ses.token());

        // Set editor.requestor
        self.editor.checkauth()?;

        Ok(())
    }

    fn start(&mut self) -> Result<(), String> {
        log::debug!("{} starting", self);

        loop {
            // Blocks waiting for a SIP request to arrive
            let sip_req_op = self
                .sip_connection
                .recv_with_timeout(SIP_RECV_TIMEOUT)
                .or_else(|e| Err(format!("SIP recv() failed: {e}")))?;

            let sip_req = match sip_req_op {
                Some(r) => r,
                None => {
                    if self.shutdown {
                        break;
                    }
                    // Receive timed out w/ no value.  Go back
                    // and try again.
                    continue;
                }
            };

            log::trace!("{} Read SIP message: {:?}", self, sip_req);

            let sip_resp = self.handle_sip_request(&sip_req)?;

            log::trace!("{self} server replying with {sip_resp:?}");

            // Send the HTTP response back to the SIP client as a SIP message.
            self.sip_connection
                .send(&sip_resp)
                .or_else(|e| Err(format!("SIP send failed: {e}")))?;

            log::debug!("{} Successfully relayed response back to SIP client", self);
        }

        log::info!("{} shutting down", self);

        self.sip_connection.disconnect().ok();

        Ok(())
    }

    /// Send a SIP client request to the HTTP backend for processing.
    ///
    /// Blocks waiting for a response.
    fn handle_sip_request(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        let code = msg.spec().code;

        if code.eq("99") {
            return self.handle_sc_status(msg);
        } else if code.eq("93") {
            return self.handle_login(msg);
        }

        // All remaining request require authentication
        if self.account.is_none() {
            Err(format!("SIP client is not logged in"))?;
        }

        match code {
            "09" => self.handle_checkin(msg),
            "17" => self.handle_item_info(msg),
            "23" => self.handle_patron_status(msg),
            "63" => self.handle_patron_info(msg),
            _ => Err(format!("Unsupported SIP message code={}", msg.spec().code)),
        }
    }

    fn handle_login(&mut self, msg: &sip2::Message) -> Result<sip2::Message, String> {
        let username = msg
            .get_field_value("CN")
            .ok_or(format!("login() missing username"))?;

        let password = msg
            .get_field_value("CO")
            .ok_or(format!("login() missing password"))?;

        let account = match self.sip_config().get_account(&username) {
            Some(a) => a,
            None => Err(format!("No such account: {username}"))?,
        };

        let mut login_ok = sip2::util::num_bool(false);

        if account.sip_password().eq(&password) {
            login_ok = sip2::util::num_bool(true);
            self.account = Some(account.clone());
        } else {
            self.account = None;
        }

        Ok(sip2::Message::new(
            &sip2::spec::M_LOGIN_RESP,
            vec![sip2::FixedField::new(&sip2::spec::FF_OK, login_ok).unwrap()],
            Vec::new(),
        ))
    }

    fn handle_sc_status(&mut self, _msg: &sip2::Message) -> Result<sip2::Message, String> {
        if self.account.is_none() && !self.sip_config().sc_status_before_login() {
            Err(format!("SC Status before login disabled"))?;
        }

        let mut resp = sip2::Message::new(
            &sip2::spec::M_ACS_STATUS,
            vec![
                sip2::FixedField::new(&sip2::spec::FF_ONLINE_STATUS, "Y").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_CHECKIN_OK, "Y").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_CHECKOUT_OK, "Y").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_ACS_RENEWAL_POLICY, "Y").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_STATUS_UPDATE_OK, "N").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_OFFLINE_OK, "N").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_TIMEOUT_PERIOD, "999").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_RETRIES_ALLOWED, "999").unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_DATE, &sip2::util::sip_date_now()).unwrap(),
                sip2::FixedField::new(&sip2::spec::FF_PROTOCOL_VERSION, "2.00").unwrap(),
            ],
            Vec::new(),
        );

        resp.add_field("BX", INSTITUTION_SUPPORTS.join("").as_str());

        if let Some(a) = &self.account {
            resp.add_field("AO", a.settings().institution());
        }

        Ok(resp)
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session {}", self.sesid)
    }
}
