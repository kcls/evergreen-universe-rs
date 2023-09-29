use super::conf;
use eg::auth;
use eg::auth::AuthSession;
use eg::result::EgResult;
use evergreen as eg;
use opensrf as osrf;
use sip2;
use std::collections::HashMap;
use std::fmt;
use std::net;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/* --------------------------------------------------------- */
// By order of appearance in the INSTITUTION_SUPPORTS string:
// patron status request
// checkout
// checkin
// block patron
// acs status
// request sc/acs resend
// login
// patron information
// end patron session
// fee paid
// item information
// item status update
// patron enable
// hold
// renew
// renew all
const INSTITUTION_SUPPORTS: &str = "YYYNYNYYNYYNNNYN";
/* --------------------------------------------------------- */

/// Manages a single SIP client connection.
///
/// May process multiple connections over time.
pub struct Session {
    sip_connection: sip2::Connection,

    /// If true, the server is shutting down, so we should exit.
    shutdown: Arc<AtomicBool>,

    sip_config: Arc<conf::Config>,

    /// Created in worker_start.
    osrf_client: osrf::Client,

    /// Used for pulling trivial data from Evergreen, i.e. no API required.
    ///
    /// Created at the beginning of each client session, then discarded.
    editor: eg::editor::Editor,

    /// SIP account, set after the client logs in.
    account: Option<conf::SipAccount>,

    /// Cache of org unit shortnames and IDs.
    org_cache: HashMap<i64, json::JsonValue>,
}

impl Session {

    pub fn new(
        sip_config: Arc<conf::Config>,
        osrf_conf: Arc<osrf::conf::Config>,
        osrf_bus: osrf::bus::Bus,
        idl: Arc<eg::idl::Parser>,
        stream: net::TcpStream,
        shutdown: Arc<AtomicBool>,
        org_cache: HashMap<i64, json::JsonValue>
    ) -> Self {
        if let Ok(a) = stream.peer_addr() {
            log::info!("New SIP connection from {a}");
        }

        let mut con = sip2::Connection::from_stream(stream);
        con.set_ascii(sip_config.ascii());

        let osrf_client = osrf::Client::from_bus(osrf_bus, osrf_conf);

        osrf_client.set_serializer(eg::idl::Parser::as_serializer(&idl));

        let editor = eg::Editor::new(&osrf_client, &idl);

        Session {
            editor,
            shutdown,
            sip_config,
            osrf_client,
            org_cache,
            account: None,
            sip_connection: con,
        }
    }

    /// Panics if our client has no bus.  Use with caution and only
    /// after this Session has completed.
    pub fn take_bus(&mut self) -> osrf::bus::Bus {
        self.osrf_client.take_bus()
    }

    pub fn org_cache(&self) -> &HashMap<i64, json::JsonValue> {
        &self.org_cache
    }

    pub fn org_cache_mut(&mut self) -> &mut HashMap<i64, json::JsonValue> {
        &mut self.org_cache
    }

    /// True if our SIP client has successfully logged in.
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

    /// Verifies the existing authtoken if present, requesting a new
    /// authtoken when necessary.
    ///
    /// Returns Err if we fail to verify the token or login as needed.
    pub fn set_authtoken(&mut self) -> EgResult<()> {
        if self.editor.authtoken().is_some() {
            // If we have an authtoken, verify it's still valid.
            if self.editor.checkauth()? {
                return Ok(());
            } else {
                // Stale authtoken.  Remove it.
                AuthSession::logout(&self.osrf_client, self.authtoken()?)?;
            }
        }

        self.login()
    }

    pub fn authtoken(&self) -> EgResult<&str> {
        match self.editor().authtoken() {
            Some(a) => Ok(a),
            None => Err(format!("Authtoken is unset").into()),
        }
    }

    /// Find the ID of the ILS user account whose username matches
    /// the ILS username for our SIP account.
    ///
    /// Cache the user id after the first lookup.
    fn get_ils_user_id(&mut self) -> EgResult<i64> {
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
            true => eg::util::json_int(&users[0]["id"])?,
            false => Err(format!("No such user: {ils_username}"))?,
        };

        self.account_mut().set_ils_user_id(user_id);

        Ok(user_id)
    }

    /// Create a internal auth session in the ILS
    fn login(&mut self) -> EgResult<()> {
        let ils_user_id = self.get_ils_user_id()?;
        let mut args = auth::AuthInternalLoginArgs::new(ils_user_id, "staff");

        if self.has_account() {
            if let Some(w) = self.account().workstation() {
                args.workstation = Some(w.to_string());
            }
        }

        let auth_ses = match AuthSession::internal_session(&self.osrf_client, &args)? {
            Some(s) => s,
            None => Err(format!("Internal Login failed"))?,
        };

        self.editor.set_authtoken(auth_ses.token());

        // Set editor.requestor
        self.editor.checkauth()?;

        Ok(())
    }

    /// Wait for SIP requests in a loop and send replies.
    ///
    /// Exits when the shutdown signal is set or on unrecoverable error.
    pub fn start(&mut self) -> EgResult<()> {
        log::debug!("{self} starting");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("{self} Shutdown notice received, exiting listen loop");
                break;
            }

            let sip_req_op = self
                .sip_connection
                .recv_with_timeout(conf::SIP_SHUTDOWN_POLL_INTERVAL)
                .or_else(|e| Err(format!("{self} SIP recv() failed: {e}")))?;

            let sip_req = match sip_req_op {
                Some(r) => r,
                None => continue,
            };

            log::trace!("{self} Read SIP message: {:?}", sip_req);

            let mut sip_resp = self.handle_sip_request(&sip_req)?;

            log::trace!("{self} server replying with {sip_resp:?}");

            self.redact_sip_response(&mut sip_resp);

            log::trace!("{self} server response after redaction: {sip_resp:?}");

            // Send the SIP response back to the SIP client
            self.sip_connection
                .send(&sip_resp)
                .or_else(|e| Err(format!("SIP send failed: {e}")))?;

            log::debug!("{self} Successfully relayed response back to SIP client");
        }

        log::info!("{self} shutting down");

        self.sip_connection.disconnect().ok();

        if self.authtoken().is_ok() {
            AuthSession::logout(&self.osrf_client, self.authtoken()?).ok();
        }

        // Remove any cruft we may have left on the bus.
        self.osrf_client.clear()?;

        Ok(())
    }

    fn redact_sip_response(&self, resp: &mut sip2::Message) {
        if !self.has_account() {
            // Can happen if this is a pre-log SC response.
            return;
        }

        for filter in self.account().settings().field_filters() {
            if let Some(replacement) = filter.replace_with() {
                for field in resp
                    .fields_mut()
                    .iter_mut()
                    .filter(|f| f.code().eq(filter.field_code()))
                {
                    field.set_value(replacement);
                }
            } else {
                loop {
                    // Keep deleting till we got em all
                    let pos = match resp
                        .fields()
                        .iter()
                        .position(|f| f.code().eq(filter.field_code()))
                    {
                        Some(p) => p,
                        None => break, // got them all
                    };
                    resp.fields_mut().remove(pos);
                }
            }
        }
    }

    /// Process a single SIP request.
    fn handle_sip_request(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        let code = msg.spec().code;

        if code.eq("99") {
            // May not require an existing login / account
            return self.handle_sc_status(msg);
        }

        if code.eq("93") {
            // Create a login / account
            return self.handle_login(msg);
        }

        // All remaining request require authentication
        if self.account.is_none() {
            Err(format!("SIP client is not logged in"))?;
        }

        match code {
            "09" => self.handle_checkin(msg),
            "11" => self.handle_checkout(msg),
            "17" => self.handle_item_info(msg),
            "23" => self.handle_patron_status(msg),
            "35" => self.handle_end_patron_session(msg),
            "37" => self.handle_payment(msg),
            "63" => self.handle_patron_info(msg),
            _ => Err(format!("Unsupported SIP message code={}", msg.spec().code).into()),
        }
    }

    fn handle_login(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        self.account = None;
        let mut login_ok = "0";

        if let Some(username) = msg.get_field_value("CN") {
            if let Some(password) = msg.get_field_value("CO") {
                // Caller sent enough values to attempt login

                if let Some(account) = self.sip_config().get_account(&username) {
                    if account.sip_password().eq(&password) {
                        login_ok = "1";
                        self.account = Some(account.clone());
                    }
                } else {
                    log::warn!("No such SIP account: {username}");
                }
            } else {
                log::warn!("Login called with no password");
            }
        } else {
            log::warn!("Login called with no username");
        }

        Ok(sip2::Message::from_ff_values(&sip2::spec::M_LOGIN_RESP, &[login_ok]).unwrap())
    }

    fn handle_sc_status(&mut self, _msg: &sip2::Message) -> EgResult<sip2::Message> {
        if self.account.is_none() && !self.sip_config().sc_status_before_login() {
            Err(format!("SC Status before login disabled"))?;
        }

        let mut resp = sip2::Message::from_values(
            &sip2::spec::M_ACS_STATUS,
            &[
                "Y",   // online status
                "Y",   // checkin ok
                "Y",   // checkout ok
                "Y",   // renewal policy
                "N",   // status update
                "N",   // offline ok
                "999", // timeout
                "999", // max retries
                &sip2::util::sip_date_now(),
                "2.00", // SIP version
            ],
            &[("BX", INSTITUTION_SUPPORTS)],
        )
        .unwrap();

        if let Some(a) = &self.account {
            resp.add_field("AO", a.settings().institution());

            if a.settings().sc_status_library_info() {
                // This sets the requestor value on our editor so we can
                // find its workstation / home org.
                self.set_authtoken()?;

                if let Some(org) = self.org_from_id(self.get_ws_org_id()?)? {
                    resp.add_field("AM", org["name"].as_str().unwrap());
                    resp.add_field("AN", org["shortname"].as_str().unwrap());
                }
            }
        }

        Ok(resp)
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref acct) = self.account {
            write!(f, "SIPSession({})", acct.sip_username())
        } else {
            write!(f, "SIPSession")
        }
    }
}
