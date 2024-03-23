use evergreen as eg;
use eg::Client;
use eg::EgResult;
use eg::EgValue;
use super::conf;
use sip2;
use std::fmt;
use std::net;
use uuid::Uuid;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// How often do we wake up from blocking on our sip socket socket to check
/// for shutdown, etc. signals.
const SIG_POLL_INTERVAL: u64 = 5;

/// Manages the connection between a SIP client and the HTTP backend.
pub struct Session {
    sip_connection: sip2::Connection,

    /// Unique session identifier
    key: String,

    /// SIP login; useful or logging.
    sip_user: Option<String>,

    /// If true, we're shutting down.
    shutdown: Arc<AtomicBool>,
}

impl Session {
    /// Our thread starts here.  If anything fails, we just log it and
    /// exit.
    pub fn run(config: conf::Config, stream: net::TcpStream, shutdown: Arc<AtomicBool>) {
        let options = eg::init::InitOptions {
            skip_logging: true,
            skip_host_settings: true,
            appname: None,
        };

        // We don't need all the Evergreen stuff.. just a bus connection.
        let osrf_config = match eg::init::osrf_init(&options) {
            Ok(c) => c,
            Err(e) => {
                log::error!("Cannot init OpenSRF: {e}");
                return;
            }
        };

        let client = match Client::connect(osrf_config.into_shared()) {
            Ok(c) => c,
            Err(e) => {
                log::error!("Cannot connect to Evergreen: {e}");
                return;
            }
        };

        match stream.peer_addr() {
            Ok(a) => log::info!("New SIP connection from {a}"),
            Err(e) => {
                log::error!("SIP connection has no peer addr? {e}");
                return;
            }
        }

        let key = Uuid::new_v4().as_simple().to_string()[..16].to_string();

        let mut con = sip2::Connection::from_stream(stream);
        con.set_ascii(config.ascii);

        let mut ses = Session {
            key,
            shutdown,
            sip_connection: con,
            sip_user: None,
        };

        ses.start(client);
    }

    fn start(&mut self, mut client: Client) {
        log::debug!("{} starting", self);

        loop {
            // Blocks waiting for a SIP request to arrive or for the
            // poll interval to timeout.
            let sip_req_op = match self.sip_connection.recv_with_timeout(SIG_POLL_INTERVAL) {
                Ok(msg_op) => msg_op,
                Err(e) => {
                    log::error!("{self} SIP receive exited early; ending session: [{e}]");
                    break;
                }
            };

            let sip_req = match sip_req_op {
                Some(r) => r,
                None => {
                    // Woke up from blocking to check signals.  Check 'em.
                    if self.shutdown.load(Ordering::Relaxed) {
                        log::debug!("Shutdown signal received, exiting listen loop");
                        break;
                    }

                    // Go back and start listenting again.
                    continue;
                }
            };

            log::trace!("{} Read SIP message: {:?}", self, sip_req);

            if sip_req.spec() == &sip2::spec::M_LOGIN {
                // If this is a login request, capture the SIP username
                // for session logging.
                if let Some(sip_user) = sip_req.get_field_value("CN") {
                    self.sip_user = Some(sip_user.to_string());
                }
            }

            // Relay the request to the HTTP backend and wait for a response.
            let sip_resp = match self.osrf_round_trip(&mut client, &sip_req) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{self} Error processing SIP request. Session exiting: {e}");
                    break;
                }
            };

            log::trace!("{self} HTTP server replied with {sip_resp:?}");

            // Send the HTTP response back to the SIP client as a SIP message.
            if let Err(e) = self.sip_connection.send(&sip_resp) {
                log::error!(
                    "{self} Error relaying response back to SIP client: {e}. shutting down session"
                );
                break;
            }

            log::debug!("{self} Successfully relayed response back to SIP client");

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("{self} Shutdown signal received, exiting listen loop");
                break;
            }
        }

        log::info!("{self} shutting down");

        self.sip_connection.disconnect().ok();

        // Tell the HTTP back-end our session is done.
        self.send_end_session(&mut client);
    }

    /// Send the final End Session (XS) message to the HTTP backend.
    ///
    /// Response and errors are ignored since this is the final step
    /// in the session shuting down.
    fn send_end_session(&self, client: &mut Client) {
        log::trace!("{} sending end of session message to HTTP backend", self);

        let msg_spec = sip2::spec::Message::from_code("XS").unwrap();

        let msg = sip2::Message::new(&msg_spec, vec![], vec![]);

        self.osrf_round_trip(client, &msg).ok();
    }

    /// Send a SIP client request to the HTTP backend for processing.
    ///
    /// Blocks waiting for a response.
    fn osrf_round_trip(&self, client: &mut Client, msg: &sip2::Message) -> EgResult<sip2::Message> {
        let msg_json = match msg.to_json_value() {
            Ok(m) => m,
            Err(e) => return Err(format!(
                "{self} Failed translating SIP message to JSON: {e}").into()),
        };

        log::debug!("{self} posting message: {msg_json}");

        let msg_val = EgValue::from_json_value(msg_json)?;

        let params = vec! [
            EgValue::from(self.key.as_str()),
            msg_val,
        ];

        let response = client.send_recv_one(
            "open-ils.sip2", "open-ils.sip2.request", params
        )?.ok_or_else(|| format!("{self} no response received"))?;

        log::debug!("{self} HTTP response JSON: {response}");

        match sip2::Message::from_json_value(&response.into()) {
            Ok(m) => Ok(m),
            Err(e) => Err(format!("{self} error translating JSON to SIP: {e}").into()),
        }
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(sip_user) = self.sip_user.as_ref() {
            write!(f, "Ses {} [{sip_user}]", self.key)
        } else {
            write!(f, "Ses {}", self.key)
        }
    }
}
