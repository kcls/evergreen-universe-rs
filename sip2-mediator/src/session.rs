use super::conf;
use reqwest;
use serde_urlencoded as urlencoded;
use sip2;
use std::fmt;
use std::net;
use uuid::Uuid;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// How often do we wake up from blocking on our sip socket socket to check
/// for shutdown, etc. signals.
const SIG_POLL_INTERVAL: u64 = 5;

/// Max time we'll wait for a response to an HTTP request.
const DEFAULT_HTTP_REQUEST_TIMEOUT: u64 = 60;

/// Manages the connection between a SIP client and the HTTP backend.
pub struct Session {
    sip_connection: sip2::Connection,

    /// Unique session identifier
    key: String,

    /// SIP login; useful or logging.
    sip_user: Option<String>,

    /// E.g. https://localhost/sip2-mediator
    http_url: String,

    http_client: reqwest::blocking::Client,

    /// If true, we're shutting down.
    shutdown: Arc<AtomicBool>,
}

impl Session {
    /// Our thread starts here.  If anything fails, we just log it and
    /// go away so as not to disrupt the main server thread.
    pub fn run(config: conf::Config, stream: net::TcpStream, shutdown: Arc<AtomicBool>) {
        match stream.peer_addr() {
            Ok(a) => log::info!("New SIP connection from {a}"),
            Err(e) => {
                log::error!("SIP connection has no peer addr? {e}");
                return;
            }
        }

        let key = Uuid::new_v4().as_simple().to_string()[..16].to_string();

        let http_builder = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(config.ignore_ssl_errors)
            .timeout(Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT));

        let http_client = match http_builder.build() {
            Ok(c) => c,
            Err(e) => {
                log::error!("Error building HTTP client: {e}; exiting");
                return;
            }
        };

        let mut con = sip2::Connection::from_stream(stream);
        con.set_ascii(config.ascii);

        let mut ses = Session {
            key,
            shutdown,
            http_url: config.http_url.to_string(),
            http_client,
            sip_connection: con,
            sip_user: None,
        };

        ses.start();
    }

    fn start(&mut self) {
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
            let sip_resp = match self.http_round_trip(&sip_req) {
                Ok(r) => r,
                _ => {
                    log::error!("{self} Error processing SIP request. Session exiting");
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
        self.send_end_session();
    }

    /// Send the final End Session (XS) message to the HTTP backend.
    ///
    /// Response and errors are ignored since this is the final step
    /// in the session shuting down.
    fn send_end_session(&self) {
        log::trace!("{} sending end of session message to HTTP backend", self);

        let msg_spec = sip2::spec::Message::from_code("XS").unwrap();

        let msg = sip2::Message::new(&msg_spec, vec![], vec![]);

        self.http_round_trip(&msg).ok();
    }

    /// Send a SIP client request to the HTTP backend for processing.
    ///
    /// Blocks waiting for a response.
    fn http_round_trip(&self, msg: &sip2::Message) -> Result<sip2::Message, ()> {
        let msg_json = match msg.to_json() {
            Ok(m) => m,
            Err(e) => {
                log::error!("{} Failed translating SIP message to JSON: {}", self, e);
                return Err(());
            }
        };

        log::debug!("{self} posting message: {msg_json}");

        let values = [("session", &self.key), ("message", &msg_json)];

        let body = match urlencoded::to_string(&values) {
            Ok(m) => m,
            Err(e) => {
                log::error!("{}, Error url-encoding SIP message: {}", self, e);
                return Err(());
            }
        };

        log::trace!("{self} Posting content: {body}");

        let request = self
            .http_client
            .post(&self.http_url)
            .header(reqwest::header::CONNECTION, "keep-alive")
            .body(body);

        let res = match request.send() {
            Ok(v) => v,
            Err(e) => {
                log::error!("{self} HTTP request failed : {e}");
                return Err(());
            }
        };

        if res.status() != 200 {
            log::error!(
                "{} HTTP server responded with a non-200 status: status={} res={:?}",
                self,
                res.status(),
                res
            );
            return Err(());
        }

        log::debug!("{self} HTTP response status: {}", res.status());

        let msg_json: String = match res.text() {
            Ok(v) => v,
            Err(e) => {
                log::error!("{self} HTTP response failed to ready body text: {}", e);
                return Err(());
            }
        };

        log::debug!("{self} HTTP response JSON: {msg_json}");

        match sip2::Message::from_json(&msg_json) {
            Ok(m) => Ok(m),
            Err(e) => {
                log::error!("{} http_round_trip from_json error: {}", self, e);
                return Err(());
            }
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
