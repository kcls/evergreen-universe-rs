use super::conf;
use log::{debug, error, info, trace};
use reqwest;
use serde_urlencoded as urlencoded;
use sip2;
use std::fmt;
use std::net;
use uuid::Uuid;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Manages the connection between a SIP client and the HTTP backend.
pub struct Session {
    sip_connection: sip2::Connection,

    /// Unique session identifier
    key: String,

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
            Ok(a) => info!("New SIP connection from {}", a),
            Err(e) => {
                error!("SIP connection has no peer addr? {}", e);
                return;
            }
        }

        let key = Uuid::new_v4().as_simple().to_string()[0..16].to_string();

        let http_builder = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(config.ignore_ssl_errors);

        let http_client = match http_builder.build() {
            Ok(c) => c,
            Err(e) => {
                error!("Error building HTTP client: {}; exiting", e);
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
        };

        ses.start();
    }

    fn start(&mut self) {
        debug!("{} starting", self);

        loop {
            // Blocks waiting for a SIP request to arrive
            let sip_req = match self.sip_connection.recv() {
                Ok(sm) => sm,
                Err(e) => {
                    error!("{} SIP receive exited early; ending session: [{}]", self, e);
                    break;
                }
            };

            trace!("{} Read SIP message: {:?}", self, sip_req);

            // Relay the request to the HTTP backend and wait for a response.
            let sip_resp = match self.http_round_trip(&sip_req) {
                Ok(r) => r,
                _ => {
                    error!("{} Error processing SIP request. Session exiting", self);
                    break;
                }
            };

            log::trace!("{self} HTTP server replied with {sip_resp:?}");

            // Send the HTTP response back to the SIP client as a SIP message.
            if let Err(e) = self.sip_connection.send(&sip_resp) {
                error!(
                    "{} Error relaying response back to SIP client: {}. shutting down session",
                    self, e
                );
                break;
            }

            debug!("{} Successfully relayed response back to SIP client", self);

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("Shutdown signal received, exiting listen loop");
                break;
            }
        }

        info!("{} shutting down", self);

        self.sip_connection.disconnect().ok();

        // Tell the HTTP back-end our session is done.
        self.send_end_session();
    }

    /// Send the final End Session (XS) message to the HTTP backend.
    ///
    /// Response and errors are ignored since this is the final step
    /// in the session shuting down.
    fn send_end_session(&self) {
        trace!("{} sending end of session message to HTTP backend", self);

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
                error!("{} Failed translating SIP message to JSON: {}", self, e);
                return Err(());
            }
        };

        let values = [("session", &self.key), ("message", &msg_json)];

        let body = match urlencoded::to_string(&values) {
            Ok(m) => m,
            Err(e) => {
                error!("{}, Error url-encoding SIP message: {}", self, e);
                return Err(());
            }
        };

        trace!("{} Posting content: {}", self, body);

        let request = self
            .http_client
            .post(&self.http_url)
            .header(reqwest::header::CONNECTION, "keep-alive")
            .body(body);

        let res = match request.send() {
            Ok(v) => v,
            Err(e) => {
                error!("{} HTTP request failed : {}", self, e);
                return Err(());
            }
        };

        if res.status() != 200 {
            error!(
                "{} HTTP server responded with a non-200 status: status={} res={:?}",
                self,
                res.status(),
                res
            );
            return Err(());
        }

        debug!("{} HTTP response status: {}", self, res.status());

        let msg_json: String = match res.text() {
            Ok(v) => v,
            Err(e) => {
                error!("{} HTTP response failed to ready body text: {}", self, e);
                return Err(());
            }
        };

        debug!("{} HTTP response JSON: {}", self, msg_json);

        match sip2::Message::from_json(&msg_json) {
            Ok(m) => Ok(m),
            Err(e) => {
                error!("{} http_round_trip from_json error: {}", self, e);
                return Err(());
            }
        }
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session {}", self.key)
    }
}
