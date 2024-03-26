use super::conf;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use sip2;
use std::fmt;
use std::net;
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

    /// OpenSRF client.
    client: eg::Client,

    /// If true, we're shutting down.
    shutdown: Arc<AtomicBool>,
}

impl Session {
    /// Our thread starts here.  If anything fails, we just log it and
    /// exit.
    pub fn new(
        sip_config: Arc<conf::Config>,
        osrf_config: Arc<eg::osrf::conf::Config>,
        osrf_bus: eg::osrf::bus::Bus,
        stream: net::TcpStream,
        shutdown: Arc<AtomicBool>,
    ) -> EgResult<Session> {
        match stream.peer_addr() {
            Ok(a) => log::info!("New SIP connection from {a}"),
            Err(e) => return Err(format!("SIP connection has no peer addr? {e}").into()),
        }

        let key = eg::util::random_number(16);

        let mut con = sip2::Connection::from_stream(stream);
        con.set_ascii(sip_config.ascii);

        let client = eg::Client::from_bus(osrf_bus, osrf_config);

        let ses = Session {
            key,
            shutdown,
            client,
            sip_connection: con,
            sip_user: None,
        };

        Ok(ses)
    }

    pub fn start(&mut self) -> EgResult<()> {
        log::debug!("{self} starting");

        loop {
            // Blocks waiting for a SIP request to arrive or for the
            // poll interval to timeout.
            let sip_req_op = match self.sip_connection.recv_with_timeout(SIG_POLL_INTERVAL) {
                Ok(msg_op) => msg_op,
                Err(e) => {
                    return Err(
                        format!("{self} SIP receive exited early; ending session: [{e}]").into(),
                    )
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
            let sip_resp = self.osrf_round_trip(&sip_req)?;

            log::trace!("{self} HTTP server replied with {sip_resp:?}");

            // Send the HTTP response back to the SIP client as a SIP message.
            if let Err(e) = self.sip_connection.send(&sip_resp) {
                return Err(format!("Error sending SIP resonse: {e}").into());
            }

            log::debug!("{self} Successfully relayed response back to SIP client");

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("{self} Shutdown signal received, exiting listen loop");
                break;
            }
        }

        log::info!("{self} shutting down");

        self.sip_connection.disconnect().ok();

        // Tell the Evergreen server our session is done.
        self.send_end_session()
    }

    /// Send the final End Session (XS) message to the ILS.
    ///
    /// Response and errors are ignored since this is the final step
    /// in the session shuting down.
    fn send_end_session(&mut self) -> EgResult<()> {
        log::trace!("{} sending end of session message to the ILS", self);

        let msg_spec = sip2::spec::Message::from_code("XS").unwrap();

        let msg = sip2::Message::new(&msg_spec, vec![], vec![]);

        self.osrf_round_trip(&msg).map(|_| ())
    }

    /// Send a SIP client request to the ILS backend for processing.
    ///
    /// Blocks waiting for a response.
    fn osrf_round_trip(&mut self, msg: &sip2::Message) -> EgResult<sip2::Message> {
        let msg_json = match msg.to_json_value() {
            Ok(m) => m,
            Err(e) => {
                return Err(format!("{self} Failed translating SIP message to JSON: {e}").into())
            }
        };

        log::debug!("{self} posting message: {msg_json}");

        let msg_val = EgValue::from_json_value(msg_json)?;

        let params = vec![EgValue::from(self.key.as_str()), msg_val];

        let response = self
            .client
            .send_recv_one("open-ils.sip2", "open-ils.sip2.request", params)?
            .ok_or_else(|| format!("{self} no response received"))?;

        log::debug!("{self} ILS response JSON: {response}");

        match sip2::Message::from_json_value(&response.into()) {
            Ok(m) => Ok(m),
            Err(e) => Err(format!("{self} error translating JSON to SIP: {e}").into()),
        }
    }

    /// Gives the bus connection back to the server so it may be
    /// reused by another session.
    pub fn take_bus(&mut self) -> eg::osrf::bus::Bus {
        self.client.take_bus()
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
