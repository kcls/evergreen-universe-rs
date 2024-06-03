use super::conf;
use eg::EgEvent;
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

// TODO make configurable?
//const EG_SERVICE: &str = "open-ils.sip2";
//const EG_METHOD: &str = "open-ils.sip2.request";
const EG_SERVICE: &str = "open-ils.rs-sip2";
const EG_METHOD: &str = "open-ils.rs-sip2.request";

/// Manages the connection between a SIP client and the Evergreen backend.
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
    /// Create a new Session
    ///
    /// At this point we are already running within our own thread.
    pub fn new(
        sip_config: Arc<conf::Config>,
        osrf_bus: eg::osrf::bus::Bus,
        stream: net::TcpStream,
        shutdown: Arc<AtomicBool>,
    ) -> EgResult<Session> {
        match stream.peer_addr() {
            Ok(a) => log::info!("New SIP connection from {a}"),
            Err(e) => return Err(format!("SIP connection has no peer addr? {e}").into()),
        }

        // Random session key string
        let key = eg::util::random_number(16);

        let mut con = sip2::Connection::from_stream(stream);
        con.set_ascii(sip_config.ascii);

        let client = eg::Client::from_bus(osrf_bus);

        let ses = Session {
            key,
            shutdown,
            client,
            sip_connection: con,
            sip_user: None,
        };

        Ok(ses)
    }

    /// /// Go into the main listen loop.
    /// Go into the main listen loop
    pub fn start(&mut self) -> EgResult<()> {
        log::debug!("{self} starting");

        loop {
            // Blocks waiting for a SIP request to arrive or for the
            // poll interval to timeout.
            let sip_req_op = match self.sip_connection.recv_with_timeout(SIG_POLL_INTERVAL) {
                Ok(msg_op) => msg_op,
                Err(e) => {
                    // We'll end up here if the client disconnects.
                    // Exit the listen loop and cleanup.
                    log::debug!("{self} SIP receive exited early; ending session: [{e}]");
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
                // for improved session logging.
                if let Some(sip_user) = sip_req.get_field_value("CN") {
                    self.sip_user = Some(sip_user.to_string());
                }
            }

            // Relay the request to the Evergreen backend and wait for a
            // response.  If an error occurs, all we can do is exit and
            // cleanup, since SIP has no concept of an error response.
            let sip_resp = match self.osrf_round_trip(sip_req) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{self} error routing ILS message: {e}");
                    break;
                }
            };

            log::trace!("{self} EG server replied with {sip_resp:?}");

            // Send the response back to the SIP client as a SIP message.
            // If there's an error, exit and cleanup.
            if let Err(e) = self.sip_connection.send(&sip_resp) {
                log::error!("{self} error sending response to SIP client: {e}");
                break;
            }

            log::debug!("{self} Successfully relayed response back to SIP client");

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("{self} Shutdown signal received, exiting listen loop");
                break;
            }
        }

        log::info!("{self} cleaning up and exiting");

        // Might already be disconnected
        self.sip_connection.disconnect().ok();

        // Tell the Evergreen server our session is done.
        self.send_end_session()
    }

    /// Send the final End Session (XS) message to the ILS.
    ///
    /// Response and errors are ignored since this is the final step
    /// in the session shuting down.
    fn send_end_session(&mut self) -> EgResult<()> {
        log::debug!("{self} sending end of session message to the ILS");

        let msg_spec = sip2::spec::Message::from_code("XS").unwrap();

        let msg = sip2::Message::new(&msg_spec, vec![], vec![]);

        self.osrf_round_trip(msg).map(|_| ())
    }

    /// Send a SIP client request to the ILS backend for processing.
    ///
    /// Blocks waiting for a response.
    fn osrf_round_trip(&mut self, msg: sip2::Message) -> EgResult<sip2::Message> {
        let msg_json = msg.to_json_value();

        log::debug!("{self} posting message: {msg_json}");

        let msg_val = EgValue::from_json_value(msg_json)?;

        let params = vec![EgValue::from(self.key.as_str()), msg_val];

        // Uses the default request timeout (probably 60 seconds).
        let response = self
            .client
            .send_recv_one(EG_SERVICE, EG_METHOD, params)?
            .ok_or_else(|| format!("{self} no response received"))?;

        log::debug!("{self} ILS response JSON: {response}");

        if let Some(evt) = EgEvent::parse(&response) {
            return Err(format!("SIP request failed with event: {evt}").into());
        }

        match sip2::Message::from_json_value(response.into()) {
            Ok(m) => Ok(m),
            Err(e) => Err(format!("{self} error translating JSON to SIP: {e}").into()),
        }
    }

    /// Gives the bus connection back to the worker thread so it may be
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
