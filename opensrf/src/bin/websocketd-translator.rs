use getopts;
use opensrf::addr::{ClientAddress, RouterAddress, ServiceAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::init;
use opensrf::logging::Logger;
use opensrf::message;
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
//use signal_hook;

const MAX_ACTIVE_SESSIONS: usize = 1024;
const MAX_MESSAGE_SIZE: usize = 10485760; // ~10M
const MAX_THREAD_SIZE: usize = 256;
//const SHUTDOWN_POLL_INTERVAL: usize = 1;
//const MAX_GRACEFUL_SHUTDOWN_INTERVAL: usize = 120;
const WEBSOCKET_INGRESS: &str = "ws-translator-v3";

struct Translator {
    // True if a shutdown signal was received.
    stopping: Arc<AtomicBool>,

    config: conf::BusClient,

    // Address we use as the send/receive address for OpenSRF.
    // Sending and receiving occur in separate threads, so storing
    // the common address here allows us to easily share with
    // our worker threads.
    address: ClientAddress,
}

impl Translator {
    fn new(config: &conf::BusClient) -> Translator {
        let address = ClientAddress::new(config.domain().name());

        Translator {
            address,
            config: config.clone(),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    /*
    fn setup_signal_handlers(&self) -> Result<(), String> {
        // A signal will set our self.stopping flag to true

        if let Err(e) = signal_hook::flag::register(
            signal_hook::consts::SIGUSR1, self.stopping.clone()) {
            Err(format!("Cannot register signal handler: {e}"))?;
        }

        Ok(())
    }
    */

    fn run(&mut self) {
        let (tx, rx): (mpsc::Sender<SessionEvent>, mpsc::Receiver<SessionEvent>) = mpsc::channel();

        let mut inbound = InboundThread::new(
            self.config.clone(),
            self.address.clone(),
            rx,
            self.stopping.clone(),
        );

        let mut outbound = OutboundThread::new(
            self.config.clone(),
            self.address.clone(),
            tx,
            self.stopping.clone(),
        );

        let outbound_thread: thread::JoinHandle<()> = thread::spawn(move || {
            log::info!("Starting outbound thread");
            match outbound.run() {
                Ok(()) => log::info!("Outbound thread exited cleanly"),
                Err(e) => log::error!("Outbound thread exited with error: {e}"),
            }
        });

        let inbound_thread: thread::JoinHandle<()> = thread::spawn(move || {
            log::info!("Starting inbound thread");
            match inbound.run() {
                Ok(()) => log::info!("Inbound thread exited cleanly"),
                Err(e) => log::error!("Inbound thread exited with error: {e}"),
            }
        });

        if let Err(e) = inbound_thread.join() {
            log::error!("Inbound thread join() failed: {e:?}");
        }
        if let Err(e) = outbound_thread.join() {
            log::error!("Outbound thread join() failed: {e:?}");
        }
    }
}

/// Allows the outbound thread to tell the inbound thread when a
/// session should be tracked as connected.
#[derive(Debug, Clone)]
struct SessionEvent {
    thread: String,
    address: Option<ClientAddress>,
    add: bool,
}

/// Relay messages from STDIN (websocketd) to OpenSRF.
struct InboundThread {
    config: conf::BusClient,

    /// Source address of OpenSRF requests
    address: ClientAddress,

    /// Map of thread to backend worker addresses
    sessions: HashMap<String, ClientAddress>,

    /// Receive messages from the outbound thread here.
    to_inbound_rx: mpsc::Receiver<SessionEvent>,

    stopping: Arc<AtomicBool>,
}

impl InboundThread {
    fn new(
        config: conf::BusClient,
        address: ClientAddress,
        to_inbound_rx: mpsc::Receiver<SessionEvent>,
        stopping: Arc<AtomicBool>,
    ) -> Self {
        InboundThread {
            address,
            config,
            to_inbound_rx,
            stopping,
            sessions: HashMap::new(),
        }
    }

    fn run(&mut self) -> Result<(), String> {
        let mut bus = Bus::new(&self.config)?;

        loop {
            log::debug!("InboundThread awaiting STDIN data");

            // TODO check for active sessions / keepalive
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Inbound recv'ed shutdown request");
                return Ok(());
            }

            if let Err(e) = self.check_session_events() {
                Err(format!("Error reading session events. Exiting. {e}"))?
            }

            let mut buffer = String::new();
            if let Err(e) = io::stdin().read_line(&mut buffer) {
                Err(format!("Error reading STDIN.  Exiting. {e}"))?;
            }

            if buffer.len() > MAX_MESSAGE_SIZE {
                log::warn!(
                    "WS message is too large at {} chars. dropping",
                    buffer.len()
                );
                continue;
            }

            // TODO check for active sessions / keepalive
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Inbound recv'ed shutdown request");
                return Ok(());
            }

            if let Err(e) = self.check_session_events() {
                Err(format!("Error reading session events. Exiting. {e}"))?
            }

            if let Err(e) = self.relay_stdin_to_osrf(&mut bus, &buffer) {
                log::error!("Error processing websocket message: {e}");
                continue;
            }
        }
    }

    fn check_session_events(&mut self) -> Result<(), String> {
        loop {
            let event = match self.to_inbound_rx.try_recv() {
                Ok(e) => e,
                Err(e) => match e {
                    mpsc::TryRecvError::Empty => return Ok(()),
                    _ => Err(format!("Session events read error: {e}"))?,
                },
            };

            let thread = event.thread.to_owned();

            log::debug!("Inbound received session event: {event:?}");

            if event.add {

                if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
                    // Caller reached the max stateful sessions limit.
                    // Kick 'em.
                    Err(format!("Caller exceeds stateful sessions limit"))?;
                }

                let address = event.address.unwrap().to_owned();
                self.sessions.insert(thread, address);
            } else {
                self.sessions.remove(&thread);
            }
        }
    }

    fn relay_stdin_to_osrf(&mut self, bus: &mut Bus, msg: &str) -> Result<(), String> {
        let mut wrapper = match json::parse(msg) {
            Ok(w) => w,
            Err(e) => Err(format!("Cannot parse websocket message: {e} {msg}"))?,
        };

        let thread = wrapper["thread"].take();
        let log_xid = wrapper["log_xid"].take();
        let service = wrapper["service"].take();
        let mut msg_list = wrapper["osrf_msg"].take();

        let thread = thread.as_str().ok_or(format!("WS message has no 'thread' key"))?;

        if thread.len() > MAX_THREAD_SIZE {
            Err(format!("Thread exceeds max thread size; dropping"))?;
        }

        let service = match service.as_str() {
            Some(s) => s,
            // 'service' should always be set by the caller, but in
            // the off chance it's not and we can still proceed, we
            // need something to log.
            None => "_",
        };

        let mut send_to_router: Option<String> = None;

        let recipient = match self.sessions.get(thread) {
            Some(r) => {
                log::debug!("Found cached recipient for thread {thread} {r}");
                r.full().to_string()
            }
            None => {
                if service.eq("_") {
                    Err(format!("WS unable to determine recipient"))?
                }
                send_to_router = Some(RouterAddress::new(service).full().to_string());
                ServiceAddress::new(service).full().to_string()
            }
        };

        log::debug!("WS relaying message thread={thread} recipient={recipient}");

        // msg_list should be an array, but may be a single opensrf message.
        if !msg_list.is_array() {
            let mut list = json::JsonValue::new_array();

            if let Err(e) = list.push(msg_list) {
                Err(format!("Error creating message list {e}"))?;
            }

            msg_list = list;
        }

        let mut body_vec: Vec<message::Message> = Vec::new();

        for msg_json in msg_list.members() {
            let mut msg = match message::Message::from_json_value(msg_json) {
                Some(m) => m,
                None => Err(format!("Error creating message from {msg_json}"))?,
            };

            msg.set_ingress(WEBSOCKET_INGRESS);

            match msg.mtype() {
                message::MessageType::Connect => {
                    log::debug!("WS received CONNECT request: {thread}");
                }
                message::MessageType::Request => {
                    self.log_request(service, &msg)?;
                }
                message::MessageType::Disconnect => {
                    log::debug!("WS removing session on DISCONNECT: {thread}");
                    self.sessions.remove(thread);
                }
                _ => Err(format!(
                    "WS received unexpected message type: {}",
                    msg.mtype()
                ))?,
            }

            body_vec.push(msg);
        }

        let mut tm = message::TransportMessage::with_body_vec(
            &recipient,
            self.address.full(),
            thread,
            body_vec,
        );

        if let Some(xid) = log_xid.as_str() {
            tm.set_osrf_xid(xid);
        }

        if let Some(router) = send_to_router {
            bus.send_to(&tm, &router)?;
        } else {
            bus.send(&tm)?;
        }

        // TODO clear local log XID

        Ok(())
    }

    fn log_request(&self, service: &str, msg: &message::Message) -> Result<(), String> {
        let request = match msg.payload() {
            message::Payload::Method(m) => m,
            _ => Err(format!("WS received Request with no payload"))?,
        };

        // Create a string from the method parameters
        let logp = request
            .params()
            .iter()
            .map(|p| p.dump())
            .collect::<Vec<_>>()
            .join(", ");

        // TODO REDACT
        // TODO log the client IP address

        // Log the API call
        log::info!("{} {} {}", service, request.method(), logp);

        Ok(())
    }
}

/// Relay messages from OpenSRF to STDOUT (websocketd).
struct OutboundThread {
    config: conf::BusClient,

    /// Recipient address for OpenSRF responses.
    address: ClientAddress,

    /// For sending session connectivity info to the inbound thread.
    to_inbound_tx: mpsc::Sender<SessionEvent>,

    stopping: Arc<AtomicBool>,
}

impl OutboundThread {
    fn new(
        config: conf::BusClient,
        address: ClientAddress,
        to_inbound_tx: mpsc::Sender<SessionEvent>,
        stopping: Arc<AtomicBool>,
    ) -> Self {
        OutboundThread {
            config,
            address,
            stopping,
            to_inbound_tx,
        }
    }

    fn run(&mut self) -> Result<(), String> {
        let mut bus = Bus::new(&self.config)?;

        // Our bus-level address will not match the address of the
        // inbound thread bus.  Listen for responses on this agreed-
        // upon recipient address.
        let sent_to = self.address.full().to_string();

        loop {
            log::debug!("OutboundThread waiting for OpenSRF Responses");

            // TODO check for active sessions / keepalive
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Inbound recv'ed shutdown request");
                return Ok(());
            }

            let msg_op = match bus.recv(-1, Some(&sent_to)) {
                Ok(o) => o,
                Err(e) => {
                    log::error!("Fatal error bus.recv(): {e}");
                    continue;
                }
            };

            let tm = match msg_op {
                Some(tm) => tm,
                None => continue, // OK to receive no message
            };

            if let Err(e) = self.relay_osrf_to_stdout(&tm) {
                log::error!("Fatal error writing reply to STDOUT: {e}");
            }
        }
    }

    fn relay_osrf_to_stdout(&mut self, tm: &message::TransportMessage) -> Result<(), String> {
        let msg_list = tm.body();

        let mut body = json::JsonValue::new_array();
        let mut transport_error = false;

        for msg in msg_list.iter() {
            if let message::Payload::Status(s) = msg.payload() {
                if s.status() == &message::MessageStatus::Ok {
                    self.update_session_state(tm.thread(), true, Some(tm.from()))?;
                }

                if *s.status() as isize >= message::MessageStatus::BadRequest as isize {
                    transport_error = true;
                    log::error!("Request returned unexpected status: {:?}", msg);
                    self.update_session_state(tm.thread(), false, None)?;
                }
            }

            if let Err(e) = body.push(msg.to_json_value()) {
                Err(format!("Error building message response: {e}"))?;
            }
        }

        self.write_stdout(tm.thread(), body, transport_error)
    }

    fn update_session_state(
        &mut self,
        thread: &str,
        add: bool,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let mut address: Option<ClientAddress> = None;
        if let Some(a) = addr {
            match ClientAddress::from_string(a) {
                Ok(aa) => address = Some(aa),
                Err(e) => Err(format!("Invalid client address: {e} {a}"))?,
            }
        }

        let event = SessionEvent {
            add,
            address,
            thread: thread.to_string(),
        };

        if let Err(e) = self.to_inbound_tx.send(event) {
            Err(format!("Error reporting session status to inbound: {e}"))?;
        }

        Ok(())
    }

    fn write_stdout(
        &self,
        thread: &str,
        body: json::JsonValue,
        transport_error: bool,
    ) -> Result<(), String> {
        let mut obj = json::object! {
            // oxrf_xid: TODO
            thread: thread,
            osrf_msg: body
        };

        if transport_error {
            obj["transport_error"] = json::from(true);
        }

        if let Err(e) = io::stdout().write_all(obj.dump().as_bytes()) {
            Err(format!("Error writing to STDOUT: {e}"))?;
        }

        Ok(())
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("o", "origin", "Origin Domain", "ORIGIN");

    let initops = init::InitOptions { skip_logging: true };

    let (config, _) = init::init_with_more_options(&mut ops, &initops).unwrap();

    let config = config.into_shared();

    let gateway = match config.gateway() {
        Some(g) => g,
        None => panic!("No gateway configuration found"),
    };

    if let Err(e) = Logger::new(gateway.logging()).unwrap().init() {
        panic!("Error initializing logger: {}", e);
    }

    Translator::new(&gateway).run();
}
