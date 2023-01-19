use std::io;
use std::io::Write;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::mpsc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use getopts;
use signal_hook;
use opensrf::addr::{BusAddress, ClientAddress, RouterAddress, ServiceAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::init;
use opensrf::logging::Logger;
use opensrf::message;

const MAX_ACTIVE_SESSIONS: usize = 1024;
const MAX_MESSAGE_SIZE: usize = 10485760; // ~10M
const MAX_THREAD_SIZE: usize = 256;
const SHUTDOWN_POLL_INTERVAL: usize = 1;
const MAX_GRACEFUL_SHUTDOWN_INTERVAL: usize = 120;
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

        //osrf_thread: thread::JoinHandle<()>,
        //websocket_thread: thread::JoinHandle<()>,
    }
}

#[derive(Debug, Clone)]
struct SessionEvent {
    thread: String,
    address: Option<ClientAddress>,
    add: bool,
}


/// Relay messages from STDIN (websocketd) to OpenSRF.
struct InboundThread {
    config: Arc<conf::Config>,

    /// Source address of OpenSRF requests
    address: ClientAddress,

    // Map of thread to backend worker addresses
    sessions: HashMap<String, ClientAddress>,

    to_inbound_rx: mpsc::Receiver<SessionEvent>,
}

impl InboundThread {
    /*
    to_inbound_tx: mpsc::Sender<WorkerStateEvent>,
    to_inbound_rx: mpsc::Receiver<WorkerStateEvent>,
    */

    fn new(
        config: Arc<conf::Config>,
        address: ClientAddress,
        to_inbound_rx: mpsc::Receiver<SessionEvent>) -> Self {

        InboundThread {
            address,
            config,
            to_inbound_rx,
            sessions: HashMap::new(),
        }
    }

    fn run(&mut self) -> Result<(), String> {
        let conf = self.config.gateway().unwrap(); // known good
        let mut bus = Bus::new(&conf)?;

        loop {

            log::debug!("InboundThread awaiting STDIN data");

            let mut buffer = String::new();
            if let Err(e) = io::stdin().read_line(&mut buffer) {
                Err(format!("Error reading STDIN.  Exiting. {e}"))?;
            }

            if buffer.len() > MAX_MESSAGE_SIZE {
                log::warn!("WS message is too large at {} chars. dropping", buffer.len());
                continue;
            }

            if let Err(e) = self.relay_stdin_to_osrf(&mut bus, &buffer) {
                log::error!("Error processing websocket message: {e}");
                continue;
            }
        }
    }

    fn relay_stdin_to_osrf(&mut self, bus: &mut Bus, msg: &str) -> Result<(), String> {

        let wrapper = json::parse(msg).or_else(|e|
            Err(format!("Cannot parse websocket message: {e} {msg}")))?;

        let osrf_msg = wrapper["osrf_msg"].as_str().ok_or(
            format!("WS message has no 'osrf_msg' key"))?;

        let thread = wrapper["thread"].as_str().ok_or(
            format!("WS message has no 'thread' key"))?;

        if thread.len() > MAX_THREAD_SIZE {
            Err(format!("Thread exceeds max thread size; dropping"))?;
        }

        let log_xid_op = wrapper["log_xid"].as_str(); // TODO

        let service = match wrapper["service"].as_str() {
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
            },
            None => {
                if service.eq("_") {
                    Err(format!("WS unable to determine recipient"))?
                }
                send_to_router = Some(RouterAddress::new(service).full().to_string());
                ServiceAddress::new(service).full().to_string()
            }
        };

        log::debug!("WS relaying message thread={thread} recipient={recipient}");

        let mut message_list = match json::parse(msg) {
            Ok(m) => m,
            Err(e) => Err(format!("Error parsing websocket message: {e} {msg}"))?,
        };

        // message_list should be an array, but may be a single opensrf message.
        if !message_list.is_array() {
            let mut list = json::JsonValue::new_array();

            if let Err(e) = list.push(message_list) {
                Err(format!("Error creating message list {e}"))?;
            }

            message_list = list;
        }

        let mut body_vec: Vec<message::Message> = Vec::new();

        for msg_json in message_list.members() {

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
                _ => Err(format!("WS received unexpected message type: {}", msg.mtype()))?,
            }

            body_vec.push(msg);
        }

        let mut tm = message::TransportMessage::with_body_vec(
            &recipient, bus.address().full(), thread, body_vec);

        if let Some(xid) = log_xid_op {
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
    config: Arc<conf::Config>,

    /// Recipient address for OpenSRF responses.
    address: ClientAddress,

    to_inbound_tx: mpsc::Sender<SessionEvent>,
}

impl OutboundThread {

    fn new(
        config: Arc<conf::Config>,
        address: ClientAddress,
        to_inbound_tx: mpsc::Sender<SessionEvent>) -> Self {

        OutboundThread {
            config,
            address,
            to_inbound_tx,
        }
    }


    fn run(&mut self) -> Result<(), String> {
        let conf = self.config.gateway().unwrap(); // known good
        let mut bus = Bus::new(&conf)?;

        // Our bus-level address will not match the address of the
        // inbound thread bus.  Listen for responses on this agreed-
        // upon recipient address.
        let sent_to = self.address.full();

        loop {
            log::debug!("OutboundThread waiting for OpenSRF Responses");

            match bus.recv(-1, Some(sent_to)) {
                Ok(msg_op) => match msg_op {
                    Some(tm) => self.relay_osrf_to_stdout(&mut bus, &tm)?,
                    None => { continue; }
                }
                Err(e) => {
                    // transport_error -- can we get the thread? TODO
                    self.write_stdout("", json::JsonValue::new_array(), true)?;
                }
            }
        }
    }

    fn relay_osrf_to_stdout(&self,
        bus: &mut Bus, tm: &message::TransportMessage) -> Result<(), String> {

        let msg_list = tm.body();
        let sender = tm.from();

        let mut body = json::JsonValue::new_array();
        let mut transport_error = false;

        for msg in msg_list.iter() {

            if let message::Payload::Status(s) = msg.payload() {

                if s.status() == &message::MessageStatus::Ok {
                    // TODO
                    // Tell Inbound to add this thread/recipient to session cache
                }

                if *s.status() as isize >= message::MessageStatus::BadRequest as isize {
                    Err(format!("Request returned unexpected status"))?;
                }
            }

            body.push(msg.to_json_value());
        }

        self.write_stdout(tm.thread(), body, false)
    }

    fn write_stdout(&self, thread: &str,
        body: json::JsonValue, transport_error: bool) -> Result<(), String> {

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

    let (config, params) =
        init::init_with_more_options(&mut ops, &initops).unwrap();

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

