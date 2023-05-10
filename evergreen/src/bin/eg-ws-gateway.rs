use mptc;
use eg::idl;
use evergreen as eg;
use opensrf as osrf;
use osrf::addr::{RouterAddress, ServiceAddress};
use osrf::bus::Bus;
use osrf::conf;
use osrf::init;
use osrf::logging::Logger;
use osrf::message;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::fmt;
use std::thread;
use std::any::Any;
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::sync::Arc;
use websocket::client::sync::Client;
use websocket::receiver::Reader;
use websocket::sender::Writer;
use websocket::server::NoTlsAcceptor;
use websocket::OwnedMessage;

const DEFAULT_PORT: u16 = 7682;

/// Prevent huge session threads
const MAX_THREAD_SIZE: usize = 256;

/// Largest allowed inbound websocket message.
///
/// Message size is typically limited by the the HTTP proxy,
/// e.g. nginx, so this is more of a backstop.
const MAX_MESSAGE_SIZE: usize = 10485760; // ~10M

/// Max size of the backlog queue
///
/// If we reach MAX_ACTIVE_REQUESTS, we start leaving new requests in
/// the backlog.  If the size of the baclkog exceeds this amount,
/// reject future requests until the backlog gets back below this amount.
/// NOTE: should we kick the client off at this point?
const MAX_BACKLOG_SIZE: usize = 1000;

const WEBSOCKET_INGRESS: &str = "ws-translator-v3";

const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1";

/// Max active parallel requests
const MAX_ACTIVE_REQUESTS: usize = 8;

/// ChannelMessage's are delivered to the main thread.  There are 3
/// varieties: inbound websocket request, outbound opensrf response,
/// and a wakeup message.
#[derive(Debug, PartialEq)]
enum ChannelMessage {
    /// Websocket Request
    Inbound(OwnedMessage),

    /// OpenSRF Reply
    Outbound(message::TransportMessage),
}

/// Listens for inbound websocket requests from our connected client
/// and relay them to the main thread.
struct SessionInbound {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Websocket client address.
    client_ip: SocketAddr,
}

impl fmt::Display for SessionInbound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionInbound ({})", self.client_ip)
    }
}

impl SessionInbound {
    fn run(&mut self, mut receiver: Reader<TcpStream>) {
        // Pull messages from our websocket TCP stream, forwarding each to
        // the Session thread for processing.
        for message in receiver.incoming_messages() {
            let channel_msg = match message {
                Ok(m) => {
                    log::trace!("{self} SessionInbound received message: {m:?}");
                    ChannelMessage::Inbound(m)
                }
                Err(e) => {
                    log::debug!("{self} Client likely disconnected: {e}");
                    break;
                }
            };

            if self.to_main_tx.send(channel_msg).is_err() {
                // Likely the main thread has exited.
                log::error!("{self} Cannot sent message to Session.  Exiting");
                break;
            }
        }
    }
}

/// Listens for responses on the OpenSRF bus and relays each to the
/// main thread for processing.
struct SessionOutbound {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Pulls messages from the OpenSRF bus for delivery back to the
    /// websocket client.
    osrf_receiver: Bus,

    /// Websocket client address.
    client_ip: SocketAddr,
}

impl fmt::Display for SessionOutbound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionOutbound ({})", self.client_ip)
    }
}

impl SessionOutbound {
    fn run(&mut self) {
        loop {
            log::trace!(
                "{self} waiting for opensrf response at {}",
                self.osrf_receiver.address()
            );

            let msg = match self.osrf_receiver.recv(-1, None) {
                Ok(op) => match op {
                    Some(tm) => {
                        log::debug!("{self} received message from: {}", tm.from());
                        ChannelMessage::Outbound(tm)
                    }
                    None => {
                        log::trace!(
                            "{self} no response received within poll interval.  trying again"
                        );
                        continue;
                    }
                },
                Err(e) => {
                    log::error!("{self} Fatal error reading OpenSRF message: {e}");
                    break;
                }
            };

            if self.to_main_tx.send(msg).is_err() {
                break; // Session thread has exited.
            }
        }
    }
}

/// Manages a single websocket client connection.  Sessions run in the
/// main thread for each websocket connection.
struct Session {
    /// OpenSRF config
    conf: Arc<conf::Config>,

    /// All messages flow to the main thread via this channel.
    to_main_rx: mpsc::Receiver<ChannelMessage>,

    /// For posting messages to the outbound websocket stream.
    sender: Writer<TcpStream>,

    /// Relays request to the OpenSRF bus.
    osrf_sender: Bus,

    /// Websocket client address.
    client_ip: SocketAddr,

    /// Currently active (stateful) OpenSRF sessions.
    osrf_sessions: HashMap<String, String>,

    /// Number of inbound connects/requests that are currently
    /// awaiting a final response.
    reqs_in_flight: usize,

    /// Backlog of messages yet to be delivered to OpenSRF.
    request_queue: VecDeque<String>,

    /// Maximum number of active/parallel websocket requests to
    /// relay to OpenSRF at a time.  Once exceeded, new messages
    /// are queued for delivery and relayed as soon as possible.
    max_parallel: usize,

    log_trace: Option<String>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Session ({})", self.client_ip)
    }
}

impl Session {
    fn run(conf: Arc<conf::Config>, client: Client<TcpStream>, max_parallel: usize) {
        let client_ip = match client.peer_addr() {
            Ok(ip) => ip,
            Err(e) => {
                log::error!("Could not determine client IP address: {e}");
                return;
            }
        };

        log::debug!("Starting new session for {client_ip}");

        let (receiver, sender) = match client.split() {
            Ok((r, s)) => (r, s),
            Err(e) => {
                log::error!("Fatal error splitting client streams: {e}");
                return;
            }
        };

        let (to_main_tx, to_main_rx) = mpsc::channel();

        let busconf = conf.gateway().unwrap(); // previoiusly verified

        let osrf_sender = match Bus::new(&busconf) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error connecting to OpenSRF: {e}");
                return;
            }
        };

        let mut osrf_receiver = match Bus::new(&busconf) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error connecting to OpenSRF: {e}");
                return;
            }
        };

        // Outbound OpenSRF connection must share the same address
        // as the inbound connection so it can receive replies to
        // requests relayed by the inbound connection.
        osrf_receiver.set_address(osrf_sender.address());

        let mut inbound = SessionInbound {
            to_main_tx: to_main_tx.clone(),
            client_ip: client_ip.clone(),
        };

        let mut outbound = SessionOutbound {
            to_main_tx: to_main_tx.clone(),
            client_ip: client_ip.clone(),
            osrf_receiver,
        };

        let mut session = Session {
            client_ip,
            to_main_rx,
            sender,
            conf,
            osrf_sender,
            max_parallel,
            reqs_in_flight: 0,
            log_trace: None,
            osrf_sessions: HashMap::new(),
            request_queue: VecDeque::new(),
        };

        log::debug!("{session} starting channel threads");

        thread::spawn(move || inbound.run(receiver));
        thread::spawn(move || outbound.run());

        session.listen();
    }

    /// Main Session listen loop
    fn listen(&mut self) {
        loop {
            let channel_msg = match self.to_main_rx.recv() {
                Ok(m) => m,
                Err(e) => {
                    log::error!("{self} Error in main thread reading message channel: {e}");
                    return;
                }
            };

            log::trace!("{self} read channel message: {channel_msg:?}");

            if let ChannelMessage::Inbound(m) = channel_msg {
                log::debug!("{self} received an Inbound channel message");

                match self.handle_inbound_message(m) {
                    Ok(closing) => {
                        if closing {
                            log::debug!("{self} Client closed connection.  Exiting");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("{self} Error relaying request to OpenSRF: {e}");
                        return;
                    }
                }
            } else if let ChannelMessage::Outbound(tm) = channel_msg {
                log::debug!("{self} received an Outbound channel message");
                if let Err(e) = self.relay_to_websocket(tm) {
                    log::error!("{self} Error relaying response: {e}");
                    return;
                }
            }

            if let Err(e) = self.process_message_queue() {
                log::error!("{self} Error processing inbound message: {e}");
                return;
            }
        }
    }

    /// handle_inbound_message tosses inbound messages onto a queue.
    /// Here we pop them off the queue and relay them to OpenSRF,
    /// taking the MAX_ACTIVE_REQUESTS limit into consideration.
    fn process_message_queue(&mut self) -> Result<(), String> {
        while self.reqs_in_flight < self.max_parallel {
            if let Some(text) = self.request_queue.pop_front() {
                // relay_to_osrf() increments self.reqs_in_flight as needed.
                self.relay_to_osrf(&text)?;
            } else {
                // Backlog is empty
                log::trace!("{self} message queue is empty");
                return Ok(());
            }
        }

        if self.request_queue.len() > 0 {
            log::warn!(
                "{self} MAX_ACTIVE_REQUESTS reached.  {} messages queued",
                self.request_queue.len()
            );
        }

        Ok(())
    }

    /// Process each inbound websocket message.  Requests are relayed
    /// to the OpenSRF bus.
    fn handle_inbound_message(&mut self, msg: OwnedMessage) -> Result<bool, String> {
        match msg {
            OwnedMessage::Text(text) => {
                let tlen = text.len();

                if tlen >= MAX_MESSAGE_SIZE {
                    log::error!("{self} Dropping huge websocket message size={tlen}");
                } else if self.request_queue.len() >= MAX_BACKLOG_SIZE {
                    log::error!("Backlog exceeds max size={}; dropping", MAX_BACKLOG_SIZE);
                } else {
                    log::trace!("{self} Queueing inbound message for processing");
                    self.request_queue.push_back(text);
                }

                Ok(false)
            }
            OwnedMessage::Ping(text) => {
                let message = OwnedMessage::Pong(text);
                self.sender
                    .send_message(&message)
                    .or_else(|e| Err(format!("{self} Error sending Pong to client: {e}")))?;
                Ok(false)
            }
            OwnedMessage::Close(_) => {
                // Let the main session loop know we're all done.
                Ok(true)
            }
            _ => {
                log::warn!("{self} Ignoring unexpected websocket message: {msg:?}");
                Ok(false)
            }
        }
    }

    /// Wrap a websocket request in an OpenSRF transport message and
    /// put on the OpenSRF bus for delivery.
    fn relay_to_osrf(&mut self, json_text: &str) -> Result<(), String> {
        let mut wrapper = json::parse(json_text).or_else(|e| {
            Err(format!(
                "{self} Cannot parse websocket message: {e} {json_text}"
            ))
        })?;

        let thread = wrapper["thread"].take();
        let log_xid = wrapper["log_xid"].take();
        let mut msg_list = wrapper["osrf_msg"].take();

        if let Some(xid) = log_xid.as_str() {
            self.log_trace = Some(xid.to_string());
        } else {
            self.log_trace = Some(Logger::mk_log_trace());
        };

        let thread = thread
            .as_str()
            .ok_or(format!("{self} websocket message has no 'thread' key"))?;

        if thread.len() > MAX_THREAD_SIZE {
            Err(format!("{self} Thread exceeds max thread size; dropping"))?;
        }

        let service = wrapper["service"]
            .as_str()
            .ok_or(format!("{self} service name is required"))?;

        // recipient is the final destination, but me may put this
        // message into the queue of the router as needed.
        let mut send_to_router: Option<String> = None;

        let recipient = match self.osrf_sessions.get(thread) {
            Some(a) => {
                log::debug!("{self} Found cached recipient for thread {thread} {a}");
                a.clone()
            }
            None => {
                let domain = self.osrf_sender.address().domain();
                send_to_router = Some(RouterAddress::new(domain).as_str().to_string());
                ServiceAddress::new(service).as_str().to_string()
            }
        };

        log::debug!("{self} WS relaying message thread={thread} recipient={recipient}");

        // msg_list should be an array, but may be a single opensrf message.
        if !msg_list.is_array() {
            let mut list = json::JsonValue::new_array();

            if let Err(e) = list.push(msg_list) {
                Err(format!("{self} Error creating message list {e}"))?;
            }

            msg_list = list;
        }

        let mut body_vec: Vec<message::Message> = Vec::new();

        loop {
            let msg_json = msg_list.array_remove(0);

            if msg_json.is_null() {
                break;
            }

            let mut msg = match message::Message::from_json_value(msg_json) {
                Some(m) => m,
                None => Err(format!("{self} could not create message from JSON"))?,
            };

            msg.set_ingress(WEBSOCKET_INGRESS);

            match msg.mtype() {
                message::MessageType::Connect => {
                    self.reqs_in_flight += 1;
                    log::debug!("{self} WS received CONNECT request: {thread}");
                }
                message::MessageType::Request => {
                    self.reqs_in_flight += 1;
                    self.log_request(service, &msg)?;
                }
                message::MessageType::Disconnect => {
                    log::debug!("{self} WS removing session on DISCONNECT: {thread}");
                    self.osrf_sessions.remove(thread);
                }
                _ => Err(format!(
                    "{self} WS received unexpected message type: {}",
                    msg.mtype()
                ))?,
            }

            body_vec.push(msg);
        }

        let mut tm = message::TransportMessage::with_body_vec(
            &recipient,
            self.osrf_sender.address().as_str(),
            thread,
            body_vec,
        );

        if let Some(xid) = self.log_trace.as_ref() {
            tm.set_osrf_xid(xid);
        }

        log::trace!(
            "{self} sending request to opensrf from {}",
            self.osrf_sender.address()
        );

        if let Some(router) = send_to_router {
            self.osrf_sender.send_to(&tm, &router)?;
        } else {
            self.osrf_sender.send(&tm)?;
        }

        self.log_trace = None;

        Ok(())
    }

    /// Package an OpenSRF response as a websocket message and
    /// send the message to this Session's websocket client.
    fn relay_to_websocket(&mut self, tm: message::TransportMessage) -> Result<(), String> {
        let msg_list = tm.body();

        let mut body = json::JsonValue::new_array();
        let mut transport_error = false;

        for msg in msg_list.iter() {
            if let osrf::message::Payload::Status(s) = msg.payload() {
                match *s.status() {
                    message::MessageStatus::Ok => {
                        if self.reqs_in_flight > 0 {
                            // avoid underflow
                            self.reqs_in_flight -= 1;
                        };
                        self.osrf_sessions
                            .insert(tm.thread().to_string(), tm.from().to_string());
                    }
                    message::MessageStatus::Complete => {
                        if self.reqs_in_flight > 0 {
                            self.reqs_in_flight -= 1;
                        };
                    }
                    s if s as usize >= message::MessageStatus::BadRequest as usize => {
                        if self.reqs_in_flight > 0 {
                            self.reqs_in_flight -= 1;
                        };
                        transport_error = true;
                        log::error!("{self} Request returned unexpected status: {:?}", msg);
                        self.osrf_sessions.remove(tm.thread());
                    }
                    _ => {}
                }
            }

            if let Err(e) = body.push(msg.to_json_value()) {
                Err(format!("{self} Error building message response: {e}"))?;
            }
        }

        let mut obj = json::object! {
            oxrf_xid: tm.osrf_xid(),
            thread: tm.thread(),
            osrf_msg: body
        };

        if transport_error {
            obj["transport_error"] = json::from(true);
        }

        let msg_json = obj.dump();

        log::trace!("{self} replying with message: {msg_json}");

        let msg = OwnedMessage::Text(msg_json);

        self.sender.send_message(&msg).or_else(|e| {
            Err(format!(
                "{self} Error sending response to websocket client: {e}"
            ))
        })
    }

    /// Log an API call, honoring the log-protect configs.
    fn log_request(&self, service: &str, msg: &message::Message) -> Result<(), String> {
        let request = match msg.payload() {
            osrf::message::Payload::Method(m) => m,
            _ => Err(format!("{self} WS received Request with no payload"))?,
        };

        let mut log_params: Option<String> = None;

        if self
            .conf
            .log_protect()
            .iter()
            .filter(|m| request.method().starts_with(&m[..]))
            .next()
            .is_none()
        {
            log_params = Some(
                request
                    .params()
                    .iter()
                    .map(|p| p.dump())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        };

        let log_params = log_params.as_deref().unwrap_or("**PARAMS REDACTED**");
        let xid = self.log_trace.as_deref().unwrap_or("");

        log::info!(
            "ACT:[{}:{}] {} {} {}",
            self.client_ip,
            xid,
            service,
            request.method(),
            log_params
        );

        Ok(())
    }
}

/* ----------------------------------------------------------------------
 * Wrap our Websocket handing code into a set of 'mptc' classes so
 * we can tie this all together into a threaded server
 */

struct WsConnectionRequest {
    client: Option<Client<TcpStream>>,
}

impl WsConnectionRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut WsConnectionRequest {
        h.as_any_mut()
            .downcast_mut::<WsConnectionRequest>()
            .expect("WsConnectionRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for WsConnectionRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct WsConnectionHandler {
    osrf_conf: Arc<osrf::conf::Config>,
    max_parallel: usize,
}

impl mptc::RequestHandler for WsConnectionHandler {
    fn worker_start(&mut self) -> Result<(), String> {
        // We do all of our startup work in process() since it's only
        // called once per websocket client connection.
        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// In the context of a Websocket server, this function acts as the
    /// entry point for a new websocket client.  From here, we start
    /// a new Session and then let it do its thing.
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let connection = WsConnectionRequest::downcast(&mut request);

        // connection.client will be set since that's why we are here
        // in the first place.  Extract the client value and pass
        // it off to our new Session instance for processing.
        let client = std::mem::replace(&mut connection.client, None).unwrap();

        Session::run(self.osrf_conf.clone(), client, self.max_parallel);
        Ok(())
    }
}

struct WsStream {
    server: websocket::sync::Server<NoTlsAcceptor>,
    osrf_conf: Arc<osrf::conf::Config>,
    max_parallel: usize,
    idl: Arc<idl::Parser>,
}

impl WsStream {
    fn new(
        osrf_conf: Arc<osrf::conf::Config>,
        idl: Arc<eg::idl::Parser>,
        address: &str,
        port: u16,
        max_parallel: usize,
    ) -> Result<Self, String> {
        let hostport = format!("{}:{}", address, port);

        log::info!("Server listening for connections at {hostport}");

        let server = match websocket::sync::Server::bind(&hostport) {
            Ok(s) => s,
            Err(e) => Err(format!("Could not start websockets server: {e}"))?,
        };

        Ok(WsStream {
            idl,
            server,
            osrf_conf,
            max_parallel,
        })
    }
}

impl mptc::RequestStream for WsStream {

    /// Returns the next client request stream.
    fn next(&mut self) -> Result<Box<dyn mptc::Request>, String> {

        // Wait for our next websocket client to connect
        let connection = match self.server.accept() {
            Ok(c) => c,
            Err(e) => Err(format!("Error accepting new connection: {e:?}"))?,
        };

        // Accept the connection and pass it off for processing.
        let client = match connection.accept() {
            Ok(c) => c,
            Err(e) => Err(format!("Error accepting new connection: {}", e.1))?,
        };

        let wsc = WsConnectionRequest { client: Some(client) };

        Ok(Box::new(wsc))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let handler = WsConnectionHandler {
            max_parallel: self.max_parallel,
            osrf_conf: self.osrf_conf.clone(),
        };

        Box::new(handler)
    }

    fn reload(&mut self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {

    let init_ops = eg::init::InitOptions {
        // As a gateway, we generally won't have access to the host
        // settings, since that's typically on a private domain.
        skip_host_settings: true,

        // Skip logging so we can use the loging config in
        // the gateway() config instead.
        osrf_ops: osrf::init::InitOptions { skip_logging: true },
    };

    // Connect to OpenSRF, parse the IDL
    let eg_ctx = eg::init::init_with_options(&init_ops).expect("Evergreen init");
    let idl = eg_ctx.idl().clone();
    let config = eg_ctx.config().clone();

    drop(eg_ctx); // Force a disconnect / cleanup

    let gateway = config.gateway().expect("No gateway configuration found");

    let logger = Logger::new(gateway.logging()).expect("Creating logger");
    logger.init().expect("Logger Init");

    let address = env::var("EG_WS_ADDRESS").unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());

    let port = match env::var("EG_WS_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    // Max number of active / open requests per Session at a time.
    // Any amount beyond this is backloged until it can be relayed.
    let max_parallel = match env::var("EG_WS_MAX_PARALLEL") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-parallel value"),
        _ => MAX_ACTIVE_REQUESTS,
    };

    let stream = WsStream::new(config, idl, &address, port, max_parallel).expect("Build stream");
    let mut server = mptc::Server::new(Box::new(stream));

    if let Ok(n) = env::var("EG_WS_MAX_WORKERS") {
        server.set_max_workers(n.parse::<usize>().expect("Invalid max-workers"));
    }

    if let Ok(n) = env::var("EG_WS_MIN_WORKERS") {
        server.set_min_workers(n.parse::<usize>().expect("Invalid min-workers"));
    }

    server.run();
}
