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
use std::net::TcpListener;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tungstenite as ws;
use ws::protocol::Message as WebSocketMessage;
use ws::protocol::WebSocket;
use serde_json as json;

/* Server spawns a new client session per connection.
 *
 * Each client session is composed of 3 threads: Inbound, Main, and Outbound.
 *
 * Inbound session thread reads websocket requests and relays them to
 * the main thread for processing.
 *
 * Outbound session thread reads opensrf replies and relays them to the
 * main thread for processing.
 *
 * The Session thread writes responses to the websocket client and
 * tracks connections.
 */

const DEFAULT_PORT: u16 = 7682;

/// How many websocket clients we allow before block new connections.
const MAX_WS_CLIENTS: usize = 256;

/// Prevent huge session threads
const MAX_THREAD_SIZE: usize = 256;

/// Largest allowed inbound websocket message.
///
/// Message size is typically limited by the the HTTP proxy,
/// e.g. nginx, so this is more of a backstop.
const MAX_MESSAGE_SIZE: usize = 10485760; // ~10M

const WEBSOCKET_INGRESS: &str = "ws-translator-v3";

const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1";

/// Max active parallel requests
const MAX_ACTIVE_REQUESTS: usize = 8;

/// Max size of the backlog queue
///
/// If we reach MAX_ACTIVE_REQUESTS, we start leaving new requests in
/// the backlog.  If the size of the baclkog exceeds this amount,
/// reject future requests until the backlog gets back below this amount.
/// NOTE: should we kick the client off at this point?
const MAX_BACKLOG_SIZE: usize = 1000;

const SHUTDOWN_POLL_INTERVAL: i64 = 3;

/// ChannelMessage's are delivered to the main thread.  There are 2
/// types: Inbound websocket request and Ooutbound opensrf response.
#[derive(Debug, PartialEq)]
enum ChannelMessage {
    /// Websocket Request
    Inbound(WebSocketMessage),

    /// OpenSRF Reply
    Outbound(message::TransportMessage),

    /// Tell the main Session thread to wakeup and check for
    /// a shutdown signal.
    Wakeup,
}

/// Listens for inbound websocket requests from our connected client
/// and relay them to the main thread.
struct SessionInbound {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Cleanup and exit if true.
    shutdown_session: Arc<AtomicBool>,

    /// Websocket client address.
    client_ip: SocketAddr,
}

impl fmt::Display for SessionInbound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionInbound ({})", self.client_ip)
    }
}

impl SessionInbound {
    fn run(&mut self, mut receiver: WebSocket<TcpStream>) {
        // Pull messages from our websocket TCP stream, forwarding each to
        // the Session thread for processing.

        loop {
            let message = match receiver.read_message() {
                Ok(m) => m,
                Err(e) => {
                    match e {
                        ws::error::Error::ConnectionClosed | ws::error::Error::AlreadyClosed => {
                            log::debug!("Connection closed normally")
                        }
                        _ => log::error!("Error reading inbound message: {e}"),
                    }
                    break;
                }
            };

            let channel_msg = ChannelMessage::Inbound(message);

            if self.to_main_tx.send(channel_msg).is_err() {
                // Likely the main thread has exited.
                log::error!("{self} Cannot sent message to Session.  Exiting");
                break;
            }

            // Check before going back to wait for the next ws message.
            if self.shutdown_session.load(Ordering::Relaxed) {
                break;
            }
        }

        self.shutdown();
    }

    fn shutdown(&mut self) {
        log::debug!("{self} shutting down");

        self.shutdown_session.store(true, Ordering::Relaxed);

        // Tell our Session thread to wake up and check for shutdown
        // signals.  At this point, it's 50/50 our Session thread is
        // already exited, so we can ignore errors.
        self.to_main_tx.send(ChannelMessage::Wakeup).ok();
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

    /// Cleanup and exit if true.
    shutdown_session: Arc<AtomicBool>,

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
            // Check before going back to wait for the next ws message.
            if self.shutdown_session.load(Ordering::Relaxed) {
                break;
            }

            let msg = match self.osrf_receiver.recv(SHUTDOWN_POLL_INTERVAL, None) {
                Ok(op) => match op {
                    Some(tm) => {
                        log::debug!("{self} received message from: {}", tm.from());
                        ChannelMessage::Outbound(tm)
                    }
                    None => continue, // recv timeout, try again
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

        self.shutdown();
    }

    fn shutdown(&mut self) {
        log::debug!("{self} shutting down");

        self.shutdown_session.store(true, Ordering::Relaxed);

        // Tell our Session thread to wake up and check for shutdown
        // signals.  At this point, it's 50/50 our Session thread is
        // already exited, so we can ignore errors.
        self.to_main_tx.send(ChannelMessage::Wakeup).ok();
    }
}

/// Manages a single websocket client connection.  Sessions run in the
/// main thread for each websocket connection.
struct Session {
    /// OpenSRF config
    conf: Arc<conf::Config>,

    /// All messages flow to the main thread via this channel.
    to_main_rx: mpsc::Receiver<ChannelMessage>,

    /// For posting responses to the outbound websocket stream.
    sender: WebSocket<TcpStream>,

    /// Relays request to the OpenSRF bus.
    osrf_sender: Bus,

    /// Websocket client address.
    client_ip: SocketAddr,

    /// Cleanup and exit if true.
    shutdown_session: Arc<AtomicBool>,

    /// Currently active stateful/connected OpenSRF sessions.
    /// These must be tracked so that subsequent requests for the
    /// same OpenSRF session may be routed to the OpenSRF worker
    /// we have already connected to.
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
    fn run(conf: Arc<conf::Config>, stream: TcpStream, max_parallel: usize) {
        let client_ip = match stream.peer_addr() {
            Ok(ip) => ip,
            Err(e) => {
                log::error!("Could not determine client IP address: {e}");
                return;
            }
        };

        log::debug!("Starting new session for {client_ip}");

        // Split the TcpStream into a read/write pair so each endpoint
        // can be managed within its own thread.
        let instream = stream;
        let outstream = match instream.try_clone() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Fatal error splitting client streams: {e}");
                return;
            }
        };

        // Wrap each endpoint in a WebSocket container.

        let receiver = match ws::accept(instream) {
            Ok(c) => c,
            Err(e) => {
                log::error!("Error accepting new connection: {}", e);
                return;
            }
        };

        let sender = WebSocket::from_raw_socket(outstream, ws::protocol::Role::Server, None);

        let (to_main_tx, to_main_rx) = mpsc::channel();

        let busconf = conf.gateway().unwrap(); // previously verified

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

        let shutdown = Arc::new(AtomicBool::new(false));

        let mut inbound = SessionInbound {
            to_main_tx: to_main_tx.clone(),
            client_ip: client_ip.clone(),
            shutdown_session: shutdown.clone(),
        };

        let mut outbound = SessionOutbound {
            to_main_tx: to_main_tx.clone(),
            client_ip: client_ip.clone(),
            shutdown_session: shutdown.clone(),
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
            shutdown_session: shutdown,
            osrf_sessions: HashMap::new(),
            request_queue: VecDeque::new(),
        };

        log::debug!("{session} starting channel threads");

        let in_thread = thread::spawn(move || inbound.run(receiver));
        let out_thread = thread::spawn(move || outbound.run());

        session.listen();
        session.shutdown(in_thread, out_thread);
    }

    fn shutdown(&mut self, in_thread: JoinHandle<()>, out_thread: JoinHandle<()>) {
        log::debug!("{self} shutting down");

        // It's possible we are shutting down due to an issue that
        // occurred within this thread.  In that case, let the other
        // session threads know it's time to cleanup and go home.
        self.shutdown_session.store(true, Ordering::Relaxed);

        // Send a Close message to the Websocket client.  This has the
        // secondary benefit of forcing the SessionInbound to exit its
        // listen loop.  (The SessionOutbound will periodically check
        // for shutdown messages on its own).
        // During shutdown, various error conditions may occur as our
        // sockets are in different states of disconnecting.  Discard
        // any errors and keep going.
        self.sender
            .write_message(WebSocketMessage::Close(None))
            .ok();

        if let Err(e) = in_thread.join() {
            log::error!("{self} Inbound thread exited with error: {e:?}");
        } else {
            log::debug!("{self} Inbound thread exited gracefully");
        }

        if let Err(e) = out_thread.join() {
            log::error!("{self} Out thread exited with error: {e:?}");
        } else {
            log::debug!("{self} Outbound thread exited gracefully");
        }
    }

    /// Main Session listen loop
    fn listen(&mut self) {
        loop {
            // Check before going back to wait for the next ws message.
            if self.shutdown_session.load(Ordering::Relaxed) {
                break;
            }

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
                            return;
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
            } else {
                // Wakeup
                log::debug!("{self} received a Wakeup message.  Likely time to go");
                // Jump back to the front of the loop and check for shutdown.
                continue;
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
                log::trace!("{self} message queue is now empty");
                return Ok(());
            }
        }

        if self.request_queue.len() > 0 {
            log::warn!(
                "{self} MAX_ACTIVE_REQUESTS reached. {} messages queued",
                self.request_queue.len()
            );
        }

        Ok(())
    }

    /// Process each inbound websocket message.  Requests are relayed
    /// to the OpenSRF bus.
    fn handle_inbound_message(&mut self, msg: WebSocketMessage) -> Result<bool, String> {
        match msg {
            WebSocketMessage::Text(text) => {
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
            WebSocketMessage::Ping(text) => {
                let message = WebSocketMessage::Pong(text);
                self.sender
                    .write_message(message)
                    .or_else(|e| Err(format!("{self} Error sending Pong to client: {e}")))?;
                Ok(false)
            }
            WebSocketMessage::Close(_) => {
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
        let mut wrapper: json::Value = json::from_str(json_text).or_else(|e| {
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

        // recipient is the final destination, but we may put this
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

        // msg_list is typically an array, but may be a single opensrf message.
        if !msg_list.is_array() {
            msg_list = json::Value::Array(vec![msg_list]);
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

    /// Subtract one from our request-in-flight while protecting
    /// against underflow on an unsigned number.  Underflow should
    /// not happen in practice, but if it did, the thread would panic.
    fn subtract_reqs(&mut self) {
        if self.reqs_in_flight > 0 {
            // Avoid unsigned underflow, which would cause panic.
            self.reqs_in_flight -= 1;
        }
    }

    /// Package an OpenSRF response as a websocket message and
    /// send the message to this Session's websocket client.
    fn relay_to_websocket(&mut self, tm: message::TransportMessage) -> Result<(), String> {
        let msg_list = tm.body();

        let mut body = json::Value::new_array();
        let mut transport_error = false;

        for msg in msg_list.iter() {
            if let osrf::message::Payload::Status(s) = msg.payload() {
                match *s.status() {
                    message::MessageStatus::Complete => self.subtract_reqs(),
                    message::MessageStatus::Ok => {
                        self.subtract_reqs();
                        // Connection successful message.  Track the worker address.
                        self.osrf_sessions
                            .insert(tm.thread().to_string(), tm.from().to_string());
                    }

                    // Any response whose status is >= 400 (bad-request) is
                    // treated as an error and any stateful connection (if
                    // active) is severed.
                    s if s as usize >= message::MessageStatus::BadRequest as usize => {
                        self.subtract_reqs();
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

        let mut obj = json::json!({
            "oxrf_xid": tm.osrf_xid(),
            "thread": tm.thread(),
            "osrf_msg": body
        });

        if transport_error {
            obj["transport_error"] = true;
        }

        let msg_json = obj.dump();

        log::trace!("{self} replying with message: {msg_json}");

        let msg = WebSocketMessage::Text(msg_json);

        self.sender.write_message(msg).or_else(|e| {
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

/// Listens for websocket connections and spawn a Session thread per
/// connection.  Blocks new connections once max clients is reached.
struct Server {
    conf: Arc<conf::Config>,
    port: u16,
    address: String,
    max_clients: usize,
    max_parallel: usize,
}

impl Server {
    fn new(
        conf: Arc<conf::Config>,
        address: String,
        port: u16,
        max_clients: usize,
        max_parallel: usize,
    ) -> Self {
        Server {
            conf,
            port,
            address,
            max_clients,
            max_parallel,
        }
    }

    fn run(&mut self) {
        let hostport = format!("{}:{}", self.address, self.port);

        log::info!("Server listening for connections at {hostport}");

        let server = match TcpListener::bind(hostport) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Could not start websockets server: {e}");
                return;
            }
        };

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for connection_res in server.incoming() {
            let stream = match connection_res {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Error accepting new connection: {}", e);
                    continue;
                }
            };

            log::debug!("Server thread received new client connection");

            handles = self.cleanup_handles(&mut handles);

            if handles.len() >= self.max_clients {
                log::warn!("Max websocket clients reached.  Ignoring new connection");
                stream.shutdown(std::net::Shutdown::Both).ok();
                continue;
            }

            let conf = self.conf.clone();
            let max_parallel = self.max_parallel;

            handles.push(thread::spawn(move || {
                Session::run(conf, stream, max_parallel)
            }));
        }
    }

    /// Remove completed threads from our list of join handles.
    fn cleanup_handles(&mut self, handles: &mut Vec<JoinHandle<()>>) -> Vec<JoinHandle<()>> {
        let mut active: Vec<JoinHandle<()>> = Vec::new();

        while handles.len() > 0 {
            let handle = handles.remove(0);
            if handle.is_finished() {
                handle.join().ok();
            } else {
                active.push(handle);
            }
        }

        active
    }
}

fn main() {
    let init_ops = init::InitOptions {
        skip_logging: true,
        appname: Some(String::from("websockets")),
    };

    let config = init::init_with_options(&init_ops).unwrap();

    let config = config.into_shared();

    let gateway = config.gateway().expect("No gateway configuration found");

    let logger = Logger::new(gateway.logging()).expect("Creating logger");
    logger.init().expect("Logger Init");

    let address = env::var("OSRF_WS_ADDRESS").unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());

    let port = match env::var("OSRF_WS_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    let max_clients = match env::var("OSRF_WS_MAX_CLIENTS") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-clients value"),
        _ => MAX_WS_CLIENTS,
    };

    let max_parallel = match env::var("OSRF_WS_MAX_PARALLEL") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-parallel value"),
        _ => MAX_ACTIVE_REQUESTS,
    };

    let mut server = Server::new(config, address, port, max_clients, max_parallel);
    server.run();
}
