use eg::idl;
use eg::osrf::addr::BusAddress;
use eg::osrf::bus::Bus;
use eg::osrf::conf;
use eg::osrf::logging::Logger;
use eg::osrf::message;
use eg::Client;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;
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
use std::time::Duration;
use tungstenite as ws;
use ws::protocol::Message as WebSocketMessage;
use ws::protocol::WebSocket;

const DEFAULT_PORT: u16 = 7682;

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
/// discard all of the pending requests and disconnect the client.
const MAX_BACKLOG_SIZE: usize = 1000;

const SIG_POLL_INTERVAL: u64 = 3;

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
 * The main session thread writes responses to the websocket client and
 * tracks connected sessions.
 */

/// ChannelMessage's are delivered to the main thread.  There are 2
/// types: Inbound websocket request and Ooutbound opensrf response.
#[derive(Debug, PartialEq)]
enum ChannelMessage {
    /// Websocket Request
    Inbound(WebSocketMessage),

    /// OpenSRF Reply
    Outbound(message::TransportMessage),
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
            // Check before going back to wait for the next ws message.
            if self.shutdown_session.load(Ordering::Relaxed) {
                break;
            }

            let message = match receiver.read_message() {
                Ok(m) => m,
                Err(e) => {
                    match e {
                        // Read timeout is possible since the TcpListener
                        // which is the source of our client stream
                        // was setup with its own timeout.
                        ws::error::Error::Io(ref io_err) => match io_err.kind() {
                            std::io::ErrorKind::WouldBlock => continue,
                            _ => log::error!("Error reading inbound message: {e:?}"),
                        },
                        ws::error::Error::ConnectionClosed | ws::error::Error::AlreadyClosed => {
                            log::debug!("Connection closed normally")
                        }
                        _ => log::error!("Error reading inbound message: {e:?}"),
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
        }

        self.shutdown();
    }

    fn shutdown(&mut self) {
        log::debug!("{self} shutting down");
        self.shutdown_session.store(true, Ordering::Relaxed);
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

            let msg = match self.osrf_receiver.recv(SIG_POLL_INTERVAL as i32, None) {
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
    }
}

/// Manages a single websocket client connection.  Sessions run in the
/// main thread for each websocket connection.
struct Session {
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

    /// Any time we receive a 'format' request in a message, we
    /// set that as our default format going forward for this
    /// client session.  It's assumed that clients will generally
    /// use a single format for the duration of their connection,
    /// but it's not required.
    format: Option<idl::DataFormat>,

    shutdown: Arc<AtomicBool>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Session ({})", self.client_ip)
    }
}

impl Session {
    fn run(stream: TcpStream, max_parallel: usize, shutdown: Arc<AtomicBool>) -> EgResult<()> {
        let client_ip = stream
            .peer_addr().map_err(|e| format!("Could not determine client IP address: {e}"))?;

        log::debug!("Starting new session for {client_ip}");

        // Split the TcpStream into a read/write pair so each endpoint
        // can be managed within its own thread.
        let instream = stream;
        let outstream = instream
            .try_clone().map_err(|e| format!("Fatal error splitting client streams: {e}"))?;

        // Wrap each endpoint in a WebSocket container.
        let receiver = ws::accept(instream).map_err(|e| format!("Error accepting new connection: {}", e))?;

        let sender = WebSocket::from_raw_socket(outstream, ws::protocol::Role::Server, None);

        let (to_main_tx, to_main_rx) = mpsc::channel();

        let gateway = conf::config().gateway();
        let busconf = gateway.as_ref().unwrap(); // previously verified

        let osrf_sender = Bus::new(busconf)?;
        let mut osrf_receiver = Bus::new(busconf)?;

        // The main Session thread has an OpenSRF bus connection that
        // only ever calls send() / send_to() -- never recv().  The
        // Outbound thread, which listens for response on the OpenSRF
        // bus has a bus connection that only ever calls recv().  (Note
        // the lower-level Bus API never mingles send/receive actions).
        // In this, we have a split-brain bus connections that won't
        // step each other's toes.
        //
        // It also means the bus receiver must have the same bus address
        // as the sender so it can act as its receiver.
        osrf_receiver.set_address(osrf_sender.address());

        let shutdown_session = Arc::new(AtomicBool::new(false));

        let mut inbound = SessionInbound {
            to_main_tx: to_main_tx.clone(),
            client_ip,
            shutdown_session: shutdown_session.clone(),
        };

        let mut outbound = SessionOutbound {
            to_main_tx: to_main_tx.clone(),
            client_ip,
            shutdown_session: shutdown_session.clone(),
            osrf_receiver,
        };

        let mut session = Session {
            client_ip,
            to_main_rx,
            sender,
            osrf_sender,
            max_parallel,
            reqs_in_flight: 0,
            format: None,
            shutdown,
            shutdown_session,
            osrf_sessions: HashMap::new(),
            request_queue: VecDeque::new(),
        };

        log::debug!("{session} starting channel threads");

        let in_thread = thread::spawn(move || inbound.run(receiver));
        let out_thread = thread::spawn(move || outbound.run());

        session.listen();
        session.shutdown(in_thread, out_thread);

        Ok(())
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

    /// Returns true if we should exit our main listen loop.
    fn housekeeping(&mut self) -> bool {
        if self.shutdown_session.load(Ordering::Relaxed) {
            log::info!("{self} session is shutting down");
            // This session is done
            return true;
        }

        if self.shutdown.load(Ordering::Relaxed) {
            // Websocket server is shutting down.
            // Tell our sub-threads to exit.
            self.shutdown_session.store(true, Ordering::Relaxed);
            log::info!("{self} server is shutting down");
            eprintln!("{self} server is shutting down");
            return true;
        }

        false
    }

    /// Main Session listen loop
    fn listen(&mut self) {
        loop {
            if self.housekeeping() {
                return;
            }

            let recv_result = self
                .to_main_rx
                .recv_timeout(Duration::from_secs(SIG_POLL_INTERVAL));

            let channel_msg = match recv_result {
                Ok(m) => m,
                Err(e) => {
                    match e {
                        // Timeouts are expected.
                        std::sync::mpsc::RecvTimeoutError::Timeout => continue,
                        // Other errors are not.
                        _ => {
                            log::error!("{self} Error in main thread reading message channel: {e}");
                            return;
                        }
                    }
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

        if !self.request_queue.is_empty() {
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
                    // Client is getting out of handle.  Let them go.
                    return Err(format!(
                        "Backlog exceeds max size={}; dropping connectino",
                        MAX_BACKLOG_SIZE
                    ));
                } else {
                    log::trace!("{self} Queueing inbound message for processing");
                    self.request_queue.push_back(text);
                }

                Ok(false)
            }
            WebSocketMessage::Ping(text) => {
                let message = WebSocketMessage::Pong(text);
                self.sender
                    .write_message(message).map_err(|e| format!("{self} Error sending Pong to client: {e}"))?;
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
        let mut wrapper = json::parse(json_text).map_err(|e| format!(
                "{self} Cannot parse websocket message: {e} {json_text}"
            ))?;

        let thread = wrapper["thread"].take();
        let log_xid = wrapper["log_xid"].take();
        let mut msg_list = wrapper["osrf_msg"].take();

        if let Some(xid) = log_xid.as_str() {
            Logger::set_log_trace(xid);
        } else {
            Logger::mk_log_trace();
        };

        let thread = thread
            .as_str()
            .ok_or_else(|| format!("{self} websocket message has no 'thread' key"))?;

        if thread.len() > MAX_THREAD_SIZE {
            Err(format!("{self} Thread exceeds max thread size; dropping"))?;
        }

        let service = wrapper["service"]
            .as_str()
            .ok_or_else(|| format!("{self} service name is required"))?;

        // recipient is the final destination, but we may put this
        // message into the queue of the router as needed.
        let mut send_to_router: Option<String> = None;

        let recipient = match self.osrf_sessions.get(thread) {
            Some(a) => {
                log::debug!("{self} Found cached recipient for thread {thread} {a}");
                a.clone()
            }
            None => {
                let username = self.osrf_sender.router_name();
                let domain = self.osrf_sender.address().domain();
                send_to_router = Some(
                    BusAddress::for_router(username, domain)
                        .as_str()
                        .to_string(),
                );
                BusAddress::for_bare_service(service).as_str().to_string()
            }
        };

        log::debug!("{self} WS relaying message thread={thread} recipient={recipient}");

        // msg_list is typically an array, but may be a single opensrf message.
        if !msg_list.is_array() {
            let mut list = json::JsonValue::new_array();

            if let Err(e) = list.push(msg_list) {
                Err(format!("{self} Error creating message list {e}"))?;
            }

            msg_list = list;
        }

        let mut format_hash = false;
        if let Some(format) = wrapper["format"].as_str() {
            self.format = Some(format.into());
            format_hash = self.format.as_ref().unwrap().is_hash();
        }

        let mut body_vec: Vec<message::Message> = Vec::new();

        loop {
            let msg_json = msg_list.array_remove(0);

            if msg_json.is_null() {
                break;
            }

            // false here means "non-raw data mode" which means we
            // require the IDL.  The IDL is required for HASH-ifying
            // inputs and outputs.
            let mut msg = message::Message::from_json_value(msg_json, false)?;
            msg.set_ingress(WEBSOCKET_INGRESS);

            match msg.mtype() {
                message::MessageType::Connect => {
                    self.reqs_in_flight += 1;
                    log::debug!("{self} WS received CONNECT request: {thread}");
                }
                message::MessageType::Request => {
                    self.reqs_in_flight += 1;

                    // Inbound requests using a hash format need to be
                    // turned into Fieldmapper objects before they
                    // are relayed to the API.
                    if format_hash {
                        if let eg::osrf::message::Payload::Method(ref mut meth) = msg.payload_mut()
                        {
                            for p in meth.params_mut() {
                                p.from_classed_hash()?;
                            }
                        }
                    }

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

        let tm = message::TransportMessage::with_body_vec(
            &recipient,
            self.osrf_sender.address().as_str(),
            thread,
            body_vec,
        );

        log::trace!(
            "{self} sending request to opensrf from {}",
            self.osrf_sender.address()
        );

        if let Some(router) = send_to_router {
            self.osrf_sender.send_to(tm, &router)?;
        } else {
            self.osrf_sender.send(tm)?;
        }

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
    fn relay_to_websocket(&mut self, mut tm: message::TransportMessage) -> Result<(), String> {
        let mut msg_list = tm.take_body();

        let mut body = json::JsonValue::new_array();
        let mut transport_error = false;

        for mut msg in msg_list.drain(..) {
            if let eg::osrf::message::Payload::Status(s) = msg.payload() {
                let stat = *s.status();
                match stat {
                    message::MessageStatus::Complete => self.subtract_reqs(),
                    message::MessageStatus::Ok => {
                        self.subtract_reqs();
                        // Connection successful message.  Track the worker address.
                        self.osrf_sessions
                            .insert(tm.thread().to_string(), tm.from().to_string());
                    }
                    // We don't need to analyze every non-error message.
                    s if (s as usize) < 400 => {}
                    _ => {
                        log::error!("{self} Request returned unexpected status: {:?}", msg);
                        self.subtract_reqs();
                        self.osrf_sessions.remove(tm.thread());

                        if stat.is_4xx() {
                            // roughly: service-not-found.
                            transport_error = true;
                        }
                    }
                }
            } else if let eg::osrf::message::Payload::Result(ref mut r) = msg.payload_mut() {
                // Decode (hashify) the result content instead of the
                // response message as a whole, because opensrf uses
                // the same class/payload encoding that the IDL/Fieldmapper
                // does.  We don't want to modify the opensrf messages,
                // just the result content.  (I mean, we could, but that
                // would break existing opensrf parsers).
                if let Some(format) = self.format.as_ref() {
                    if format.is_hash() {
                        // The caller wants result data returned in HASH format
                        r.content_mut().to_classed_hash();
                        if format == &idl::DataFormat::Hash {
                            // Caller wants a default slim hash
                            r.content_mut().scrub_hash_nulls();
                        }
                    }
                }
            }

            if let Err(e) = body.push(msg.into_json_value()) {
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

        let msg = WebSocketMessage::Text(msg_json);

        self.sender.write_message(msg).map_err(|e| format!(
                "{self} Error sending response to websocket client: {e}"
            ))
    }

    /// Log an API call, honoring the log-protect configs.
    fn log_request(&self, service: &str, msg: &message::Message) -> Result<(), String> {
        let request = match msg.payload() {
            eg::osrf::message::Payload::Method(m) => m,
            _ => Err(format!("{self} WS received Request with no payload"))?,
        };

        let log_params = eg::util::stringify_params(
            request.method(),
            request.params(),
            conf::config().log_protect(),
        );

        log::info!(
            "ACT:[{}] {} {} {}",
            self.client_ip,
            service,
            request.method(),
            log_params
        );

        // Also log as INFO e.g. gateway.xx.log
        log::info!(
            "[{}] {} {} {}",
            self.client_ip,
            service,
            request.method(),
            log_params
        );

        Ok(())
    }
}

// -- Here starts the MPTC glue --

struct WebsocketRequest {
    stream: Option<TcpStream>,
}

impl WebsocketRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut WebsocketRequest {
        h.as_any_mut()
            .downcast_mut::<WebsocketRequest>()
            .expect("WebsocketRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for WebsocketRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct WebsocketHandler {
    max_parallel: usize,
    shutdown: Arc<AtomicBool>,
}

impl mptc::RequestHandler for WebsocketHandler {
    fn worker_start(&mut self) -> Result<(), String> {
        // Session handles Bus connects and disconnects.
        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        // Session handles Bus connects and disconnects.
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = WebsocketRequest::downcast(&mut request);

        // Grab the stream so we can hand it off to our Session.
        let stream = request.stream.take().unwrap();

        let shutdown = self.shutdown.clone();

        if let Err(e) = Session::run(stream, self.max_parallel, shutdown) {
            log::error!("Websocket session ended with error: {e}");
        }

        Ok(())
    }
}

struct WebsocketStream {
    listener: TcpListener,
    client: Client,

    /// Maximum number of active/parallel websocket requests to
    /// relay to OpenSRF at a time.  Once exceeded, new messages
    /// are queued for delivery and relayed as soon as possible.
    max_parallel: usize,

    /// Set to true of the mptc::Server tells us it's time to shutdown.
    ///
    /// Read by our Sessions
    shutdown: Arc<AtomicBool>,
}

impl WebsocketStream {
    fn new(client: Client, address: &str, port: u16, max_parallel: usize) -> Result<Self, String> {
        log::info!("EG Websocket listening at {address}:{port}");

        let listener = eg::util::tcp_listener(address, port, SIG_POLL_INTERVAL).map_err(|e| format!(
                "Cannot listen for connections at {address}:{port} {e}"
            ))?;

        let stream = WebsocketStream {
            listener,
            client,
            max_parallel,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        Ok(stream)
    }
}

impl mptc::RequestStream for WebsocketStream {
    /// Returns the next client request stream.
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let (stream, _address) = match self.listener.accept() {
            Ok((s, a)) => (s, a),
            Err(e) => match e.kind() {
                // socket read timeout.
                std::io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(format!("accept() failed: {e}")),
            },
        };

        let request = WebsocketRequest {
            stream: Some(stream),
        };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let handler = WebsocketHandler {
            shutdown: self.shutdown.clone(),
            max_parallel: self.max_parallel,
        };

        Box::new(handler)
    }

    fn reload(&mut self) -> Result<(), String> {
        // We have no config file to reload.
        Ok(())
    }

    fn shutdown(&mut self) {
        // Tell our Session workers it's time to finish any active
        // requests then exit.
        // This only affects active Sessions.  mptc will notify its
        // own idle workers.
        log::info!("Server received mptc shutdown request");
        eprintln!("Server received mptc shutdown request");

        self.shutdown.store(true, Ordering::Relaxed);
        self.client.clear().ok();
    }
}

fn main() {
    let init_ops = eg::init::InitOptions {
        // As a gateway, we generally won't have access to the host
        // settings, since that's typically on a private domain.
        skip_host_settings: true,

        // Skip logging so we can use the logging config in
        // the gateway() config instead.
        skip_logging: true,
        appname: Some(String::from("http-gateway")),
    };

    // Connect to OpenSRF, parse the IDL
    // NOTE: Since we are not fetching host settings, we use
    // the default IDL path unless it's overridden with the
    // EG_IDL_FILE environment variable.
    let client = eg::init::with_options(&init_ops).expect("Evergreen init");

    // Setup logging with the gateway config
    let gateway_conf = conf::config().gateway().expect("Gateway config required");

    eg::osrf::logging::Logger::new(gateway_conf.logging())
        .expect("Creating logger")
        .init()
        .expect("Logger Init");

    let max_parallel = match env::var("EG_WEBSOCKETS_MAX_PARALLEL") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-parallel value"),
        _ => MAX_ACTIVE_REQUESTS,
    };

    let port = match env::var("EG_WEBSOCKETS_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    let address = env::var("EG_WEBSOCKETS_ADDRESS").unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());

    let stream = WebsocketStream::new(client, &address, port, max_parallel).expect("Build stream");

    let mut server = mptc::Server::new(Box::new(stream));

    if let Ok(n) = env::var("EG_WEBSOCKETS_MAX_WORKERS") {
        server.set_max_workers(n.parse::<usize>().expect("Invalid max-workers"));
    }

    // For websockets, where we don't pre-connect to the Bus, spawning
    // a lot of idle workers serves little purpose.
    if let Ok(n) = env::var("EG_WEBSOCKETS_MIN_WORKERS") {
        server.set_min_workers(n.parse::<usize>().expect("Invalid min-workers"));
    }

    // EG_WEBSOCKETS_MAX_REQUESTS for Websockets really means max sessions.
    if let Ok(n) = env::var("EG_WEBSOCKETS_MAX_REQUESTS") {
        server.set_max_worker_requests(n.parse::<usize>().expect("Invalid max-requests"));
    }

    server.run();
}
