use std::fmt;
use std::net::{TcpStream, SocketAddr};
use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::mpsc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use threadpool::ThreadPool;
use getopts;
use opensrf as osrf;
use osrf::bus::Bus;
use osrf::conf;
use osrf::init;
use osrf::message::TransportMessage;
use osrf::logging::Logger;
use osrf::addr::{ServiceAddress, RouterAddress};
use websocket::client::sync::Client;
use websocket::receiver::Reader;
use websocket::sender::Writer;
use websocket::OwnedMessage;

/*
 * Server spawns a session thread per connection.
 *
 * Each session has 3 threads of its own: Inbound, Main, and Outbound.
 *
 * Inbound session thread reads websocket requests and relays them to
 * the main thread for processing.
 *
 * Outbound session thread reads opensrf replies and relays them to the
 * main thread for processing.
 *
 * Main sesion thread does everything else.
 */

/// How many websocket clients we allow before blocking new connections.
const MAX_WS_CLIENTS: usize = 256;

/// How often to wake the OutboundThread to check for a shutdown signal.
const SHUTDOWN_POLL_INTERVAL: i32 = 5;

/// Prevent huge session threads
const MAX_THREAD_SIZE: usize = 256;

const WEBSOCKET_INGRESS: &str = "ws-translator-v3";

#[derive(Debug, PartialEq)]
enum ChannelMessage {
    /// Websocket Request
    Inbound(OwnedMessage),

    /// OpenSRF Reply
    Outbound(TransportMessage),

    /// Tell the main thread to wake up and assess, e.g. check for stopping flag.
    Wakeup,
}

struct InboundThread {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Cleanup and exit if true.
    stopping: Arc<AtomicBool>,
}

impl InboundThread {

    fn run(&mut self, mut receiver: Reader<TcpStream>) {
        for message in receiver.incoming_messages() {

            let channel_msg = match message {
                Ok(m) => {
                    log::trace!("InboundThread received message: {m:?}");
                    ChannelMessage::Inbound(m)
                }
                Err(e) => {
                    log::error!("Fatal error unpacking websocket message: {e}");
                    self.stopping.store(true, Ordering::Relaxed);
                    ChannelMessage::Wakeup
                }
            };

            if let Err(e) = self.to_main_tx.send(channel_msg) {
                // Likely the main thread has exited.
                log::error!("Fatal error sending websocket message to main thread: {e}");
                return;
            }

            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Inbound thread received a stop signal.  Exiting");
                break;
            }
        }
    }
}

struct OutboundThread {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Pulls messages from the OpenSRF bus for delivery back to the
    /// websocket client.
    osrf_receiver: Bus,

    /// Cleanup and exit if true.
    stopping: Arc<AtomicBool>,
}

impl OutboundThread {
    fn run(&mut self) {
        loop {

            // Wait for outbound OpenSRF messages, waking periodically
            // to assess, e.g. check for 'stopping' flag.
            let msg = match self.osrf_receiver.recv(SHUTDOWN_POLL_INTERVAL, None) {
                Ok(op) => match op {
                    Some(tm) => {
                        log::trace!("OutboundThread received message: {tm:?}");
                        ChannelMessage::Outbound(tm)
                    }
                    None => continue, // timeout
                }
                Err(e) => {
                    log::error!("Fatal error reading OpenSRF message: {e}");
                    self.stopping.store(true, Ordering::Relaxed);
                    ChannelMessage::Wakeup
                }
            };

            if let Err(e) = self.to_main_tx.send(msg) {
                // Likely the main thread has already exited.
                log::error!("Fatal error relaying channel message to main thread: {e}");
                return;
            }

            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Outbound thread received a stop signal.  Exiting");
                return;
            }
        }
    }
}

struct Session {
    /// All data flows to the main thread via this channel.
    to_main_rx: mpsc::Receiver<ChannelMessage>,

    /// Posts messages to the outbound websocket stream.
    sender: Writer<TcpStream>,

    /// Relays request to the OpenSRF bus.
    osrf_sender: Bus,

    /// Websocket client address.
    client_ip: SocketAddr,

    /// Cleanup and exit if true.
    stopping: Arc<AtomicBool>,

    /// Currently active (stateful) OpenSRF sessions.
    osrf_sessions: HashMap<String, String>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Session ({})", self.client_ip)
    }
}

impl Session {

    fn run(conf: conf::BusClient, client: Client<TcpStream>) {

        let client_ip = match client.peer_addr() {
            Ok(ip) => ip,
            Err(e) => {
                log::error!("Could not determine client IP address: {e}");
                return;
            }
        };

        let (receiver, sender) = match client.split() {
            Ok((r, s)) => (r, s),
            Err(e) => {
                log::error!("Fatal error splitting client streams: {e}");
                return;
            }
        };

        let (to_main_tx, to_main_rx) = mpsc::channel();

        let osrf_sender = match Bus::new(&conf) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error connecting to OpenSRF: {e}");
                return;
            }
        };

        let mut osrf_receiver = match Bus::new(&conf) {
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

        let stopping = Arc::new(AtomicBool::new(false));

        let mut inbound = InboundThread {
            stopping: stopping.clone(),
            to_main_tx: to_main_tx.clone(),
        };

        let mut outbound = OutboundThread {
            stopping: stopping.clone(),
            to_main_tx: to_main_tx.clone(),
            osrf_receiver,
        };

        let mut session = Session {
            stopping,
            client_ip,
            to_main_rx,
            sender,
            osrf_sender,
            osrf_sessions: HashMap::new(),
        };

        let in_thread = thread::spawn(move || inbound.run(receiver));
        let out_thread = thread::spawn(move || outbound.run());

        session.listen();
        session.shutdown(in_thread, out_thread);
    }

    fn shutdown(&mut self, in_thread: JoinHandle<()>, out_thread: JoinHandle<()>) {
        log::info!("{self} shutting down");

        // It's possible we are shutting down due to an issue that
        // occurred within this thread.  In that case, let the other
        // threads know it's time to cleanup and go home.
        self.stopping.store(true, Ordering::Relaxed);

        // Send a Close message to the Websocket client.  This has the
        // secondary benefit of forcing the InboundThread to exit its
        // listen loop.  (The OutboundThread will periodically check
        // for shutdown messages on its own).
        // TODO check for cases where a Close has already been sent.
        // see handle_inbound_message()
        if let Err(e) = self.sender.send_message(&OwnedMessage::Close(None)) {
            log::error!("Main thread could not send a Close message: {e}");
        }

        if let Err(e) = in_thread.join() {
            log::error!("Inbound thread exited with error: {e:?}");
        } else {
            log::debug!("Inbound thread exited gracefully");
        }

        if let Err(e) = out_thread.join() {
            log::error!("Out thread exited with error: {e:?}");
        } else {
            log::debug!("Outbound thread exited gracefully");
        }
    }

    /// Main Session listen loop
    fn listen(&mut self) {
        loop {

            let channel_msg = match self.to_main_rx.recv() {
                Ok(m) => m,
                Err(e) => {
                    log::error!("Error in main thread reading message channel: {e}");
                    return;
                }
            };

            log::trace!("Main thread read channel message: {channel_msg:?}");

            if let ChannelMessage::Inbound(m) = channel_msg {
                if let Err(e) = self.handle_inbound_message(m) {
                    log::error!("Error relaying request to OpenSRF: {e}");
                    return;
                }

            } else if let ChannelMessage::Outbound(tm) = channel_msg {
                if let Err(e) = self.relay_to_websocket(tm) {
                    log::error!("Error relaying response: {e}");
                    return;
                }
            }

            // Looks like we got a Wakeup message.  Assess.
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("Main thread received a stop signal.  Exiting");
                return;
            }
        }
    }

    fn handle_inbound_message(&mut self, msg: OwnedMessage) -> Result<(), String> {
        match msg {
            OwnedMessage::Text(text) => self.relay_to_osrf(&text),
            OwnedMessage::Ping(text) => {
                let message = OwnedMessage::Pong(text);
                self.sender.send_message(&message)
                    .or_else(|e| Err(format!("Error sending Pong to client: {e}")))
            }
            OwnedMessage::Close(_) => {
                // Set the stopping flag which will result in us
                // sending a Close back to the client.
                self.stopping.store(true, Ordering::Relaxed);
                Ok(())
            }
            _ => {
                log::warn!("Ignoring unexpected websocket message: {msg:?}");
                Ok(())
            }
        }
    }

    fn relay_to_osrf(&mut self, json_text: &str) -> Result<(), String> {

        let mut wrapper = json::parse(json_text)
            .or_else(|e| Err(format!("Cannot parse websocket message: {e} {json_text}")))?;

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

        // recipient is the final destination, but me may put this
        // message into the queue of the router as needed.
        let mut send_to_router: Option<String> = None;

        let recipient = match self.osrf_sessions.get(thread) {
            Some(a) => {
                log::debug!("Found cached recipient for thread {thread} {a}");
                a.clone()
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

        let mut body_vec: Vec<osrf::message::Message> = Vec::new();

        for msg_json in msg_list.members() {
            let mut msg = match osrf::message::Message::from_json_value(msg_json) {
                Some(m) => m,
                None => Err(format!("Error creating message from {msg_json}"))?,
            };

            msg.set_ingress(WEBSOCKET_INGRESS);

            match msg.mtype() {
                osrf::message::MessageType::Connect => {
                    log::debug!("WS received CONNECT request: {thread}");
                }
                osrf::message::MessageType::Request => {
                    self.log_request(service, &msg)?;
                }
                osrf::message::MessageType::Disconnect => {
                    log::debug!("WS removing session on DISCONNECT: {thread}");
                    self.osrf_sessions.remove(thread);
                }
                _ => Err(format!(
                    "WS received unexpected message type: {}",
                    msg.mtype()
                ))?,
            }

            body_vec.push(msg);
        }

        let mut tm = TransportMessage::with_body_vec(
            &recipient,
            self.osrf_sender.address().full(),
            thread,
            body_vec,
        );

        if let Some(xid) = log_xid.as_str() {
            tm.set_osrf_xid(xid);
        }

        if let Some(router) = send_to_router {
            self.osrf_sender.send_to(&tm, &router)?;
        } else {
            self.osrf_sender.send(&tm)?;
        }
        // TODO clear local log XID

        Ok(())
    }

    fn relay_to_websocket(&mut self, tm: TransportMessage) -> Result<(), String> {
        let msg_list = tm.body();

        let mut body = json::JsonValue::new_array();
        let mut transport_error = false;

        for msg in msg_list.iter() {
            if let osrf::message::Payload::Status(s) = msg.payload() {
                if s.status() == &osrf::message::MessageStatus::Ok {
                    self.osrf_sessions.insert(tm.thread().to_string(), tm.from().to_string());
                }

                if *s.status() as isize >= osrf::message::MessageStatus::BadRequest as isize {
                    transport_error = true;
                    log::error!("Request returned unexpected status: {:?}", msg);
                    self.osrf_sessions.remove(tm.thread());
                }
            }

            if let Err(e) = body.push(msg.to_json_value()) {
                Err(format!("Error building message response: {e}"))?;
            }
        }

        let mut obj = json::object! {
            // oxrf_xid: TODO
            thread: tm.thread(),
            osrf_msg: body
        };

        if transport_error {
            obj["transport_error"] = json::from(true);
        }

        let msg_json = obj.dump();

        log::trace!("{self} replying with message: {msg_json}");

        let msg = OwnedMessage::Text(msg_json);

        self.sender.send_message(&msg).or_else(
            |e| Err(format!("Error sending response to websocket client: {e}")))
    }

    fn log_request(&self, service: &str, msg: &osrf::message::Message) -> Result<(), String> {
        let request = match msg.payload() {
            osrf::message::Payload::Method(m) => m,
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

struct Server {
    conf: conf::BusClient,
    port: u16,
    address: String,
}

impl Server {
    fn new(conf: conf::BusClient, address: String, port: u16) -> Self {
        Server {
            conf,
            port,
            address,
        }
    }

    fn run(&mut self) {

        let host = format!("{}:{}", self.address, self.port);

        let server = websocket::sync::Server::bind(host)
            .expect("Could not start websockets server");

        let pool = ThreadPool::new(MAX_WS_CLIENTS);

        for connection in server.filter_map(Result::ok) {

            let tcount = pool.active_count() + pool.queued_count();

            if tcount >= MAX_WS_CLIENTS {
                log::warn!("Max websocket clients reached.  Ignoring new connection");
                continue;
            }

            let conf = self.conf.clone();

            pool.execute(move || {
                match connection.accept() {
                    Ok(client) => Session::run(conf, client),
                    Err(e) => log::error!("Error accepting new connection: {}", e.1),
                }
            });
        }
    }
}


fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("p", "port", "Port", "PORT");
    ops.optopt("a", "address", "Listen Address", "ADDRESS");

    let initops = init::InitOptions { skip_logging: true };

    let (config, params) = init::init_with_more_options(&mut ops, &initops).unwrap();

    let config = config.into_shared();

    let gateway = config.gateway().expect("No gateway configuration found");

    let logger = Logger::new(gateway.logging()).expect("Creating logger");
    logger.init().expect("Logger Init");

    let address = params.opt_get_default("a", "127.0.0.1".to_string()).unwrap();
    let port = params.opt_get_default("p", "7692".to_string()).unwrap();
    let port = port.parse::<u16>().expect("Invalid port number");

    let mut server = Server::new(gateway.clone(), address, port);
    server.run();
}


