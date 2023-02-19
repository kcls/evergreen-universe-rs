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
use osrf::addr::ClientAddress;
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

const MAX_CLIENTS: usize = 256; // TODO configurable

/// How often to wake the OutboundThread to check for a shutdown signal.
const SHUTDOWN_POLL_INTERVAL: i32 = 5;

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
        let mut shutdown = false;

        for message in receiver.incoming_messages() {

            let channel_msg = match message {
                Ok(m) => ChannelMessage::Inbound(m),
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
        let mut shutdown = false;

        loop {

            // Wait for outbound OpenSRF messages, waking periodically
            // to assess, e.g. check for 'stopping' flag.
            let msg = match self.osrf_receiver.recv(SHUTDOWN_POLL_INTERVAL, None) {
                Ok(op) => match op {
                    Some(tm) => ChannelMessage::Outbound(tm),
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

        let mut osrf_sender = match Bus::new(&conf) {
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
        if let Err(e) = self.sender.send_message(&OwnedMessage::Close(None)) {
            log::error!("Main thread could not send a Close message");
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

    /// Main listen loop
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
                if let Err(e) = self.relay_to_osrf(m) {
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

    fn relay_to_osrf(&mut self, msg: OwnedMessage) -> Result<(), String> {
        Ok(())
    }

    fn relay_to_websocket(&mut self, msg: TransportMessage) -> Result<(), String> {
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
        todo!()
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


