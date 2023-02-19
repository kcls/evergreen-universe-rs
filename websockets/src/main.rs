use std::fmt;
use std::net::{TcpStream, SocketAddr};
use std::thread;
use std::sync::Arc;
use std::sync::mpsc;
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
 * Server spawns a sesion thread per connection via threadpool.
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

#[derive(Debug)]
enum ChannelMessage {
    /// Websocket Request
    Inbound(OwnedMessage),

    /// OpenSRF Reply
    Outbound(TransportMessage),
}

struct InboundThread {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,
}

impl InboundThread {
    fn run(&mut self, mut receiver: Reader<TcpStream>) {
        for message in receiver.incoming_messages() {

            let message = match message {
                Ok(m) => m,
                Err(e) => {
                    log::error!("Fatal error unpacking websocket message: {e}");
                    // TODO set shutdown
                    return;
                }
            };

            let channel_msg = ChannelMessage::Inbound(message);

            if let Err(e) = self.to_main_tx.send(channel_msg) {
                log::error!("Fatal error sedning websocket message to main thread: {e}");
                // TODO set shutdown
                return;
            }
        }
    }
}

struct OutboundThread {
    /// Relays messages to the main session thread.
    to_main_tx: mpsc::Sender<ChannelMessage>,

    /// Pulls messages from the OpenSRF bus for delivery back to the
    /// websocket client.
    osrf_outbound: Bus,
}

impl OutboundThread {
    fn run(&mut self) {

        loop {

            let tm = match self.osrf_outbound.recv(-1, None) {
                Ok(op) => match op {
                    Some(m) => m,
                    None => continue,
                }
                Err(e) => {
                    log::error!("Fatal error reading OpenSRF message: {e}");
                    // TODO shutdown
                    return;
                }
            };

            let channel_msg = ChannelMessage::Outbound(tm);

            if let Err(e) = self.to_main_tx.send(channel_msg) {
                log::error!("Fatal error relaying outbound message to main thread: {e}");
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
    osrf_inbound: Bus,

    /// Websocket client address.
    client_ip: SocketAddr,

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

        let mut osrf_inbound = match Bus::new(&conf) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error connecting to OpenSRF: {e}");
                return;
            }
        };

        let mut osrf_outbound = match Bus::new(&conf) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error connecting to OpenSRF: {e}");
                return;
            }
        };

        // Outbound OpenSRF connection must share the same address
        // as the inbound connection so it can receive replies.
        osrf_outbound.set_address(osrf_inbound.address());

        let mut inbound = InboundThread {
            to_main_tx: to_main_tx.clone(),
        };

        let mut outbound = OutboundThread {
            to_main_tx: to_main_tx.clone(),
            osrf_outbound,
        };

        let mut session = Session {
            client_ip,
            to_main_rx,
            sender,
            osrf_inbound,
        };

        let in_thread = thread::spawn(move || inbound.run(receiver));
        let out_thread = thread::spawn(move || outbound.run());

        session.listen();

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


