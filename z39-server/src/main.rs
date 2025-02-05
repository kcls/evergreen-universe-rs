use evergreen as eg;

use std::any::Any;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod query;
mod session;
use session::Z39Session;

struct Z39ConnectRequest {
    tcp_stream: Option<TcpStream>,
}

impl Z39ConnectRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut Z39ConnectRequest {
        h.as_any_mut()
            .downcast_mut::<Z39ConnectRequest>()
            .expect("Z39ConnectRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for Z39ConnectRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct Z39SessionBroker {
    shutdown: Arc<AtomicBool>,
    bus: Option<eg::osrf::bus::Bus>,
}

impl mptc::RequestHandler for Z39SessionBroker {

    /// Connect to the Evergreen bus
    fn worker_start(&mut self) -> Result<(), String> {
        let bus = eg::osrf::bus::Bus::new(eg::osrf::conf::config().client())?;
        self.bus = Some(bus);
        Ok(())
    }

    /// Create a Z session to handle the connection and let it run.
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = Z39ConnectRequest::downcast(&mut request);
        
        // Temporarily give our bus to the zsession
        let bus = self.bus.take().unwrap();

        // Give the stream to the zsession
        let tcp_stream = request.tcp_stream.take().unwrap();

        let peer_addr = tcp_stream.peer_addr().map_err(|e| e.to_string())?.to_string();

        let mut session = Z39Session::new(
            tcp_stream,
            peer_addr,
            self.shutdown.clone(),
            eg::Client::from_bus(bus),
        );

        let result = session.listen()
            .inspect_err(|e| log::error!("{session} exited unexpectedly: {e}"));

        // Take the bus connection back so we can reuse it.
        self.bus = Some(session.client.take_bus());

        result
    }
}


struct Z39Server {
    tcp_listener: TcpListener,
    shutdown: Arc<AtomicBool>,
}

impl mptc::RequestStream for Z39Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let tcp_stream = match self.tcp_listener.accept() {
            Ok((stream, _addr)) => stream,
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // No connection received within the timeout.
                        // Return None to the mptc::Server so it can
                        // perform housekeeping.
                        return Ok(None);
                    }
                    _ => {
                        log::error!("Z39Server accept() failed {e}");
                        return Ok(None);
                    }
                }
            }
        };

        let request = Z39ConnectRequest {
            tcp_stream: Some(tcp_stream),
        };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        Box::new(Z39SessionBroker {
            shutdown: self.shutdown.clone(),
            bus: None,
        })
    }

    fn shutdown(&mut self) {
        // Tell our active workers to stop exit their listen loops.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn main() {
    let options = eg::init::InitOptions {
        skip_logging: false,
        skip_host_settings: true,
        appname: Some("z39-server".to_string()),
    };

    // Connect, parse the IDL, setup logging, etc.
    let client = eg::init::with_options(&options).unwrap();

    // No need to keep this connection open.  Drop it to force disconnect.
    drop(client);

    let settings = z39::Settings {
        implementation_id: Some("EG".to_string()),
        implementation_name: Some("Evergreen".to_string()),
        implementation_version: Some("0.1.0".to_string()),
        ..Default::default()
    };

    settings.apply();

    // TODO command line, etc.
    let tcp_listener = eg::util::tcp_listener(
        "127.0.0.1",
        2210,
        3,
    )
    .unwrap(); // todo error reporting

    let server = Z39Server { 
        tcp_listener,
        shutdown: Arc::new(AtomicBool::new(false)),
    };

    let mut s = mptc::Server::new(Box::new(server));

    s.run();
}
