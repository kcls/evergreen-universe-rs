use evergreen as eg;

use std::any::Any;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::session::Z39Session;

struct Z39ConnectRequest {
    tcp_stream: Option<TcpStream>,
}

impl Z39ConnectRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut Z39ConnectRequest {
        h.as_any_mut()
            .downcast_mut::<Z39ConnectRequest>()
            .expect("Z39ConnectRequest::downcast() should work")
    }
}

impl mptc::Request for Z39ConnectRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Intermediary for relaying Send'able pieces to the Z39Session.
struct Z39SessionBroker {
    bus: Option<eg::osrf::bus::Bus>,
    shutdown: Arc<AtomicBool>,
}

impl mptc::RequestHandler for Z39SessionBroker {
    fn worker_start(&mut self) -> Result<(), String> {
        let conf = eg::osrf::conf::config().client();
        self.bus = Some(eg::osrf::bus::Bus::new(conf)?);
        Ok(())
    }

    /// Create a Z session to handle the connection and let it run.
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = Z39ConnectRequest::downcast(&mut request);

        // Give the stream to the zsession
        let tcp_stream = request.tcp_stream.take().unwrap();

        let peer_addr = tcp_stream
            .peer_addr()
            .map_err(|e| e.to_string())?
            .to_string();

        // Give the bus to the session while it's active
        let bus = self.bus.take().unwrap();

        let mut session = Z39Session::new(tcp_stream, peer_addr, bus, self.shutdown.clone());

        let result = session
            .listen()
            .inspect_err(|e| log::error!("{session} exited unexpectedly: {e}"));

        // Attempt to shut down the TCP stream regardless of how
        // the conversation ended.
        session.shutdown();

        // Take our bus back so we don't have to reconnect in between
        // z39 clients.  This z39 Session is done with it.
        let mut bus = session.take_bus();

        // Remove any trailing data on the Bus.
        bus.clear_bus()?;

        // Apply a new Bus address to prevent any possibility of
        // trailing message cross-talk.  (Note, it wouldn't do anything,
        // since messages would refer to unknown sessions, but still).
        bus.generate_address();

        result.map_err(|e| e.to_string())
    }
}

pub struct Z39Server {
    tcp_listener: TcpListener,
    shutdown: Arc<AtomicBool>,
}

impl Z39Server {
    pub fn start(tcp_listener: TcpListener) {
        let server = Z39Server {
            tcp_listener,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let mut s = mptc::Server::new(Box::new(server));

        s.run();
    }
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
            bus: None,
            shutdown: self.shutdown.clone(),
        })
    }

    fn shutdown(&mut self) {
        // Tell our active workers to exit their listen loops.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
