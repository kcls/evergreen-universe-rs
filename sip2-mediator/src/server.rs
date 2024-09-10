use super::conf::Config;
use super::session::Session;
use eg::osrf;
use evergreen as eg;
use mptc::signals::SignalTracker;
use std::any::Any;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// How often do we wake to check for shutdown signals
const SIP_SHUTDOWN_POLL_INTERVAL: u64 = 5;

/// Wraps the TCP stream created by the initial connection from a SIP client.
struct SipConnectRequest {
    stream: Option<TcpStream>,
}

impl SipConnectRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut SipConnectRequest {
        h.as_any_mut()
            .downcast_mut::<SipConnectRequest>()
            .expect("SipConnectRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for SipConnectRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct SessionFactory {
    shutdown: Arc<AtomicBool>,

    /// Parsed SIP config
    sip_config: Arc<Config>,

    /// OpenSRF bus.
    osrf_bus: Option<eg::osrf::bus::Bus>,

    is_ready: Arc<AtomicBool>,
}

impl mptc::RequestHandler for SessionFactory {
    fn worker_start(&mut self) -> Result<(), String> {
        // Connect to Evergreen when each thread first starts.
        let bus = eg::osrf::bus::Bus::new(osrf::conf::config().client())?;
        self.osrf_bus = Some(bus);

        log::debug!("SessionFactory connected OK to opensrf");

        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("SessionFactory worker_end()");
        // OpenSRF bus will disconnect and cleanup when the thread exits
        Ok(())
    }

    /// Build a new Session from a SipConnectRequest and let the
    /// Session manage the rest of the communication.
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = SipConnectRequest::downcast(&mut request);

        let shutdown = self.shutdown.clone();

        // Set in worker_start
        let osrf_bus = self.osrf_bus.take().unwrap();

        let sip_config = self.sip_config.clone();

        // request.stream is set in the call to next() that produced
        // this request.
        let stream = request.stream.take().unwrap();

        let is_ready = self.is_ready.clone();

        let mut session = Session::new(sip_config, osrf_bus, stream, shutdown, is_ready)?;

        if let Err(e) = session.start() {
            // This is not necessarily an error.  The client may simply
            // have disconnected.  There is no "disconnect" message in
            // SIP -- you just chop off the socket.
            log::info!("{session} exited with message: {e}");
        }

        // Take our bus back so we don't have to reconnect in between
        // SIP clients.  This SIP Session is done with it.
        let mut bus = session.take_bus();

        // Remove any trailing data on the Bus.
        bus.clear_bus()?;

        // Apply a new Bus address to prevent any possibility of
        // trailing message cross-talk.  (Note, it wouldn't do anything,
        // since messages would refer to unknown sessions, but still).
        bus.generate_address();

        self.osrf_bus = Some(bus);

        Ok(())
    }
}

/// Listens for SIP client connections and passes them off to mptc:: for
/// relaying to a Session worker.
pub struct Server {
    /// Parsed config
    sip_config: Arc<Config>,

    /// Set to true of the mptc::Server tells us it's time to shutdown.
    ///
    /// Read by our Sessions
    shutdown: Arc<AtomicBool>,

    /// Inbound SIP connections start here.
    tcp_listener: TcpListener,

    sig_tracker: SignalTracker,

    is_ready: Arc<AtomicBool>,
}

impl mptc::RequestStream for Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let stream = match self.tcp_listener.accept() {
            Ok((stream, _addr)) => stream,
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // See if we need to to into/out of ready mode.
                        self.check_aliveness_signals();

                        // No connection received within the timeout.
                        // Return None to the mptc::Server so it can
                        // perform housekeeping.
                        return Ok(None);
                    }
                    _ => {
                        log::error!("SIPServer accept() failed {e}");
                        return Ok(None);
                    }
                }
            }
        };

        Ok(Some(Box::new(SipConnectRequest {
            stream: Some(stream),
        })))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let sf = SessionFactory {
            shutdown: self.shutdown.clone(),
            is_ready: self.is_ready.clone(),
            sip_config: self.sip_config.clone(),
            osrf_bus: None, // set in worker_start
        };

        Box::new(sf)
    }

    fn reload(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn shutdown(&mut self) {
        // Tell our Session workers it's time to finish any active
        // requests then exit.
        // This only affects active Sessions.  mptc will notify its
        // own idle workers.
        log::info!("Server received mptc shutdown request");

        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl Server {
    /// Check for SIGUSR1 to go into non-ready mode or SIGUSR2 to exit
    /// non-ready mode.
    fn check_aliveness_signals(&mut self) {

        if self.is_ready.load(Ordering::Relaxed) {
            // In ready mode.

            if self.sig_tracker.usr1_is_set() {
                // Received exit-ready-mode signal
                log::info!("Entering non-ready mode");
                self.is_ready.store(false, Ordering::Relaxed);
                self.sig_tracker.clear_usr1();
            }

            if self.sig_tracker.usr2_is_set() {
                // Received superfluous ready-mode signal
                self.sig_tracker.clear_usr2();
            }

        } else {
            // In non-ready mode.

            if self.sig_tracker.usr1_is_set() {
                // Received superflous exit-ready-mode signal
                self.sig_tracker.clear_usr1();
            }

            if self.sig_tracker.usr2_is_set() {
                // Received ready-mode signal
                log::info!("Entering ready mode");
                self.is_ready.store(true, Ordering::Relaxed);
                self.sig_tracker.clear_usr2();
            }
        }
    }

    /// Setup our TCP server socket and create our Server instance.
    pub fn setup(config: Config) -> Result<Server, String> {
        let tcp_listener = eg::util::tcp_listener(
            &config.sip_address,
            config.sip_port,
            SIP_SHUTDOWN_POLL_INTERVAL,
        )?;

        let mut sig_tracker = SignalTracker::new();
        sig_tracker.track_usr1();
        sig_tracker.track_usr2();

        let ready = config.start_in_ready_mode;

        let server = Server {
            tcp_listener,
            sig_tracker,
            sip_config: Arc::new(config),
            is_ready: Arc::new(AtomicBool::new(ready)),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        Ok(server)
    }
}
