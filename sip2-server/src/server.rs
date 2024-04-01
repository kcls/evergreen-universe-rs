use super::conf;
use super::conf::Config;
use super::session::Session;
use eg::EgValue;
use evergreen as eg;
use mptc;
use std::any::Any;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// If we get this many TCP errors in a row, with no successful connections
/// in between, exit.
const MAX_TCP_ERRORS: usize = 100;

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

    sip_config: Arc<Config>,

    idl: Arc<eg::idl::Parser>,

    osrf_conf: Arc<eg::osrf::conf::Config>,

    /// OpenSRF bus.
    osrf_bus: Option<eg::osrf::bus::Bus>,

    /// Cache of org unit shortnames and IDs.
    org_cache: HashMap<i64, EgValue>,
}

impl mptc::RequestHandler for SessionFactory {
    fn worker_start(&mut self) -> Result<(), String> {
        let bus = eg::osrf::bus::Bus::new(self.osrf_conf.client())?;
        self.osrf_bus = Some(bus);
        eg::idl::set_thread_idl(&self.idl);

        log::debug!("SessionFactory connected OK to opensrf");

        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("SessionFactory worker_end()");
        // OpenSRF bus will disconnect and cleanup once
        Ok(())
    }

    /// Build a new Session from a SipConnectRequest and let the
    /// Session manage the rest of the communication.
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = SipConnectRequest::downcast(&mut request);

        let sip_conf = self.sip_config.clone();
        let org_cache = self.org_cache.clone();
        let shutdown = self.shutdown.clone();
        let osrf_conf = self.osrf_conf.clone();

        // Set in worker_start
        let osrf_bus = self.osrf_bus.take().unwrap();

        // request.stream is set in the call to next() that produced
        // this request.
        let stream = request.stream.take().unwrap();

        let mut session = Session::new(sip_conf, osrf_conf, osrf_bus, stream, shutdown, org_cache);

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
        // since messages would refer to unknown sessions, but still..).
        bus.generate_address();

        self.osrf_bus = Some(bus);

        Ok(())
    }
}

/// Listens for SIP client connections and passes them off to mptc:: for
/// relaying to a Session worker.
pub struct Server {
    eg_ctx: eg::init::Context,

    /// Parsed config
    sip_config: Arc<Config>,

    /// Path the SIP config so it can be reloaded on request.
    sip_config_file: String,

    /// Set to true of the mptc::Server tells us it's time to shutdown.
    ///
    /// Read by our Sessions
    shutdown: Arc<AtomicBool>,

    /// Cache of org unit shortnames and IDs.
    org_cache: Option<HashMap<i64, EgValue>>,

    tcp_error_count: usize,

    /// Inbound SIP connections start here.
    tcp_listener: TcpListener,
}

impl mptc::RequestStream for Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let stream = match self.tcp_listener.accept() {
            Ok((stream, _addr)) => {
                self.tcp_error_count = 0;
                stream
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // No connection received within the timeout.
                        // Return None to the mptc::Server so it can
                        // perform housekeeping.
                        return Ok(None);
                    }
                    _ => {
                        log::error!(
                            "SIPServer accept() failed: error_count={} {e}",
                            self.tcp_error_count
                        );
                        self.tcp_error_count += 1;

                        if self.tcp_error_count > MAX_TCP_ERRORS {
                            // Net IO errors can happen for all kinds of reasons.
                            // https://doc.rust-lang.org/stable/std/io/enum.ErrorKind.html
                            // Concern is some of these errors could put
                            // us into an infinite loop of "stuff is broken".
                            // Break out of the loop if we've hit too many.
                            return Err(format!("SIPServer exited on too many connect errors"));
                        }

                        // Error, but not too many yet.
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
            sip_config: self.sip_config.clone(),
            idl: self.eg_ctx.idl().clone(),
            osrf_conf: self.eg_ctx.config().clone(),
            osrf_bus: None, // set in worker_start
            org_cache: self.org_cache.as_ref().unwrap().clone(),
        };

        Box::new(sf)
    }

    fn reload(&mut self) -> Result<(), String> {
        match Server::load_config(&self.sip_config_file) {
            Ok(c) => self.sip_config = Arc::new(c),
            Err(e) => log::error!("Error reloading config.  Using old config. {e}"),
        }

        // Fails if we cannot talk to OpenSRF.
        self.precache()?;

        // No need to inform our worker sessions that we're reloading.
        // mptc will clear/reload idle workers, and there's no need to
        // force-exit a connected session.

        Ok(())
    }

    fn shutdown(&mut self) {
        // Tell our Session workers it's time to finish any active
        // requests then exit.
        // This only affects active Sessions.  mptc will notify its
        // own idle workers.
        log::info!("Server received mptc shutdown request");

        self.shutdown.store(true, Ordering::Relaxed);
        self.eg_ctx.client().clear().ok();
    }
}

impl Server {
    pub fn sip_config(&self) -> &Config {
        &self.sip_config
    }

    pub fn setup(sip_config_file: &str, eg_ctx: eg::init::Context) -> Result<Server, String> {
        let sip_config = Server::load_config(sip_config_file)?;

        let tcp_listener = eg::util::tcp_listener(
            sip_config.sip_address(),
            sip_config.sip_port(),
            conf::SIP_SHUTDOWN_POLL_INTERVAL,
        )?;

        let mut server = Server {
            eg_ctx,
            tcp_listener,
            sip_config: Arc::new(sip_config),
            sip_config_file: sip_config_file.to_string(),
            org_cache: None,
            tcp_error_count: 0,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        server.precache()?;

        Ok(server)
    }

    fn load_config(filename: &str) -> Result<Config, String> {
        let mut sip_conf = conf::Config::new();
        sip_conf.read_yaml(filename)?;
        Ok(sip_conf)
    }

    /// Pre-cache data that's universally useful.
    fn precache(&mut self) -> Result<(), String> {
        let mut e = eg::Editor::new(self.eg_ctx.client());

        let search = eg::hash! {
            "id": {"!=": EgValue::Null},
        };

        let mut orgs = e.search("aou", search)?;

        let mut map = HashMap::new();

        for org in orgs.drain(..) {
            map.insert(org.id()?, org);
        }

        self.org_cache = Some(map);

        Ok(())
    }
}
