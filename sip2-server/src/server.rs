use super::conf;
use super::conf::Config;
use super::session::Session;
use evergreen as eg;
use opensrf as osrf;
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::any::Any;
use mptc;

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

    osrf_conf: Arc<osrf::conf::Config>,

    /// OpenSRF bus.
    osrf_bus: Option<osrf::bus::Bus>,

    /// Cache of org unit shortnames and IDs.
    org_cache: HashMap<i64, json::JsonValue>,
}

impl mptc::RequestHandler for SessionFactory {
    fn worker_start(&mut self) -> Result<(), String> {
        log::debug!("SessionFactory connecting to opensrf");

        let bus = osrf::bus::Bus::new(self.osrf_conf.client())?;
        self.osrf_bus = Some(bus);

        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("SessionFactory worker_end()");
        // OpenSRF bus will disconnect and cleanup once
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = SipConnectRequest::downcast(&mut request);

        let sip_conf = self.sip_config.clone();
        let org_cache = self.org_cache.clone();
        let shutdown = self.shutdown.clone();
        let idl = self.idl.clone();
        let osrf_conf = self.osrf_conf.clone();

        // Set in worker_start
        let osrf_bus = self.osrf_bus.take().unwrap();

        // request.stream is set in the call to next() that produced
        // this request.
        let stream = request.stream.take().unwrap();

        let mut session =
            Session::new(sip_conf, osrf_conf, osrf_bus, idl, stream, shutdown, org_cache);

        if let Err(e) = session.start() {
            // This is not necessarily an error.  The client may simply
            // have disconnected.  There is no "disconnect" message in
            // SIP -- you just chop off the socket.
            log::info!("{session} exited with message: {e}");
        }

        // Take our bus back so we don't have to reconnect in between
        // SIP clients.  The session is done with it.
        let mut bus = session.take_bus();

        // Remove any trailing data on the Bus.
        bus.clear_bus()?;

        // TODO set a new bus address to avoid any lingering cross-talk
        // from previous sessions.

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
    org_cache: Option<HashMap<i64, json::JsonValue>>,

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
                        log::error!("SIPServer accept() failed: error_count={} {e}", self.tcp_error_count);
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

        Ok(Some(Box::new(SipConnectRequest { stream: Some(stream) })))
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

        Ok(())
    }

    fn shutdown(&mut self) {
        // Tell our Session workers it's time to finish any active
        // requests then exit.
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

        let bind = format!(
            "{}:{}",
            sip_config.sip_address(),
            sip_config.sip_port()
        );

        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .or_else(|e| Err(format!("Socket::new() failed with {e}")))?;

        // When we stop/start the service, the address may briefly linger
        // from open (idle) client connections.
        socket
            .set_reuse_address(true)
            .or_else(|e| Err(format!("Error setting reuse address: {e}")))?;

        let address: SocketAddr = bind
            .parse()
            .or_else(|e| Err(format!("Error parsing listen address: {bind}: {e}")))?;

        socket
            .bind(&address.into())
            .or_else(|e| Err(format!("Error binding to address: {bind}: {e}")))?;

        // 128 == backlog
        socket
            .listen(128)
            .or_else(|e| Err(format!("Error listending on socket {bind}: {e}")))?;

        // We need a read timeout so we can wake periodically to check
        // for shutdown signals.
        let polltime = Duration::from_secs(conf::SIP_SHUTDOWN_POLL_INTERVAL);

        socket
            .set_read_timeout(Some(polltime))
            .or_else(|e| Err(format!("Error setting socket read_timeout: {e}")))?;

        let tcp_listener: TcpListener = socket.into();

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
        let mut e = eg::Editor::new(self.eg_ctx.client(), self.eg_ctx.idl());

        let search = json::object! {
            id: {"!=": json::JsonValue::Null},
        };

        let orgs = e.search("aou", search)?;

        let mut map = HashMap::new();

        for org in orgs {
            map.insert(eg::util::json_int(&org["id"])?, org.clone());
        }

        self.org_cache = Some(map);

        Ok(())
    }
}

