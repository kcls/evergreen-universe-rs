use super::conf;
use super::conf::Config;
use super::session::Session;
use evergreen as eg;
use signal_hook;
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;
use std::net;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use threadpool::ThreadPool;

pub struct Server {
    ctx: eg::init::Context,
    sip_config: Config,
    sip_config_file: String,
    sesid: usize,
    /// If this ever contains a true, we shut down.
    shutdown: Arc<AtomicBool>,
    reload: Arc<AtomicBool>,
    /// Cache of org unit shortnames and IDs.
    org_cache: HashMap<i64, json::Value>,
}

impl Server {
    pub fn new(sip_config_file: &str, ctx: eg::init::Context) -> Server {
        let sip_config = Server::load_config(sip_config_file).expect("Error reading config");

        Server {
            ctx,
            sesid: 0,
            sip_config,
            sip_config_file: sip_config_file.to_string(),
            org_cache: HashMap::new(),
            reload: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn load_config(filename: &str) -> Result<Config, String> {
        let mut sip_conf = conf::Config::new();
        sip_conf.read_yaml(filename)?;
        Ok(sip_conf)
    }

    fn sighup(&mut self) {
        log::info!("SIGHUP received.  Reloading config");
        match Server::load_config(&self.sip_config_file) {
            Ok(c) => self.sip_config = c,
            Err(e) => log::error!("Error reloading config.  Using old config. {e}"),
        }
        self.reload.store(false, Ordering::Relaxed);
    }

    /// Pre-cache data that's universally useful.
    fn precache(&mut self) -> Result<(), String> {
        let mut e = eg::Editor::new(self.ctx.client(), self.ctx.idl());

        let search = json::object! {
            id: {"!=": json::Value::Null},
        };

        let orgs = e.search("aou", search)?;

        for org in orgs {
            self.org_cache
                .insert(eg::util::json_int(&org["id"])?, org.clone());
        }

        Ok(())
    }

    fn setup_signal_handlers(&self) -> Result<(), String> {
        // TERM and INT result in a graceful shutdown
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            if let Err(e) = signal_hook::flag::register(sig, self.shutdown.clone()) {
                Err(format!("Cannot register signal handler: {e}"))?;
            }
        }

        // HUP causes us to reload our configuration.
        if let Err(e) =
            signal_hook::flag::register(signal_hook::consts::SIGHUP, self.reload.clone())
        {
            Err(format!("Cannot register HUP signal: {e}"))?;
        }

        Ok(())
    }

    pub fn serve(&mut self) -> Result<(), String> {
        log::info!("SIP2Meditor server starting");

        self.setup_signal_handlers()?;
        self.precache()?;

        let pool = ThreadPool::new(self.sip_config.max_clients());

        let bind = format!(
            "{}:{}",
            self.sip_config.sip_address(),
            self.sip_config.sip_port()
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

        let listener: TcpListener = socket.into();

        let mut error_count = 0;

        loop {
            // Check flags after every block on accept(), which may result
            // in a 'continue', bypassing the mid-loop checks.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            if self.reload.load(Ordering::Relaxed) {
                self.sighup();
            }

            let client_socket = match listener.accept() {
                Ok((s, _)) => s,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            // Poll timeout -- circle back and try again.
                            continue;
                        }
                        _ => {
                            log::error!("SIPServer accept() failed: error_count={error_count} {e}");
                            error_count += 1;
                            if error_count > 100 {
                                // Net IO errors can happen for all kinds of reasons.
                                // https://doc.rust-lang.org/stable/std/io/enum.ErrorKind.html
                                // Concern is some of these errors could put
                                // us into an infinite loop of "stuff is broken".
                                // Break out of the loop if we've hit too many.
                                log::error!("SIPServer exited on too many connect errors");
                                break;
                            }
                            // Error, but not too many yet.
                            continue;
                        }
                    }
                }
            };

            // And check flags before processing messages
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            if self.reload.load(Ordering::Relaxed) {
                self.sighup();
            }

            let sesid = self.next_sesid();
            self.dispatch(&pool, client_socket.into(), sesid, self.shutdown.clone());
        }

        self.ctx.client().clear().ok();

        log::debug!("Server shutting down; waiting for threads to complete");

        pool.join();

        log::info!("All threads complete.  Shutting down");

        Ok(())
    }

    fn next_sesid(&mut self) -> usize {
        self.sesid += 1;
        self.sesid
    }

    /// Pass the new SIP TCP stream off to a thread for processing.
    fn dispatch(
        &self,
        pool: &ThreadPool,
        stream: TcpStream,
        sesid: usize,
        shutdown: Arc<AtomicBool>,
    ) {
        log::info!(
            "Accepting new SIP connection; active={} pending={}",
            pool.active_count(),
            pool.queued_count()
        );

        let threads = pool.active_count() + pool.queued_count();
        let maxcon = self.sip_config.max_clients();

        log::debug!("Working thread count = {threads}");

        // It does no good to queue up a new connection if we hit max
        // threads, because active threads have a long life time, even
        // when they are not currently busy.
        if threads >= maxcon {
            log::warn!("Max clients={maxcon} reached.  Rejecting new connections");

            if let Err(e) = stream.shutdown(net::Shutdown::Both) {
                log::error!("Error shutting down SIP TCP connection: {}", e);
            }

            return;
        }

        // Hand the stream off for processing.
        let conf = self.sip_config.clone();
        let idl = self.ctx.idl().clone();
        let osrf_config = self.ctx.config().clone();
        let org_cache = self.org_cache.clone();

        pool.execute(move || {
            Session::run(conf, osrf_config, idl, stream, sesid, shutdown, org_cache)
        });
    }
}
