use super::conf;
use super::conf::Config;
use super::monitor::{Monitor, MonitorAction, MonitorEvent};
use super::session::Session;
use evergreen as eg;
use signal_hook;
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;
use std::net;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use threadpool::ThreadPool;

pub struct Server {
    ctx: eg::init::Context,
    sip_config: Config,
    sesid: usize,
    /// If this ever contains a true, we shut down.
    shutdown: Arc<AtomicBool>,
    from_monitor_tx: mpsc::Sender<MonitorEvent>,
    from_monitor_rx: mpsc::Receiver<MonitorEvent>,
    /// Cache of org unit shortnames and IDs.
    org_cache: HashMap<i64, json::JsonValue>,
}

impl Server {
    pub fn new(sip_config: Config, ctx: eg::init::Context) -> Server {
        let (tx, rx): (mpsc::Sender<MonitorEvent>, mpsc::Receiver<MonitorEvent>) = mpsc::channel();

        Server {
            ctx,
            sip_config,
            sesid: 0,
            from_monitor_tx: tx,
            from_monitor_rx: rx,
            org_cache: HashMap::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Pre-cache data that's universally useful.
    fn precache(&mut self) -> Result<(), String> {
        let mut e = eg::Editor::new(self.ctx.client(), self.ctx.idl());

        let search = json::object! {
            id: {"!=": json::JsonValue::Null},
        };

        let orgs = e.search("aou", search)?;

        for org in orgs {
            self.org_cache
                .insert(eg::util::json_int(&org["id"])?, org.clone());
        }

        Ok(())
    }

    fn setup_signal_handlers(&self) -> Result<(), String> {
        // If any of these signals occur, our self.stopping flag will be set to true
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            if let Err(e) = signal_hook::flag::register(sig, self.shutdown.clone()) {
                return Err(format!("Cannot register signal handler: {e}"));
            }
        }

        Ok(())
    }

    pub fn serve(&mut self) {
        log::info!("SIP2Meditor server staring up");

        if let Err(e) = self.setup_signal_handlers() {
            log::error!("Error setting signal handler: {e}");
            return;
        }

        if let Err(e) = self.precache() {
            log::error!("Error pre-caching SIP data: {e}");
            return;
        }

        let pool = ThreadPool::new(self.sip_config.max_clients());

        if self.sip_config.monitor_enabled() {
            log::info!("Starting monitor thread");

            let mut monitor = Monitor::new(
                self.sip_config.clone(),
                self.from_monitor_tx.clone(),
                self.shutdown.clone(),
            );

            pool.execute(move || monitor.run());
        }

        let bind = format!(
            "{}:{}",
            self.sip_config.sip_address(),
            self.sip_config.sip_port()
        );

        let socket = match Socket::new(Domain::IPV4, Type::STREAM, None) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Socket::new() failed with {e}");
                return;
            }
        };

        // When we stop/start the service, the address may briefly linger
        // from open (idle) client connections.
        if let Err(e) = socket.set_reuse_address(true) {
            log::error!("Error setting reuse address: {e}");
            return;
        }

        let address: SocketAddr = match bind.parse() {
            Ok(a) => a,
            Err(e) => {
                log::error!("Error parsing listen address: {bind}: {e}");
                return;
            }
        };

        if let Err(e) = socket.bind(&address.into()) {
            log::error!("Error binding to address: {bind}: {e}");
            return;
        }

        if let Err(e) = socket.listen(128) {
            // 128 == backlog
            log::error!("Error listending on socket {bind}: {e}");
            return;
        }

        // We need a read timeout so we can wake periodically to check
        // for shutdown signals.
        if let Err(e) =
            socket.set_read_timeout(Some(Duration::from_secs(conf::SIP_SHUTDOWN_POLL_INTERVAL)))
        {
            log::error!("Error setting socket read_timeout: {e}");
            return;
        }

        let listener: TcpListener = socket.into();

        loop {
            // This can happen while we are waiting for a TCP connect.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let client_socket = match listener.accept() {
                Ok((s, _)) => s,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            log::trace!("Accept timed out.  Trying again");
                            continue;
                        }
                        _ => {
                            log::error!("SIPServer accept() failed: {e}");
                            continue; // break?
                        }
                    }
                }
            };

            // This can happen while we are waiting for a TCP connect.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let sesid = self.next_sesid();
            self.dispatch(&pool, client_socket.into(), sesid, self.shutdown.clone());

            self.process_monitor_events();
        }

        self.ctx.client().clear().ok();

        log::info!("Server shutting down; waiting for threads to complete");

        pool.join();

        log::info!("All threads complete.  Shutting down");
    }

    /// Check for messages from the monitor thread.
    fn process_monitor_events(&mut self) {
        loop {
            let event = match self.from_monitor_rx.try_recv() {
                Ok(e) => e,
                Err(e) => match e {
                    // No more events to process
                    mpsc::TryRecvError::Empty => return,

                    // Monitor thread exited.
                    mpsc::TryRecvError::Disconnected => {
                        log::error!("Monitor thread exited.  Shutting down.");
                        self.shutdown.store(true, Ordering::Relaxed);
                        return;
                    }
                },
            };

            log::debug!("Server received monito event: {event:?}");

            match event.action() {
                MonitorAction::AddAccount(account) => {
                    log::info!("Adding new account {}", account.sip_username());
                    self.sip_config.add_account(account);
                }

                MonitorAction::DisableAccount(username) => {
                    log::info!("Disabling account {username}");
                    self.sip_config.remove_account(username);
                }
            }
        }
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
        let maxcon = self.sip_config.max_clients() + 1; // +1 monitor thread

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
