use super::conf::Config;
use super::monitor::{Monitor, MonitorAction, MonitorEvent};
use super::session::Session;
use evergreen as eg;
use std::collections::HashMap;
use std::net;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
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

    pub fn serve(&mut self) {
        log::info!("SIP2Meditor server staring up");

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

        let listener = TcpListener::bind(bind).expect("Error starting SIP server");

        for stream in listener.incoming() {
            // This can happen while we are waiting for a TCP connect.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let sesid = self.next_sesid();

            match stream {
                Ok(s) => self.dispatch(&pool, s, sesid, self.shutdown.clone()),
                Err(e) => log::error!("Error accepting TCP connection {}", e),
            }

            self.process_monitor_events();

            // This can happen while we are launching a new thread.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }

        log::info!("Server shutting down; waiting for threads to complete");

        pool.join();
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
