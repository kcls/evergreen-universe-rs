use super::conf::Config;
use super::monitor::{Monitor, MonitorAction, MonitorEvent};
use super::session::Session;
use evergreen as eg;
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
}

impl Server {
    pub fn new(sip_config: Config, ctx: eg::init::Context) -> Server {
        let (tx, rx): (
            mpsc::Sender<MonitorEvent>,
            mpsc::Receiver<MonitorEvent>
        ) = mpsc::channel();

        Server {
            ctx,
            sip_config,
            sesid: 0,
            from_monitor_tx: tx,
            from_monitor_rx: rx,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn serve(&mut self) {
        log::info!("SIP2Meditor server staring up");

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
            let sesid = self.next_sesid();

            match stream {
                Ok(s) => self.dispatch(&pool, s, sesid, self.shutdown.clone()),
                Err(e) => log::error!("Error accepting TCP connection {}", e),
            }

            self.process_monitor_events();

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }

        log::info!("SIP2Mediator shutting down; waiting for threads to complete");

        pool.join();
    }

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

            match event.action() {
                MonitorAction::AddAccount(account) => {
                    self.sip_config.add_account(account);
                }

                MonitorAction::DisableAccount(username) => {
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

        // TODO
        // Just because a thread is 'active' does not mean the SIP
        // client it manages is sending requests.  It may just be hunkered
        // down on the socket, idle for long stretches of time.
        // Consider an option to send a message to SIP threads telling
        // idle threads to self-destruct in cases where we hit/approach
        // the max thread limit.
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

        pool.execute(move ||
            Session::run(conf, osrf_config, idl, stream, sesid, shutdown));
    }
}
