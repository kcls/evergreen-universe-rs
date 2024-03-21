use super::conf::Config;
use super::session::Session;
use log::{debug, error, info, warn};
use std::net;
use std::net::TcpListener;
use std::net::TcpStream;
use threadpool::ThreadPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
//use signal_hook as sigs; TODO

pub struct Server {
    config: Config,
    /// If true, we're shutting down.
    shutdown: Arc<AtomicBool>,
}

impl Server {
    pub fn new(config: Config) -> Server {
        Server {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn serve(&mut self) {
        info!("SIP2Meditor server staring up");

        if let Err(e) = self.setup_signal_handlers() {
            log::error!("Cannot setup signal handlers: {e}");
            return;
        }

        let pool = ThreadPool::new(self.config.max_clients);

        let bind = format!("{}:{}", self.config.sip_address, self.config.sip_port);

        let listener = TcpListener::bind(bind).expect("Error starting SIP server");

        for stream in listener.incoming() {
            match stream {
                Ok(s) => self.dispatch(&pool, s),
                Err(e) => error!("Error accepting TCP connection {}", e),
            }

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("Shutdown signal received, exiting listen loop");
                break;
            }
        }

        info!("SIP2Mediator shutting down; waiting for threads to complete");

        pool.join();
    }

    /// Pass the new SIP TCP stream off to a thread for processing.
    fn dispatch(&self, pool: &ThreadPool, stream: TcpStream) {
        debug!(
            "Accepting new SIP connection; active={} pending={}",
            pool.active_count(),
            pool.queued_count()
        );

        let threads = pool.active_count() + pool.queued_count();

        if threads >= self.config.max_clients {
            warn!(
                "Max clients={} reached.  Rejecting new connections",
                self.config.max_clients
            );

            if let Err(e) = stream.shutdown(net::Shutdown::Both) {
                error!("Error shutting down SIP TCP connection: {}", e);
            }

            return;
        }

        // Hand the stream off for processing.
        let conf = self.config.clone();
        let shutdown = self.shutdown.clone();
        pool.execute(|| Session::run(conf, stream, shutdown));
    }

    fn setup_signal_handlers(&self) -> Result<(), String> {
        /* Maybe later.
        if let Err(e) = sigs::flag::register(sigs::consts::SIGHUP, self.reload.clone()) {
            return Err(format!("Cannot register HUP signal: {e}"));
        }
        */

        /* Disabling for now until the tcp stream timeout is in place,
         * otherwise SIGINT will just hang if no traffic is flowing.
        if let Err(e) = sigs::flag::register(sigs::consts::SIGINT, self.shutdown.clone()) {
            return Err(format!("Cannot register INT signal: {e}"));
        }
        */

        Ok(())
    }

}
