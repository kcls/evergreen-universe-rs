use super::conf::Config;
use super::session::Session;
use evergreen::util as egutil;
use std::net;
use std::net::TcpStream;
use threadpool::ThreadPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use signal_hook as sigs;

/// How often do we wake up from blocking on the main socket to check
/// for shutdown, etc. signals.
const SIG_POLL_INTERVAL: u64 = 5;

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

    /// Listen on the SIP socket and spawn a handler Session per connection.
    pub fn serve(&mut self) {
        log::info!("SIP2Meditor server staring up");

        if let Err(e) = self.setup_signal_handlers() {
            log::error!("Cannot setup signal handlers: {e}");
            return;
        }

        let pool = ThreadPool::new(self.config.max_clients);

        let listener_result = egutil::tcp_listener(
            &self.config.sip_address,
            self.config.sip_port,
            SIG_POLL_INTERVAL
        );

        let listener = match listener_result {
            Ok(l) => l,
            Err(e) => {
                let msg = format!(
                    "Cannot listen for connections at {}:{} {e}",
                    self.config.sip_address,
                    self.config.sip_port
                );
                log::error!("{msg}");
                panic!("{}", msg);
            }
        };

        for stream in listener.incoming() {
            match stream {
                // New stream connected; hand it off.
                Ok(s) => self.dispatch(&pool, s),

                Err(e) => match e.kind() {
                    // socket read timeout is OK.
                    std::io::ErrorKind::WouldBlock => {}

                    // Something went wrong.
                    _ => log::error!("Error accepting TCP connection {e}"),
                },
            }

            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("Shutdown signal received, exiting listen loop");
                break;
            }
        }

        log::info!("SIP2Mediator shutting down; waiting for threads to complete");

        pool.join();
    }

    /// Pass the new SIP TCP stream off to a thread for processing.
    fn dispatch(&self, pool: &ThreadPool, stream: TcpStream) {
        log::debug!(
            "Accepting new SIP connection; active={} pending={}",
            pool.active_count(),
            pool.queued_count()
        );

        let threads = pool.active_count() + pool.queued_count();

        if threads >= self.config.max_clients {
            log::warn!(
                "Max clients={} reached.  Rejecting new connections",
                self.config.max_clients
            );

            if let Err(e) = stream.shutdown(net::Shutdown::Both) {
                log::error!("Error shutting down SIP TCP connection: {}", e);
            }

            return;
        }

        // Hand the stream off for processing.
        let conf = self.config.clone();
        let shutdown = self.shutdown.clone();
        pool.execute(|| Session::run(conf, stream, shutdown));
    }

    /// Handle signals
    fn setup_signal_handlers(&self) -> Result<(), String> {

        // If we receive a SIGINT, set the shutdown flag so
        // we can gracefully shutdown.
        if let Err(e) = sigs::flag::register(sigs::consts::SIGINT, self.shutdown.clone()) {
            return Err(format!("Cannot register INT signal: {e}"));
        }

        Ok(())
    }
}
