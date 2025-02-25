use crate::limits::RateLimiter;
use evergreen as eg;

use std::any::Any;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::conf;
use crate::session::Z39Session;

/// How often in seconds we ask the RateLimiter to remove stale IPs.
const RATE_LIMITER_SYNC_INTERVAL: u64 = 360;

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

/// Intermediary for managing a long-lived Bus connection between sessions.
struct Z39SessionBroker {
    bus: Option<eg::osrf::bus::Bus>,
    shutdown: Arc<AtomicBool>,
    limits: Option<Arc<Mutex<RateLimiter>>>,
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

        let peer_addr = tcp_stream.peer_addr().map_err(|e| e.to_string())?;

        // Give the bus to the session while it's active
        let bus = self.bus.take().unwrap();

        let mut session = Z39Session::new(
            tcp_stream,
            peer_addr,
            bus,
            self.shutdown.clone(),
            self.limits.clone(),
        );

        let result = session
            .listen()
            .inspect_err(|e| log::error!("{session} exited unexpectedly: {e}"));

        // Attempt to shut down the TCP stream regardless of how
        // the conversation ended.
        session.shutdown();

        /*
        if let Some(limits) = &self.limits {
            if let Ok(mut lock) = limits.lock() {
                // TODO remove addressses only after confirming they are
                // not in use by another thread, which will be required
                // to implement the max-sessions logic.  And even then,
                // we may want to keep the entries for a short time so
                // the disconnects and reconnects do not automatically
                // clear the slate for an address.  A periodic general
                // purge of activity could resolve that.
                // limiter.remove_addr(&peer_addr);

                // lock drops here
            }
        }
        */

        // Take our bus back so we don't have to reconnect in between
        // z39 clients.  This z39 Session is done with it.
        let mut bus = session.take_bus();

        // Remove any trailing data on the Bus.
        bus.clear_bus().ok();

        // Apply a new Bus address to prevent any possibility of
        // trailing message cross-talk.  (Note, it wouldn't do anything,
        // since messages would refer to unknown sessions, but still).
        bus.generate_address();

        self.bus = Some(bus);

        result.map_err(|e| e.to_string())
    }
}

/// MPTC-based Z39 server
pub struct Z39Server {
    tcp_listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    limits: Option<Arc<Mutex<RateLimiter>>>,
    limiter_sync_interval: Duration,
    last_limiter_sync: Instant,
}

impl Z39Server {
    pub fn start(tcp_listener: TcpListener) {
        // If a rate window is defined, setup our rate limiter
        let limits = conf::global().rate_window.map(|window| {
            RateLimiter::new(
                window,
                conf::global().max_msgs_per_window,
                Some(conf::global().ip_whitelist.clone()),
            )
            .into_shared()
        });

        let server = Z39Server {
            tcp_listener,
            limits,
            last_limiter_sync: Instant::now(),
            limiter_sync_interval: Duration::from_secs(RATE_LIMITER_SYNC_INTERVAL),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let mut s = mptc::Server::new(Box::new(server));

        s.set_max_workers(conf::global().max_workers);
        s.set_min_workers(conf::global().min_workers);
        s.set_min_idle_workers(conf::global().min_idle_workers);

        s.run();
    }

    /// Periodically tell the limiter to purge stale data.
    fn sync_limiter(&mut self) {
        let Some(limiter) = &self.limits else { return };

        if (Instant::now() - self.last_limiter_sync) < self.limiter_sync_interval {
            return;
        }

        match limiter.lock() {
            Ok(mut l) => l.sync(),
            Err(e) => log::error!("limiter sync error: {e}"),
        }

        log::debug!("limiter synced");

        self.last_limiter_sync = Instant::now();
    }
}

impl mptc::RequestStream for Z39Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let tcp_stream = match self.tcp_listener.accept() {
            Ok((stream, _addr)) => stream,
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // Nothing to process; do some housekeeping.
                        self.sync_limiter();

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
            limits: self.limits.clone(),
            shutdown: self.shutdown.clone(),
        })
    }

    fn shutdown(&mut self) {
        // Tell our active workers to exit their listen loops.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
