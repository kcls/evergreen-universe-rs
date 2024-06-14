/// Signal Tracking
use signal_hook as sigs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

pub const SIG_FAST_SHUTDOWN: i32 = sigs::consts::SIGTERM;
pub const SIG_GRACEFUL_SHUTDOWN: i32 = sigs::consts::SIGINT;
pub const SIG_RELOAD: i32 = sigs::consts::SIGHUP;

/// Tracks various signals so threaded, etc. applications can
/// easily respond to received signals.
///
/// Generally, tracking is setup by the main thread and the SignalTracker
/// is cloned and distributed to the worker/spawned threads, so all
/// parties refer to the same tracker for signal events.
///
/// track_ methods panic on failure, based on the assumption that
/// applications register event handlers early on and should exit early
/// if basic signal handling cannot be setup.
#[derive(Debug, Clone)]
pub struct SignalTracker {
    graceful_shutdown: Arc<AtomicBool>,
    fast_shutdown: Arc<AtomicBool>,
    reload: Arc<AtomicBool>,
    reload_request_time: Arc<AtomicU64>,

    /// Avoid duplicate signal handlers
    graceful_shutdown_tracked: bool,
    fast_shutdown_tracked: bool,
    reload_tracked: bool,
}

impl Default for SignalTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTracker {
    pub fn new() -> SignalTracker {
        SignalTracker {
            graceful_shutdown: Arc::new(AtomicBool::new(false)),
            fast_shutdown: Arc::new(AtomicBool::new(false)),
            reload: Arc::new(AtomicBool::new(false)),
            reload_request_time: Arc::new(AtomicU64::new(0)),
            graceful_shutdown_tracked: false,
            fast_shutdown_tracked: false,
            reload_tracked: false,
        }
    }

    /// Directly initiate a graceful shutdown request.
    pub fn request_graceful_shutdown(&self) {
        self.graceful_shutdown.store(true, Ordering::Relaxed);
    }

    /// Directly initiate a fast shutdown request.
    pub fn request_fast_shutdown(&self) {
        self.fast_shutdown.store(true, Ordering::Relaxed);
    }

    /// Directly initiate a reload request.
    pub fn request_reload(&self) {
        self.reload.store(true, Ordering::Relaxed);
    }

    /// True if any shutdown signals have been received.
    pub fn any_shutdown_requested(&self) -> bool {
        self.graceful_shutdown_requested() || self.fast_shutdown_requested()
    }

    /// Activate graceful shutdown signal tracking.
    ///
    /// ```
    /// use mptc::signals::SignalTracker;
    /// use signal_hook::low_level::raise;
    ///
    /// let mut tracker = SignalTracker::new();
    /// tracker.track_graceful_shutdown();
    ///
    /// raise(mptc::signals::SIG_GRACEFUL_SHUTDOWN).expect("Signal Sent");
    ///
    /// assert!(tracker.graceful_shutdown_requested());
    /// assert!(tracker.any_shutdown_requested());
    ///
    /// ```
    pub fn track_graceful_shutdown(&mut self) {
        if self.graceful_shutdown_tracked {
            log::warn!("Already tracking graceful shutdowns");
            return;
        }

        let result = sigs::flag::register(SIG_GRACEFUL_SHUTDOWN, self.graceful_shutdown.clone());

        if let Err(e) = result {
            panic!("Cannot register graceful shutdown handler: {}", e);
        }

        self.graceful_shutdown_tracked = true;
    }

    pub fn graceful_shutdown_requested(&self) -> bool {
        self.graceful_shutdown.load(Ordering::Relaxed)
    }

    /// Activate fast shutdown signal tracking.
    ///
    /// ```
    /// use mptc::signals::SignalTracker;
    /// use signal_hook::low_level::raise;
    ///
    /// let mut tracker = SignalTracker::new();
    /// tracker.track_fast_shutdown();
    ///
    /// raise(mptc::signals::SIG_FAST_SHUTDOWN).expect("Signal Sent");
    ///
    /// assert!(tracker.fast_shutdown_requested());
    /// assert!(tracker.any_shutdown_requested());
    /// ```
    pub fn track_fast_shutdown(&mut self) {
        if self.fast_shutdown_tracked {
            log::warn!("Already tracking fast shutdowns");
            return;
        }

        let result = sigs::flag::register(SIG_FAST_SHUTDOWN, self.fast_shutdown.clone());

        if let Err(e) = result {
            panic!("Cannot register fast shutdown handler: {}", e);
        }

        self.fast_shutdown_tracked = true;
    }

    pub fn fast_shutdown_requested(&self) -> bool {
        self.fast_shutdown.load(Ordering::Relaxed)
    }

    /// Activate fast shutdown signal tracking.
    ///
    /// ```
    /// use mptc::signals::SignalTracker;
    /// use signal_hook::low_level::raise;
    ///
    /// let mut tracker = SignalTracker::new();
    /// tracker.track_reload();
    ///
    /// raise(mptc::signals::SIG_RELOAD).expect("Signal Sent");
    ///
    /// assert!(tracker.reload_requested());
    ///
    /// tracker.handle_reload_requested();
    ///
    /// assert!(!tracker.reload_requested());
    /// assert!(tracker.reload_request_time() > 0);
    ///
    /// ```
    pub fn track_reload(&mut self) {
        if self.reload_tracked {
            log::warn!("Already tracking reload signals");
            return;
        }

        let result = sigs::flag::register(SIG_RELOAD, self.reload.clone());

        if let Err(e) = result {
            panic!("Cannot register fast shutdown handler: {}", e);
        }

        self.reload_tracked = true;
    }

    pub fn reload_requested(&self) -> bool {
        self.reload.load(Ordering::Relaxed)
    }

    /// Reset the reload request flag, which may be needed again later,
    /// and store the time of the most recent reload request.
    pub fn handle_reload_requested(&mut self) {
        self.reload.store(false, Ordering::Relaxed);

        let epoch: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Epoch Duration Is Sane")
            .as_millis()
            // should be fine for another half billion years or so, I think.
            .try_into()
            .expect("Epoch Milliseconds is way too big?");

        self.reload_request_time.store(epoch, Ordering::Relaxed);
    }

    /// Epoch milliseconds of the reload request time.
    ///
    /// Workers spawned before this time know to exit, since they
    /// are presumably operating with outdated configuration, etc.
    /// information.
    pub fn reload_request_time(&self) -> u64 {
        self.reload_request_time.load(Ordering::Relaxed)
    }
}
