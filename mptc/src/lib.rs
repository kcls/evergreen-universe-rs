#![forbid(unsafe_code)]

pub mod server;
pub mod signals;
pub mod worker;

pub use server::Server;

/// How often does each component wake and check for shutdown, reload,
/// etc. signals.
pub const SIGNAL_POLL_INTERVAL: u64 = 3;

/// Default minimum number of worker threads.
pub const DEFAULT_MIN_WORKERS: usize = 5;

/// Default maximum number of worker threads.
pub const DEFAULT_MAX_WORKERS: usize = 256;

/// Default minimum number of idle workers to maintain.
const DEFAULT_MIN_IDLE_WORKERS: usize = 1;

/// By default, a worker will exit once it has handled this many requests.
///
/// A value of 0 means there is no max.
pub const DEFAULT_MAX_WORKER_REQS: usize = 10_000;

/// Models a single request to be passed to a worker for handling.
pub trait Request: Send + std::any::Any {
    /// Needed for downcasting a generic Request into the
    /// specific type used by the implementor.
    /// Example: fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Trait implemented by code that wishes to handle requests.
pub trait RequestHandler: Send {
    /// Called from within each worker thread just after spawning.
    fn worker_start(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Called from within each worker thread just before the thread exits.
    fn worker_end(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Process a single request.
    ///
    /// Returns Err of String if request processing failed.  The error
    /// string will be logged.
    fn process(&mut self, request: Box<dyn Request>) -> Result<(), String>;
}

pub trait RequestStream {
    /// Returns the next incoming request in the stream.
    ///
    /// If the implementer wants the main server to periodically check
    /// for signals, apply timeout logic in the implementation for
    /// next() and simply return Ok(None) after waking and having
    /// nothing to process.
    fn next(&mut self) -> Result<Option<Box<dyn Request>>, String>;

    /// Factory for creating new RequestHandler instances.
    fn new_handler(&mut self) -> Box<dyn RequestHandler>;

    /// Reload configuration data.
    ///
    /// If the RequestStream cannot reload, it should revert to its
    /// previous state and continue functioning.  It should only return
    /// an Err() if it cannot proceed.
    /// SIGHUP
    fn reload(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Graceful shutdown request (SIGINT)
    fn shutdown(&mut self) {}
}
