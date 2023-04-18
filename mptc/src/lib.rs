pub mod worker;
pub mod server;

pub use server::Server;

/// How often does each component wake and check for shutdown, reload,
/// etc. signals.
pub const SIGNAL_POLL_INTERVAL: u64 = 3;

/// Keep at least this many threads alive at a time.
pub const DEFAULT_MIN_WORKERS: usize = 5;

/// Avoid spawning more than this many threads.
pub const DEFAULT_MAX_WORKERS: usize = 256;

/// Each thread processes this many requests before exiting.
pub const DEFAULT_MAX_WORKER_REQS: usize = 10_000;


pub trait Request: Send + std::any::Any {
    /// Needed for downcasting a generic Request into the
    /// specific type used by the implementor.
    /// Example: fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub trait RequestHandler: Send {
    /// Called from within each worker thread just after spawning.
    fn thread_start(&mut self) -> Result<(), String>;

    /// Called from within each worker thread just before the thread exits.
    fn thread_end(&mut self) -> Result<(), String>;

    /// Process a single request.
    ///
    /// Returns Err of String if request processing failed.  The string
    /// will simply be logged.
    fn process(&mut self, request: Box<dyn Request>) -> Result<(), String>;
}

pub trait RequestStream {
    /// Returns the next incoming request in the stream.
    ///
    /// * `timeout` - Maximum amount of time to wait for the next
    /// Request to arrive.  Timeout support is not strictly required by
    /// the RequestStream, however without a timeout -- i.e. every call
    /// to next() blocks indefinitely -- the server cannot periodically
    /// wake to check for shutdown, etc. signals.
    fn next(&mut self, timeout: u64) -> Result<Option<Box<dyn Request>>, String>;

    /// Factory for creating new RequestHandler instances.
    fn new_handler(&mut self) -> Box<dyn RequestHandler>;
}


