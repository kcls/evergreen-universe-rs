use super::client;
use super::conf;
use super::method;
use super::sclient;
use std::any::Any;
use std::sync::Arc;

/// Function that generates ApplicationWorker implementers.
pub type ApplicationWorkerFactory = fn() -> Box<dyn ApplicationWorker>;

/// Opaque collection of read-only, thread-Send'able data.
pub trait ApplicationEnv: Any + Sync + Send {
    fn as_any(&self) -> &dyn Any;
}

pub trait ApplicationWorker: Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Passing copies of Server-global environment data to the worker.
    fn absorb_env(
        &mut self,
        client: client::Client,
        config: Arc<conf::Config>,
        host_settings: Arc<sclient::HostSettings>,
        env: Box<dyn ApplicationEnv>,
    ) -> Result<(), String>;

    /// Called after absorb_env, but before any work occurs.
    fn worker_start(&mut self) -> Result<(), String>;

    /// Called after all work is done and the thread is going away.
    ///
    /// Offers a chance to clean up any resources.
    fn worker_end(&mut self) -> Result<(), String>;
}

pub trait Application {
    /// Application service name, e.g. opensrf.settings
    fn name(&self) -> &str;

    /// Tell the server what methods this application implements.
    ///
    /// Called before workers are spawned.
    fn register_methods(
        &self,
        client: client::Client,
        config: Arc<conf::Config>,
        host_settings: Arc<sclient::HostSettings>,
    ) -> Result<Vec<method::Method>, String>;

    /// Returns a function pointer (ApplicationWorkerFactory) that returns
    /// new ApplicationWorker's when called.
    ///
    /// Dynamic trait objects cannot be passed to threads, but functions
    /// that generate them can.
    fn worker_factory(&self) -> fn() -> Box<dyn ApplicationWorker>;

    /// Creates a new application environment object.
    fn env(&self) -> Box<dyn ApplicationEnv>;
}
