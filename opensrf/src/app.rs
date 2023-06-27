use super::client;
use super::conf;
use super::message;
use super::method;
use super::sclient;
use std::any::Any;
use std::collections::HashMap;
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
        methods: Arc<HashMap<String, method::Method>>,
        env: Box<dyn ApplicationEnv>,
    ) -> Result<(), String>;

    fn methods(&self) -> &Arc<HashMap<String, method::Method>>;

    /// Called after absorb_env, but before any work occurs.
    fn worker_start(&mut self) -> Result<(), String>;

    /// Called every time our worker wakes up to check for signals,
    /// timeouts, etc.
    ///
    /// This method is only called when no other actions occur as
    /// a result of waking up.  It's not called if there is a
    /// shutdown signal, keepliave timeout, API request, etc.
    ///
    /// * `connected` - True if we are in the middle of a stateful conversation.
    fn worker_idle_wake(&mut self, connected: bool) -> Result<(), String>;

    /// Called after all work is done and the thread is going away.
    ///
    /// Offers a chance to clean up any resources.
    fn worker_end(&mut self) -> Result<(), String>;

    /// Called for stateful sessions on CONNECT and for each request
    /// in a stateless session.
    fn start_session(&mut self) -> Result<(), String>;

    /// Called for stateful sessions on DISCONNECT or keepliave timeout,
    /// andcalled for stateless sessions (one-offs) after the single
    /// request has completed.
    fn end_session(&mut self) -> Result<(), String>;

    /// Called if the client sent a CONNECT but never sent a DISCONNECT
    /// within the configured timeout.
    fn keepalive_timeout(&mut self) -> Result<(), String>;

    fn api_call_error(&mut self, request: &message::Method, error: &str);
}

pub trait Application {
    /// Application service name, e.g. opensrf.settings
    fn name(&self) -> &str;

    /// Called when a service first starts, just after connecting to OpenSRF.
    fn init(
        &mut self,
        client: client::Client,
        config: Arc<conf::Config>,
        host_settings: Arc<sclient::HostSettings>,
    ) -> Result<(), String>;

    /// Tell the server what methods this application implements.
    ///
    /// Called after self.init(), but before workers are spawned.
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
