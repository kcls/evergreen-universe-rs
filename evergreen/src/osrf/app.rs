use crate::osrf::client;
use crate::osrf::method;
use crate::EgError;
use crate::EgResult;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// * Server spawns a worker thread
/// * Worker thread calls an ApplicationWorkerFactory function to
///   generate an ApplicationWorker.
/// * app_worker.worker_start() is called allowing the worker to
///   perform any other startup routines.
/// * Worker waits for inbound method calls.
/// * Inbound method call arrives
/// * app_worker.start_session() is called on CONNECT or any stateless request.
/// * Called method is looked up in the app_worker's methods().
/// * method handler function is called to handle the request.
/// * If a DISCONNECT is received OR its a stateless API call,
///   worker.end_session() is called after the API call completes.
/// * Once all requests are complete in the current session,
///   the Worker goes back to sleep to wait for more requests.
/// * Just before the thread ends/joins, app_worker.worker_end() is called.

/// Function that generates ApplicationWorker implementers.
///
/// This type of function may be cloned and passed through the thread
/// boundary, but the ApplicationWorker's it generates are not
/// guaranteed to be thread-Send-able, hence the factory approach.
pub type ApplicationWorkerFactory = fn() -> Box<dyn ApplicationWorker>;

pub trait ApplicationWorker: Any {
    /// Required for downcasting into the local ApplicationWorker implementation type.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// All of our registered method definitions, keyed on API name.
    fn methods(&self) -> &Arc<HashMap<String, method::MethodDef>>;

    /// Called just after a new worker is spawned.
    fn worker_start(
        &mut self,
        client: client::Client,
        methods: Arc<HashMap<String, method::MethodDef>>,
    ) -> EgResult<()>;

    /// Called for stateful sessions on CONNECT and for each request
    /// in a stateless session.
    fn start_session(&mut self) -> EgResult<()>;

    /// Called for stateful sessions on DISCONNECT or keepliave timeout --
    /// Also called for stateless sessions (one-offs) after the request
    /// completes.
    fn end_session(&mut self) -> EgResult<()>;

    /// Called if the client sent a CONNECT but failed to send a DISCONNECT
    /// before the keepliave timeout expired.
    fn keepalive_timeout(&mut self) -> EgResult<()>;

    /// Called on the worker when a MethodCall invocation exits with an Err.
    fn api_call_error(&mut self, api_name: &str, error: EgError);

    /// Called every time our worker wakes up to check for signals,
    /// timeouts, etc.
    ///
    /// This method is only called when no other actions occur as a
    /// result of a worker thread waking up.  It's not called if there
    /// is a shutdown signal, keepliave timeout, API request, etc.
    ///
    /// * `connected` - True if we are in the middle of a stateful conversation.
    fn worker_idle_wake(&mut self, connected: bool) -> EgResult<()>;

    /// Called after all work is done and the thread is going away.
    ///
    /// Offers a chance to clean up any resources.
    fn worker_end(&mut self) -> EgResult<()>;
}

pub trait Application {
    /// Application service name, e.g. opensrf.settings
    fn name(&self) -> &str;

    /// Called when a service first starts, just after connecting to OpenSRF.
    fn init(&mut self, client: client::Client) -> EgResult<()>;

    /// Tell the server what methods this application implements.
    ///
    /// Called after self.init(), but before workers are spawned.
    fn register_methods(&self, client: client::Client) -> EgResult<Vec<method::MethodDef>>;

    /// Returns a function pointer (ApplicationWorkerFactory) that returns
    /// new ApplicationWorker's when called.
    ///
    /// Dynamic trait objects cannot be passed to threads, but functions
    /// that generate them can.
    fn worker_factory(&self) -> fn() -> Box<dyn ApplicationWorker>;
}
