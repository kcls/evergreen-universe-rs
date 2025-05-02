use eg::osrf::app::{Application, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::method::MethodDef;
use eg::Client;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-search";

/// Our main application class.
pub struct SearchApplication {}

impl Default for SearchApplication {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchApplication {
    pub fn new() -> Self {
        SearchApplication {}
    }
}

impl Application for SearchApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    /// Load the IDL and perform any other needed global startup work.
    fn init(&mut self, _client: Client) -> EgResult<()> {
        eg::init::load_idl()?;
        Ok(())
    }

    /// Tell the Server what methods we want to publish.
    fn register_methods(&self, _client: Client) -> EgResult<Vec<MethodDef>> {
        let mut methods: Vec<MethodDef> = Vec::new();

        // Create Method objects from our static method definitions.
        for def in methods::METHODS.iter() {
            log::info!("Registering method: {}", def.name());
            methods.push(def.into_method(APPNAME));
        }

        Ok(methods)
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(SearchWorker::new())
    }
}

/// Per-thread worker instance.
pub struct SearchWorker {
    client: Option<Client>,
}

impl Default for SearchWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchWorker {
    pub fn new() -> Self {
        SearchWorker { client: None }
    }

    /// Cast a generic ApplicationWorker into our SearchWorker.
    ///
    /// This is necessary to access methods/fields on our SearchWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut SearchWorker> {
        match w.as_any_mut().downcast_mut::<SearchWorker>() {
            Some(eref) => Ok(eref),
            None => Err("Cannot downcast".to_string().into()),
        }
    }

    /// Ref to our OpenSRF client.
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Mutable ref to our OpenSRF client.
    pub fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl ApplicationWorker for SearchWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn worker_start(&mut self, client: Client) -> EgResult<()> {
        self.client = Some(client);
        Ok(())
    }

    fn worker_idle_wake(&mut self, _connected: bool) -> EgResult<()> {
        Ok(())
    }

    /// Called after all requests are handled and the worker is
    /// shutting down.
    fn worker_end(&mut self) -> EgResult<()> {
        log::debug!("Thread ending");
        Ok(())
    }

    fn start_session(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn end_session(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn keepalive_timeout(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn api_call_error(&mut self, _api_name: &str, _error: EgError) {}
}
