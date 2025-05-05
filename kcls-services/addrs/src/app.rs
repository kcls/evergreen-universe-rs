use eg::osrf::app::{Application, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::method::MethodDef;
use eg::Client;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-addrs";

/// Our main application class.
pub struct AddrsApplication {}

impl Default for AddrsApplication {
    fn default() -> Self {
        Self::new()
    }
}

impl AddrsApplication {
    pub fn new() -> Self {
        AddrsApplication {}
    }
}

impl Application for AddrsApplication {
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
            log::debug!("Registering method: {}", def.name());
            methods.push(def.into_method(APPNAME));
        }

        Ok(methods)
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(AddrsWorker::new())
    }
}

/// Per-thread worker instance.
pub struct AddrsWorker {
    client: Option<Client>,
}

impl Default for AddrsWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl AddrsWorker {
    pub fn new() -> Self {
        AddrsWorker { client: None }
    }

    /// Cast a generic ApplicationWorker into our AddrsWorker.
    ///
    /// This is necessary to access methods/fields on our AddrsWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut AddrsWorker> {
        match w.as_any_mut().downcast_mut::<AddrsWorker>() {
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

impl ApplicationWorker for AddrsWorker {
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
