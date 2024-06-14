use eg::osrf::app::{Application, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::method::MethodDef;
use eg::Client;
use eg::{EgError, EgResult};
use evergreen as eg;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-circ";

/// Our main application class.
pub struct RsCircApplication {}

impl Default for RsCircApplication {
    fn default() -> Self {
        Self::new()
    }
}

impl RsCircApplication {
    pub fn new() -> Self {
        RsCircApplication {}
    }
}

impl Application for RsCircApplication {
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
        || Box::new(RsCircWorker::new())
    }
}

/// Per-thread worker instance.
pub struct RsCircWorker {
    client: Option<Client>,
    methods: Option<Arc<HashMap<String, MethodDef>>>,
}

impl Default for RsCircWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl RsCircWorker {
    pub fn new() -> Self {
        RsCircWorker {
            client: None,
            methods: None,
        }
    }

    /// Ref to our OpenSRF client.
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Cast a generic ApplicationWorker into our RsCircWorker.
    ///
    /// This is necessary to access methods/fields on our RsCircWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut RsCircWorker> {
        match w.as_any_mut().downcast_mut::<RsCircWorker>() {
            Some(eref) => Ok(eref),
            None => Err("Cannot downcast".to_string().into()),
        }
    }
}

impl ApplicationWorker for RsCircWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn methods(&self) -> &Arc<HashMap<String, MethodDef>> {
        self.methods.as_ref().unwrap()
    }

    /// Absorb our global dataset.
    fn worker_start(
        &mut self,
        client: Client,
        methods: Arc<HashMap<String, MethodDef>>,
    ) -> EgResult<()> {
        self.client = Some(client);
        self.methods = Some(methods);
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

    fn keepalive_timeout(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn start_session(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn end_session(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn api_call_error(&mut self, _api_name: &str, _error: EgError) {}
}
