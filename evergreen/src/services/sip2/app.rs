use eg::osrf::app::{Application, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::cache::Cache;
use eg::osrf::method::MethodDef;
use eg::Client;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-sip2";

/// Our main application class.
pub struct Sip2Application {}

impl Default for Sip2Application {
    fn default() -> Self {
        Self::new()
    }
}

impl Sip2Application {
    pub fn new() -> Self {
        Sip2Application {}
    }
}

impl Application for Sip2Application {
    fn name(&self) -> &str {
        APPNAME
    }

    /// Load the IDL and perform any other needed global startup work.
    fn init(&mut self, _client: Client) -> EgResult<()> {
        eg::init::load_idl()?;
        Ok(())
    }

    fn register_methods(&self, _client: Client) -> EgResult<Vec<MethodDef>> {
        let mut methods: Vec<MethodDef> = Vec::new();

        // Create Method objects from our static method definitions.
        for def in methods::METHODS.iter() {
            methods.push(def.into_method(APPNAME));
        }

        Ok(methods)
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(Sip2Worker::new())
    }
}

/// Per-thread worker instance.
pub struct Sip2Worker {
    client: Option<Client>,
    methods: Option<Arc<HashMap<String, MethodDef>>>,
}

impl Default for Sip2Worker {
    fn default() -> Self {
        Self::new()
    }
}

impl Sip2Worker {
    pub fn new() -> Self {
        Sip2Worker {
            client: None,
            methods: None,
        }
    }

    /// Casts a generic ApplicationWorker into our Sip2Worker.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut Sip2Worker> {
        match w.as_any_mut().downcast_mut::<Sip2Worker>() {
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

impl ApplicationWorker for Sip2Worker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn methods(&self) -> &Arc<HashMap<String, MethodDef>> {
        self.methods.as_ref().unwrap()
    }

    fn worker_start(
        &mut self,
        client: Client,
        methods: Arc<HashMap<String, MethodDef>>,
    ) -> EgResult<()> {
        Cache::init_cache("global")?;
        self.client = Some(client);
        self.methods = Some(methods);
        Ok(())
    }

    fn worker_idle_wake(&mut self, _connected: bool) -> EgResult<()> {
        Ok(())
    }

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
