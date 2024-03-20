use eg::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use eg::client::Client;
use eg::conf;
use eg::idl;
use eg::message;
use eg::method::MethodDef;
use eg::sclient::HostSettings;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-hold-targeter";

/// Environment shared by all service workers.
///
/// The environment is only mutable up until the point our
/// Server starts spawning threads.
#[derive(Debug, Clone)]
pub struct HoldTargeterEnv {
    /// Global / shared IDL ref
    idl: Arc<idl::Parser>,
}

impl HoldTargeterEnv {
    pub fn new(idl: &Arc<idl::Parser>) -> Self {
        HoldTargeterEnv { idl: idl.clone() }
    }

    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }
}

/// Implement the needed Env trait
impl ApplicationEnv for HoldTargeterEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Our main application class.
pub struct HoldTargeterApplication {
    /// We load the IDL during service init.
    idl: Option<Arc<idl::Parser>>,
}

impl HoldTargeterApplication {
    pub fn new() -> Self {
        HoldTargeterApplication { idl: None }
    }
}

impl Application for HoldTargeterApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(HoldTargeterEnv::new(self.idl.as_ref().unwrap()))
    }

    /// Load the IDL and perform any other needed global startup work.
    fn init(
        &mut self,
        _client: Client,
        _config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
    ) -> EgResult<()> {
        let idl_file = host_settings
            .value("IDL")
            .as_str()
            .ok_or_else(|| format!("No IDL path!"))?;

        let idl = idl::Parser::parse_file(idl_file)
            .or_else(|e| Err(format!("Cannot parse IDL file: {e}")))?;

        self.idl = Some(idl);

        Ok(())
    }

    /// Tell the Server what methods we want to publish.
    fn register_methods(
        &self,
        _client: Client,
        _config: Arc<conf::Config>,
        _host_settings: Arc<HostSettings>,
    ) -> EgResult<Vec<MethodDef>> {
        let mut methods: Vec<MethodDef> = Vec::new();

        // Create Method objects from our static method definitions.
        for def in methods::METHODS.iter() {
            log::info!("Registering method: {}", def.name());
            methods.push(def.into_method(APPNAME));
        }

        Ok(methods)
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(HoldTargeterWorker::new())
    }
}

/// Per-thread worker instance.
pub struct HoldTargeterWorker {
    env: Option<HoldTargeterEnv>,
    client: Option<Client>,
    config: Option<Arc<conf::Config>>,
    host_settings: Option<Arc<HostSettings>>,
    methods: Option<Arc<HashMap<String, MethodDef>>>,
}

impl HoldTargeterWorker {
    pub fn new() -> Self {
        HoldTargeterWorker {
            env: None,
            client: None,
            config: None,
            methods: None,
            host_settings: None,
        }
    }

    /// This will only ever be called after absorb_env(), so we are
    /// guarenteed to have an env.
    pub fn env(&self) -> &HoldTargeterEnv {
        self.env.as_ref().unwrap()
    }

    /// Ref to our OpenSRF client.
    ///
    /// Set during absorb_env()
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Cast a generic ApplicationWorker into our HoldTargeterWorker.
    ///
    /// This is necessary to access methods/fields on our HoldTargeterWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut HoldTargeterWorker> {
        match w.as_any_mut().downcast_mut::<HoldTargeterWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast").into()),
        }
    }
}

impl ApplicationWorker for HoldTargeterWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn methods(&self) -> &Arc<HashMap<String, MethodDef>> {
        &self.methods.as_ref().unwrap()
    }

    /// Absorb our global dataset.
    ///
    /// Panics if we cannot downcast the env provided to the expected type.
    fn absorb_env(
        &mut self,
        client: Client,
        config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
        methods: Arc<HashMap<String, MethodDef>>,
        env: Box<dyn ApplicationEnv>,
    ) -> EgResult<()> {
        let worker_env = env
            .as_any()
            .downcast_ref::<HoldTargeterEnv>()
            .ok_or_else(|| format!("Unexpected environment type in absorb_env()"))?;

        self.env = Some(worker_env.clone());
        self.client = Some(client);
        self.config = Some(config);
        self.methods = Some(methods);
        self.host_settings = Some(host_settings);

        Ok(())
    }

    /// Called after this worker thread is spawned, but before the worker
    /// goes into its listen state.
    fn worker_start(&mut self) -> EgResult<()> {
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

    fn api_call_error(&mut self, _request: &message::MethodCall, error: EgError) {
        log::debug!("API failed: {error}");
    }
}
