use eg::osrf::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::message;
use eg::osrf::method::MethodDef;
use eg::osrf::sclient::HostSettings;
use eg::auth;
use eg::date;
use eg::Client;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-auth-internal";

/// Environment shared by all service workers.
///
/// The environment is only mutable up until the point our
/// Server starts spawning threads.
#[derive(Debug, Clone)]
pub struct RsAuthInternalEnv {
}

impl RsAuthInternalEnv {
    pub fn new() -> Self {
        RsAuthInternalEnv {}
    }
}

/// Implement the needed Env trait
impl ApplicationEnv for RsAuthInternalEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Our main application class.
pub struct RsAuthInternalApplication {
    login_durations: HashMap<auth::AuthLoginType, i64>,
}

impl RsAuthInternalApplication {
    pub fn new() -> Self {
        RsAuthInternalApplication {login_durations: HashMap::new()}
    }
}

impl Application for RsAuthInternalApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsAuthInternalEnv::new())
    }

    /// Load the IDL and perform any other needed global startup work.
    fn init(&mut self, _client: Client, host_settings: Arc<HostSettings>) -> EgResult<()> {
        eg::init::load_idl(Some(&host_settings))?;

        // TODO move settings into app chunk for this service
        let opac_timeout = host_settings
            .value("apps/open-ils.auth_internal/app_settings/default_timeout/opac")
            .as_str()
            .unwrap_or("0s");

        self.login_durations.insert(
            auth::AuthLoginType::Opac,
            date::interval_to_seconds(opac_timeout)?
        );

        Ok(())
    }

    /// Tell the Server what methods we want to publish.
    fn register_methods(
        &self,
        _client: Client,
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
        || Box::new(RsAuthInternalWorker::new())
    }
}

/// Per-thread worker instance.
pub struct RsAuthInternalWorker {
    env: Option<RsAuthInternalEnv>,
    client: Option<Client>,
    host_settings: Option<Arc<HostSettings>>,
    methods: Option<Arc<HashMap<String, MethodDef>>>,
}

impl RsAuthInternalWorker {
    pub fn new() -> Self {
        RsAuthInternalWorker {
            env: None,
            client: None,
            methods: None,
            host_settings: None,
        }
    }

    /// This will only ever be called after absorb_env(), so we are
    /// guarenteed to have an env.
    pub fn env(&self) -> &RsAuthInternalEnv {
        self.env.as_ref().unwrap()
    }

    /// Cast a generic ApplicationWorker into our RsAuthInternalWorker.
    ///
    /// This is necessary to access methods/fields on our RsAuthInternalWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut RsAuthInternalWorker> {
        match w.as_any_mut().downcast_mut::<RsAuthInternalWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast").into()),
        }
    }

    /// Ref to our OpenSRF client.
    ///
    /// Set during absorb_env()
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Mutable ref to our OpenSRF client.
    ///
    /// Set during absorb_env()
    pub fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl ApplicationWorker for RsAuthInternalWorker {
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
        host_settings: Arc<HostSettings>,
        methods: Arc<HashMap<String, MethodDef>>,
        env: Box<dyn ApplicationEnv>,
    ) -> EgResult<()> {
        let worker_env = env
            .as_any()
            .downcast_ref::<RsAuthInternalEnv>()
            .ok_or_else(|| format!("Unexpected environment type in absorb_env()"))?;

        self.env = Some(worker_env.clone());
        self.client = Some(client);
        self.methods = Some(methods);
        self.host_settings = Some(host_settings);

        Ok(())
    }

    /// Called before the worker goes into its listen state.
    fn worker_start(&mut self) -> EgResult<()> {
        log::debug!("Thread starting");
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

    fn api_call_error(&mut self, _request: &message::MethodCall, _error: EgError) {}
}
