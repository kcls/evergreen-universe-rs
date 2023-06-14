use eg::db::{DatabaseConnection, DatabaseConnectionBuilder};
use eg::idl;
use evergreen as eg;
use opensrf::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use opensrf::client::Client;
use opensrf::conf;
use opensrf::method::Method;
use opensrf::sclient::HostSettings;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-store";

/// Environment shared by all service workers.
///
/// The environment is only mutable up until the point our
/// Server starts spawning threads.
#[derive(Debug, Clone)]
pub struct RsStoreEnv {
    /// Global / shared IDL ref
    idl: Arc<idl::Parser>,
}

impl RsStoreEnv {
    pub fn new(idl: &Arc<idl::Parser>) -> Self {
        RsStoreEnv { idl: idl.clone() }
    }

    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }
}

/// Implement the needed Env trait
impl ApplicationEnv for RsStoreEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Our main application class.
pub struct RsStoreApplication {
    /// We load the IDL during service init.
    idl: Option<Arc<idl::Parser>>,
}

impl RsStoreApplication {
    pub fn new() -> Self {
        RsStoreApplication { idl: None }
    }

    fn register_retrieve_methods(&self, methods: &mut Vec<Method>) {
        let stub = methods::METHODS
            .iter()
            .filter(|m| m.name.eq("retrieve-stub"))
            .next()
            .unwrap();

        for idl_class in self.idl.as_ref().unwrap().classes().values() {
            if let Some(ctrl) = idl_class.controller() {
                // For now, publish all of cstore's classes
                if ctrl.contains("open-ils.cstore") || ctrl.contains("open-ils.rs-store") {
                    if let Some(fm) = idl_class.fieldmapper() {
                        let mut clone = stub.into_method(APPNAME);
                        let fieldmapper = fm.replace("::", ".");
                        let apiname = format!("{APPNAME}.direct.{fieldmapper}.retrieve");
                        log::trace!("REGISTERING: {apiname}");
                        clone.set_name(&apiname);
                        methods.push(clone);
                    }
                }
            }
        }
    }
}

impl Application for RsStoreApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsStoreEnv::new(self.idl.as_ref().unwrap()))
    }

    /// Load the IDL and perform any other needed global startup work.
    fn init(
        &mut self,
        _client: Client,
        _config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
    ) -> Result<(), String> {
        let idl_file = host_settings
            .value("IDL")
            .as_str()
            .ok_or(format!("No IDL path!"))?;

        let idl = idl::Parser::parse_file(&idl_file)
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
    ) -> Result<Vec<Method>, String> {
        let mut methods: Vec<Method> = Vec::new();

        self.register_retrieve_methods(&mut methods);

        Ok(methods)
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(RsStoreWorker::new())
    }
}

/// Per-thread worker instance.
pub struct RsStoreWorker {
    env: Option<RsStoreEnv>,
    client: Option<Client>,
    config: Option<Arc<conf::Config>>,
    host_settings: Option<Arc<HostSettings>>,
    methods: Option<Arc<HashMap<String, Method>>>,
    database: Option<Rc<RefCell<DatabaseConnection>>>,
}

impl RsStoreWorker {
    pub fn new() -> Self {
        RsStoreWorker {
            env: None,
            client: None,
            config: None,
            methods: None,
            host_settings: None,
            database: None,
        }
    }

    /// This will only ever be called after absorb_env(), so we are
    /// guarenteed to have an env.
    pub fn env(&self) -> &RsStoreEnv {
        self.env.as_ref().unwrap()
    }

    /// Cast a generic ApplicationWorker into our RsStoreWorker.
    ///
    /// This is necessary to access methods/fields on our RsStoreWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsStoreWorker, String> {
        match w.as_any_mut().downcast_mut::<RsStoreWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
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

    /// Get a reference to our database connection.
    ///
    /// Panics if we have no connection.
    pub fn database(&mut self) -> &Rc<RefCell<DatabaseConnection>> {
        self.database
            .as_ref()
            .expect("We have no database connection!")
    }
}

impl ApplicationWorker for RsStoreWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn methods(&self) -> &Arc<HashMap<String, Method>> {
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
        methods: Arc<HashMap<String, Method>>,
        env: Box<dyn ApplicationEnv>,
    ) -> Result<(), String> {
        let worker_env = env
            .as_any()
            .downcast_ref::<RsStoreEnv>()
            .ok_or(format!("Unexpected environment type in absorb_env()"))?;

        // Each worker gets its own client, so we have to tell our
        // client how to pack/unpack network data.
        client.set_serializer(idl::Parser::as_serializer(worker_env.idl()));

        self.env = Some(worker_env.clone());
        self.client = Some(client);
        self.config = Some(config);
        self.methods = Some(methods);
        self.host_settings = Some(host_settings);

        Ok(())
    }

    /// Called after this worker thread is spawned, but before the worker
    /// goes into its listen state.
    fn worker_start(&mut self) -> Result<(), String> {
        log::debug!("Thread starting");

        // TODO pull DB settings from host settings
        // TODO add hostname and thread ID to application name.
        let mut builder = DatabaseConnectionBuilder::new();
        builder.set_application(APPNAME);

        log::info!("{APPNAME} connecting to database");
        let mut db = builder.build();
        db.connect()?;
        self.database = Some(db.into_shared());
        Ok(())
    }

    /// Called after all requests are handled and the worker is
    /// shutting down.
    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("Thread ending");
        // Our database connection will clean itself up on Drop.
        Ok(())
    }
}
