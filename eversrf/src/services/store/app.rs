use eg::db::{DatabaseConnection, DatabaseConnectionBuilder};
use eg::idl;
use eversrf as eg;
use eg::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use eg::client::Client;
use eg::conf;
use eg::message;
use eg::method::MethodDef;
use eg::sclient::HostSettings;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

// Import our local methods module.
use eg::methods;

const APPNAME: &str = "open-ils.rs-store";

/// If this worker instance has performed no tasks in this amount of
/// time, disconnect our database connection and free up resources.
/// Will reconnect when needed.  A value of 0 disables the feature.
/// TODO make this configurable.
const IDLE_DISCONNECT_TIME: i32 = 300;

const DIRECT_METHODS: &[&str] = &["create", "retrieve", "search", "update", "delete"];

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

    /// Register CRUD (and search) methods for classes we control.
    fn register_auto_methods(&self, methods: &mut Vec<MethodDef>) {
        let classes = self.idl.as_ref().unwrap().classes().values();

        // Filter function to find classes with the wanted controllers.
        // Find classes controlled by our service and (for now) cstore.
        let cfilter = |c: &&Arc<idl::Class>| {
            if let Some(ctrl) = c.controller() {
                ctrl.contains("open-ils.cstore") || ctrl.contains("open-ils.rs-store")
            } else {
                false
            }
        };

        for fieldmapper in classes
            .filter(|c| !c.is_virtual())
            .filter(cfilter)
            .filter(|c| c.fieldmapper().is_some())
            .map(|c| c.fieldmapper().unwrap())
            .map(|fm| fm.replace("::", "."))
        {
            for mtype in DIRECT_METHODS {
                // Each direct method type has a stub method defined
                // in our list of StaticMethodDef's.  Use the stub as the
                // basis for each auto-method.  The stubs themselves are
                // not registered.
                let stub = methods::METHODS
                    .iter()
                    .filter(|m| m.name.eq(&format!("{mtype}-stub")))
                    .next()
                    .unwrap(); // these are hard-coded to exist.

                let mut clone = stub.into_method(APPNAME);
                let apiname = format!("{APPNAME}.direct.{fieldmapper}.{mtype}");

                log::trace!("Registering: {apiname}");

                clone.set_name(&apiname);
                methods.push(clone);
            }
        }

        log::info!("{APPNAME} registered {} auto methods", methods.len());
    }

    fn register_xact_methods(&self, methods: &mut Vec<MethodDef>) {
        let api = "transaction.begin";
        let begin = methods::METHODS
            .iter()
            .filter(|m| m.name.eq(api))
            .next()
            .unwrap();

        methods.push(begin.into_method(APPNAME));

        let api = "transaction.rollback";
        let rollback = methods::METHODS
            .iter()
            .filter(|m| m.name.eq(api))
            .next()
            .unwrap();

        methods.push(rollback.into_method(APPNAME));

        let api = "transaction.commit";
        let commit = methods::METHODS
            .iter()
            .filter(|m| m.name.eq(api))
            .next()
            .unwrap();

        methods.push(commit.into_method(APPNAME));
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
    ) -> Result<Vec<MethodDef>, String> {
        let mut methods: Vec<MethodDef> = Vec::new();

        self.register_auto_methods(&mut methods);
        self.register_xact_methods(&mut methods);

        let json_query = methods::METHODS
            .iter()
            .filter(|m| m.name.eq("json_query"))
            .next()
            .unwrap();

        methods.push(json_query.into_method(APPNAME));

        log::info!("{APPNAME} registered {} total methods", methods.len());

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
    methods: Option<Arc<HashMap<String, MethodDef>>>,
    database: Option<Rc<RefCell<DatabaseConnection>>>,
    last_work_timer: Option<eg::util::Timer>,
}

impl RsStoreWorker {
    pub fn new() -> Self {
        let mut timer = None;
        if IDLE_DISCONNECT_TIME > 0 {
            timer = Some(eg::util::Timer::new(IDLE_DISCONNECT_TIME));
        }

        RsStoreWorker {
            env: None,
            client: None,
            config: None,
            methods: None,
            host_settings: None,
            database: None,
            last_work_timer: timer,
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

    /// Get a reference to our database connection.
    ///
    /// Panics if we have no connection.
    pub fn database(&mut self) -> &Rc<RefCell<DatabaseConnection>> {
        self.database
            .as_ref()
            .expect("We have no database connection!")
    }

    pub fn setup_database(&mut self) -> Result<(), String> {
        // Our builder will apply default values where none exist in
        // settings or environment variables.
        let mut builder = DatabaseConnectionBuilder::new();

        let path = format!("apps/{APPNAME}/app_settings/database");
        let settings = self.host_settings.as_ref().unwrap().value(&path);

        if let Some(user) = settings["user"].as_str() {
            builder.set_user(user);
        }

        if let Some(host) = settings["host"].as_str() {
            builder.set_host(host);
        }

        if let Some(port) = settings["port"].as_u16() {
            builder.set_port(port);
        }

        // Support short and long forms of database name and password.
        if let Some(db) = settings["db"].as_str() {
            builder.set_database(db);
        } else if let Some(db) = settings["database"].as_str() {
            builder.set_database(db);
        }

        if let Some(db) = settings["pw"].as_str() {
            builder.set_password(db);
        } else if let Some(db) = settings["password"].as_str() {
            builder.set_password(db);
        }

        // Build the application name with host and thread ID info.
        builder.set_application(&format!(
            "{APPNAME}@{}(thread_{})",
            self.config.as_ref().unwrap().hostname(),
            eg::util::thread_id()
        ));

        log::debug!("{APPNAME} connecting to database");

        let mut db = builder.build();
        db.connect()?;
        self.database = Some(db.into_shared());

        Ok(())
    }
}

impl ApplicationWorker for RsStoreWorker {
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
    ) -> Result<(), String> {
        let worker_env = env
            .as_any()
            .downcast_ref::<RsStoreEnv>()
            .ok_or_else(|| format!("Unexpected environment type in absorb_env()"))?;

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
        self.setup_database()
    }

    fn worker_idle_wake(&mut self, connected: bool) -> Result<(), String> {
        if connected {
            // Avoid any idle database maintenance when we're mid-session.
            return Ok(());
        }

        if let Some(ref t) = self.last_work_timer {
            if t.done() {
                if let Some(db) = self.database.take() {
                    log::debug!("Disconnecting DB on idle timeout");

                    // drop()'ing the database will result in a disconnect.
                    // drop() is not strictly necessary here, since the
                    // variable goes out of scope, but it's nice for clarity.
                    drop(db);
                }
            }
        }

        Ok(())
    }

    /// Called after all requests are handled and the worker is
    /// shutting down.
    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("Thread ending");
        // Our database connection will clean itself up on Drop.
        Ok(())
    }

    fn keepalive_timeout(&mut self) -> Result<(), String> {
        log::debug!("IDL worker timed out in keepalive");
        self.end_session()
    }

    fn start_session(&mut self) -> Result<(), String> {
        if let Some(ref mut t) = self.last_work_timer {
            t.reset();
        }
        if self.database.is_none() {
            return self.setup_database();
        }

        Ok(())
    }

    fn end_session(&mut self) -> Result<(), String> {
        // Alway rollback an active transaction if our client goes away
        // or disconnects prematurely.
        if let Some(ref mut db) = self.database {
            if db.borrow().in_transaction() {
                log::info!("Rollback back DB transaction on end of session");
                db.borrow_mut().xact_rollback()?;
            }
        }

        // Reset here so long-running sessions are counted as "work".
        if let Some(ref mut t) = self.last_work_timer {
            t.reset();
        }

        Ok(())
    }

    fn api_call_error(&mut self, _request: &message::MethodCall, error: &str) {
        log::debug!("API failed: {error}");
        self.end_session().ok(); // ignore additional errors
    }
}
