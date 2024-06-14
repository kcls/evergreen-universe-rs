use eg::db::{DatabaseConnection, DatabaseConnectionBuilder};
use eg::idl;
use eg::osrf::app::{Application, ApplicationWorker, ApplicationWorkerFactory};
use eg::osrf::conf;
use eg::osrf::method::MethodDef;
use eg::osrf::sclient::HostSettings;
use eg::Client;
use eg::EgError;
use eg::EgResult;
use evergreen as eg;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

// Import our local methods module.
use crate::methods;

const APPNAME: &str = "open-ils.rs-store";

/// If a worker instance has not communicted with the database in this amount of
/// time, it will disconnect from the database to free up database resources.
/// The worker will reconnect when needed.  A value of 0 disables the feature.
///
/// Setting to zero to disable for now since this is experimental and
/// maybe wholly unnecessary.
///
/// TODO make this configurable.
const IDLE_DISCONNECT_TIME: i32 = 0;

const DIRECT_METHODS: &[&str] = &["create", "retrieve", "search", "update", "delete"];

/// Our main application class.
pub struct RsStoreApplication {}

impl Default for RsStoreApplication {
    fn default() -> Self {
        Self::new()
    }
}

impl RsStoreApplication {
    pub fn new() -> Self {
        RsStoreApplication {}
    }

    /// Register CRUD (and search) methods for classes we control.
    fn register_auto_methods(&self, methods: &mut Vec<MethodDef>) {
        let classes = idl::parser().classes().values();

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
                    .iter().find(|m| m.name.eq(&format!("{mtype}-stub")))
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
            .iter().find(|m| m.name.eq(api))
            .unwrap();

        methods.push(begin.into_method(APPNAME));

        let api = "transaction.rollback";
        let rollback = methods::METHODS
            .iter().find(|m| m.name.eq(api))
            .unwrap();

        methods.push(rollback.into_method(APPNAME));

        let api = "transaction.commit";
        let commit = methods::METHODS
            .iter().find(|m| m.name.eq(api))
            .unwrap();

        methods.push(commit.into_method(APPNAME));
    }
}

impl Application for RsStoreApplication {
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

        self.register_auto_methods(&mut methods);
        self.register_xact_methods(&mut methods);

        let json_query = methods::METHODS
            .iter().find(|m| m.name.eq("json_query"))
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
    client: Option<Client>,
    methods: Option<Arc<HashMap<String, MethodDef>>>,
    database: Option<Rc<RefCell<DatabaseConnection>>>,
    last_work_timer: Option<eg::util::Timer>,
}

impl Default for RsStoreWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl RsStoreWorker {
    pub fn new() -> Self {
        let mut timer = None;
        if IDLE_DISCONNECT_TIME > 0 {
            timer = Some(eg::util::Timer::new(IDLE_DISCONNECT_TIME));
        }

        RsStoreWorker {
            client: None,
            methods: None,
            database: None,
            last_work_timer: timer,
        }
    }

    /// Cast a generic ApplicationWorker into our RsStoreWorker.
    ///
    /// This is necessary to access methods/fields on our RsStoreWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> EgResult<&mut RsStoreWorker> {
        match w.as_any_mut().downcast_mut::<RsStoreWorker>() {
            Some(eref) => Ok(eref),
            None => Err("Cannot downcast".to_string().into()),
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

    pub fn setup_database(&mut self) -> EgResult<()> {
        // Our builder will apply default values where none exist in
        // settings or environment variables.
        let mut builder = DatabaseConnectionBuilder::new();

        let path = format!("apps/{APPNAME}/app_settings/database");
        let settings = HostSettings::get(&path)?;

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
            conf::config().hostname(),
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
        self.methods.as_ref().unwrap()
    }

    fn worker_start(
        &mut self,
        client: Client,
        methods: Arc<HashMap<String, MethodDef>>,
    ) -> EgResult<()> {
        self.client = Some(client);
        self.methods = Some(methods);
        self.setup_database()
    }

    fn worker_idle_wake(&mut self, connected: bool) -> EgResult<()> {
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
    fn worker_end(&mut self) -> EgResult<()> {
        log::debug!("Thread ending");
        // Our database connection will clean itself up on Drop.
        Ok(())
    }

    fn keepalive_timeout(&mut self) -> EgResult<()> {
        log::debug!("Idle worker timed out in keepalive");
        self.end_session()
    }

    fn start_session(&mut self) -> EgResult<()> {
        if let Some(ref mut t) = self.last_work_timer {
            t.reset();
        }
        if self.database.is_none() {
            return self.setup_database();
        }

        Ok(())
    }

    fn end_session(&mut self) -> EgResult<()> {
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

    fn api_call_error(&mut self, _api_name: &str, error: EgError) {
        log::debug!("API failed: {error}");
        self.end_session().ok(); // ignore additional errors
    }
}
