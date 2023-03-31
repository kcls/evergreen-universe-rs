use opensrf::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use opensrf::client::Client;
use opensrf::conf;
use opensrf::client;
use opensrf::message;
use opensrf::method;
use opensrf::method::ParamCount;
use opensrf::sclient::HostSettings;
use opensrf::server::Server;
use opensrf::session::ServerSession;
use std::any::Any;
use std::sync::Arc;
use evergreen as eg;
use eg::editor::Editor;
use eg::idl;

const APPNAME: &str = "open-ils.rspub";

/// Clone is needed here to support our implementation of downcast();
#[derive(Debug, Clone)]
struct RsPubEnv {
    /// Global / shared IDL ref
    idl: Arc<idl::Parser>,
}

impl RsPubEnv {
    pub fn new(idl: &Arc<idl::Parser>) -> Self {
        RsPubEnv {
            idl: idl.clone(),
        }
    }

    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }
}

impl ApplicationEnv for RsPubEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsPubApplication {
    /// We load the IDL during service init.
    idl: Option<Arc<idl::Parser>>,
}

impl RsPubApplication {
    pub fn new() -> Self {
        RsPubApplication {
            idl: None,
        }
    }

    /// Panics if the IDL is not yet set.
    fn idl(&self) -> &Arc<idl::Parser> {
        self.idl.as_ref().unwrap()
    }
}

impl Application for RsPubApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsPubEnv::new(self.idl()))
    }

    /// Load the IDL
    fn init(
        &mut self,
        _client: client::Client,
        _config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
    ) -> Result<(), String> {
        let idl_file = host_settings.value("IDL")
            .as_str().ok_or(format!("No IDL path!"))?;

        let idl = idl::Parser::parse_file(&idl_file)
            .or_else(|e| Err(format!("Cannot parse IDL file: {e}")))?;

        self.idl = Some(idl);

        Ok(())
    }

    fn register_methods(
        &self,
        _client: Client,
        _config: Arc<conf::Config>,
        _host_settings: Arc<HostSettings>,
    ) -> Result<Vec<method::Method>, String> {
        let namer = |n| format!("{APPNAME}.{n}");

        Ok(vec![
            method::Method::new(&namer("get_barcodes"), ParamCount::Exactly(4), get_barcodes),
        ])
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(RsPubWorker::new())
    }
}

/// Per-thread worker instance.
struct RsPubWorker {
    env: Option<RsPubEnv>,
    client: Option<Client>,
    config: Option<Arc<conf::Config>>,
    host_settings: Option<Arc<HostSettings>>,
}

impl RsPubWorker {
    pub fn new() -> Self {
        RsPubWorker {
            env: None,
            client: None,
            config: None,
            host_settings: None,
        }
    }

    /// This will only ever be called after absorb_env(), so we are
    /// guarenteed to have an env.
    pub fn env(&self) -> &RsPubEnv {
        self.env.as_ref().unwrap()
    }

    /// Cast a generic ApplicationWorker into our RsPubWorker.
    ///
    /// This is necessary to access methods/fields on our RsPubWorker that
    /// are not part of the ApplicationWorker trait.
    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsPubWorker, String> {
        match w.as_any_mut().downcast_mut::<RsPubWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }

    /// Ref to our OpenSRF client.
    ///
    /// Set during absorb_env()
    fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Mutable ref to our OpenSRF client.
    ///
    /// Set during absorb_env()
    fn _client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }

    /// Handy method for extracting an authtoken from a set of params.
    ///
    /// Assumes the authtoken is the first parameter.
    fn authtoken(&self, method: &message::Method) -> Result<String, String> {
        if let Some(v) = method.params().get(0) {
            if let Some(token) = v.as_str() {
                return Ok(token.to_string());
            }
        }
        Err(format!("Could not unpack authtoken from params: {:?}", method.params()))
    }
}

impl ApplicationWorker for RsPubWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Absorb our thread-global data.
    ///
    /// Panics if we cannot downcast the env provided to the expected type.
    fn absorb_env(
        &mut self,
        client: Client,
        config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
        env: Box<dyn ApplicationEnv>,
    ) -> Result<(), String> {

        let worker_env = env.as_any().downcast_ref::<RsPubEnv>()
            .ok_or(format!("Unexpected environment type in absorb_env()"))?;

        // Each worker gets its own client, so we have to tell our
        // client how to pack/unpack network data.
        client.set_serializer(idl::Parser::as_serializer(worker_env.idl()));

        self.env = Some(worker_env.clone());
        self.client = Some(client);
        self.config = Some(config);
        self.host_settings = Some(host_settings);

        Ok(())
    }

    /// Called before the worker goes into its listen state.
    fn worker_start(&mut self) -> Result<(), String> {
        log::debug!("Thread starting");
        Ok(())
    }

    /// Called after all requets are handled and the worker is
    /// about to go away.
    fn worker_end(&mut self) -> Result<(), String> {
        log::debug!("Thread ending");
        Ok(())
    }
}

fn main() {
    if let Err(e) = Server::start(Box::new(RsPubApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}

fn get_barcodes(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = RsPubWorker::downcast(worker)?;

    let authtoken = worker.authtoken(&method)?;

    let org_id = eg::util::json_int(method.params().get(1).unwrap())?;

    let context = method.params().get(2).unwrap().as_str()
        .ok_or(format!("Context parameter must be a string"))?;

    let barcode = method.params().get(3).unwrap().as_str()
        .ok_or(format!("Barcode parameter must be a string"))?;

    let mut editor =
        Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.last_event().unwrap().to_json_value());
    }

    if !editor.allowed("STAFF_LOGIN", Some(org_id))? {
        return session.respond(editor.last_event().unwrap().to_json_value());
    }

    let query = json::object! {
        from: [
            "evergreen.get_barcodes",
            org_id, context, barcode
        ]
    };

    let result = editor.json_query(query)?;

    if context.ne("actor") {
        return session.respond(result);
    }

    let requestor_id = editor.requestor_id();
    let mut response: Vec<json::JsonValue> = Vec::new();

    for user_row in result {
        let user_id = eg::util::json_int(&user_row["id"])?;

        if user_id == requestor_id {
            // We're allowed to know about ourselves.
            response.push(user_row);
            continue;
        }

        // If the found user account is not "me", verify we
        // have permission to view said account.
        let u = editor.retrieve("au", user_id)?.unwrap();
        let home_ou = eg::util::json_int(&u["home_ou"])?;

        if editor.allowed("VIEW_USER", Some(home_ou))? {
            response.push(user_row);
        } else {
            response.push(editor.last_event().unwrap().into());
        }
    }

    session.respond(response)
}

