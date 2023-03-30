use opensrf::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use opensrf::client::Client;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::method::ParamCount;
use opensrf::sclient::HostSettings;
use opensrf::server::Server;
use opensrf::session::ServerSession;
use std::any::Any;
use std::sync::Arc;
use evergreen::editor::Editor;
use evergreen::idl;

const APPNAME: &str = "open-ils.rspub";

/// Clone is needed here to support our implementation of downcast();
#[derive(Debug, Clone)]
struct RsPubEnv {
    idl: Option<Arc<idl::Parser>>,
}

impl RsPubEnv {
    pub fn new() -> Self {
        RsPubEnv {
            idl: None,
        }
    }

    pub fn idl(&self) -> &Arc<idl::Parser> {
        self.idl.as_ref().unwrap()
    }
}

impl ApplicationEnv for RsPubEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsPubApplication;

impl RsPubApplication {
    pub fn new() -> Self {
        RsPubApplication {}
    }
}

impl Application for RsPubApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsPubEnv::new())
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

    /// We must have a value here since absorb_env() is invoked on the worker.
    pub fn env(&self) -> &RsPubEnv {
        self.env.as_ref().unwrap()
    }

    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsPubWorker, String> {
        match w.as_any_mut().downcast_mut::<RsPubWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }

    ///
    /// self.client is guaranteed to set after absorb_env()
    fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl ApplicationWorker for RsPubWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

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

        let mut worker_env = worker_env.clone();

        let idl_file = host_settings.value("IDL")
            .as_str().ok_or(format!("No IDL path!"))?;

        let idl = idl::Parser::parse_file(&idl_file)
            .or_else(|e| Err(format!("Cannot parse IDL file: {e}")))?;

        client.set_serializer(idl::Parser::as_serializer(&idl));

        worker_env.idl = Some(idl);
        self.env = Some(worker_env);

        self.client = Some(client);
        self.config = Some(config);
        self.host_settings = Some(host_settings);

        Ok(())
    }

    fn worker_start(&mut self) -> Result<(), String> {
        log::debug!("Thread starting");
        Ok(())
    }

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
    let mut worker = RsPubWorker::downcast(worker)?;

    let authtoken = method.params()[0].as_str().ok_or(format!("Invalid authtoken"))?;
    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), authtoken);

    let org_id = &method.params()[1];
    let context = &method.params()[2];
    let barcode = &method.params()[3];

    session.respond(barcode.clone())?;
    session.respond(editor.retrieve("aou", 1)?)?;

    Ok(())
}
