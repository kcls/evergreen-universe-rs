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

const APPNAME: &str = "opensrf.rs-public";

/// Clone is needed here to support our implementation of downcast();
#[derive(Debug, Clone)]
struct RsPublicEnv;

impl RsPublicEnv {
    pub fn new() -> Self {
        RsPublicEnv {}
    }
}

impl ApplicationEnv for RsPublicEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsPublicApplication;

impl RsPublicApplication {
    pub fn new() -> Self {
        RsPublicApplication {}
    }
}

impl Application for RsPublicApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsPublicEnv::new())
    }

    fn register_methods(
        &self,
        _client: Client,
        _config: Arc<conf::Config>,
        _host_settings: Arc<HostSettings>,
    ) -> Result<Vec<method::Method>, String> {
        let namer = |n| format!("{APPNAME}.{n}");

        Ok(vec![
            method::Method::new(&namer("time"), ParamCount::Zero, relay),
            method::Method::new(&namer("counter"), ParamCount::Zero, relay),
            method::Method::new(&namer("sleep"), ParamCount::Range(0, 1), relay),
        ])
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(RsPublicWorker::new())
    }
}

struct RsPublicWorker {
    env: Option<RsPublicEnv>,
    client: Option<Client>,
    config: Option<Arc<conf::Config>>,
    host_settings: Option<Arc<HostSettings>>,
    // Worker/thread-specific value that persists for the life of the worker.
    relay_count: usize,
}

impl RsPublicWorker {
    pub fn new() -> Self {
        RsPublicWorker {
            env: None,
            client: None,
            config: None,
            host_settings: None,
            // A value that increases with each call relayed.
            relay_count: 0,
        }
    }

    /// We must have a value here since absorb_env() is invoked on the worker.
    pub fn _env(&self) -> &RsPublicEnv {
        self.env.as_ref().unwrap()
    }

    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsPublicWorker, String> {
        match w.as_any_mut().downcast_mut::<RsPublicWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }

    ///
    /// self.client is guaranteed to set after absorb_env()
    fn _client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl ApplicationWorker for RsPublicWorker {
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
        self.client = Some(client);
        self.config = Some(config);
        self.host_settings = Some(host_settings);

        match env.as_any().downcast_ref::<RsPublicEnv>() {
            Some(eref) => self.env = Some(eref.clone()),
            None => panic!("Unexpected environment type in absorb_env()"),
        }
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
    if let Err(e) = Server::start(Box::new(RsPublicApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}

fn relay(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let mut worker = RsPublicWorker::downcast(worker)?;
    worker.relay_count += 1;
    let api_name = method.method().replace("rs-public", "rs-private");

    for resp in
        worker
            .client_mut()
            .sendrecv("opensrf.rs-private", &api_name, method.params().clone())?
    {
        session.respond(resp.clone())?;
        session.respond(json::from(format!("Relay count: {}", worker.relay_count)))?
    }

    Ok(())
}
