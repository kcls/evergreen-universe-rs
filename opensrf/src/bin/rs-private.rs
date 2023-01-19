use opensrf::app::{Application, ApplicationEnv, ApplicationWorker, ApplicationWorkerFactory};
use opensrf::client;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::sclient::HostSettings;
use opensrf::server::Server;
use opensrf::session::ServerSession;
use std::any::Any;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const APPNAME: &str = "opensrf.rs-private";

/// Clone is needed here to support our implementation of downcast();
#[derive(Debug, Clone)]
struct RsPrivateEnv {
    some_global_thing: Arc<String>,
}

impl RsPrivateEnv {
    pub fn new(something: Arc<String>) -> Self {
        RsPrivateEnv {
            some_global_thing: something,
        }
    }
}

impl ApplicationEnv for RsPrivateEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsPrivateApplication;

impl RsPrivateApplication {
    pub fn new() -> Self {
        RsPrivateApplication {}
    }
}

impl Application for RsPrivateApplication {
    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsPrivateEnv::new(Arc::new(String::from("FOO"))))
    }

    fn register_methods(
        &self,
        _client: client::Client,
        _config: Arc<conf::Config>,
        _host_settings: Arc<HostSettings>,
    ) -> Result<Vec<method::Method>, String> {
        log::info!("Registering methods for {}", self.name());

        Ok(vec![
            method::Method::new("opensrf.rs-private.time", method::ParamCount::Zero, time),
            method::Method::new(
                "opensrf.rs-private.counter",
                method::ParamCount::Zero,
                counter,
            ),
            method::Method::new(
                "opensrf.rs-private.sleep",
                method::ParamCount::Range(0, 1),
                sleep,
            ),
        ])
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || Box::new(RsPrivateWorker::new())
    }
}

struct RsPrivateWorker {
    env: Option<RsPrivateEnv>,
    client: Option<client::Client>,
    config: Option<Arc<conf::Config>>,
    host_settings: Option<Arc<HostSettings>>,
    count: usize,
}

impl RsPrivateWorker {
    pub fn new() -> Self {
        RsPrivateWorker {
            env: None,
            client: None,
            config: None,
            host_settings: None,
            // A value that increases with each call to counter method
            // to demostrate thread-level state maintenance.
            count: 0,
        }
    }

    /// We must have a value here since absorb_env() is invoked on the worker.
    pub fn env(&self) -> &RsPrivateEnv {
        self.env.as_ref().unwrap()
    }

    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsPrivateWorker, String> {
        match w.as_any_mut().downcast_mut::<RsPrivateWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }
}

impl ApplicationWorker for RsPrivateWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Panics if we cannot downcast the env provided to the expected type.
    fn absorb_env(
        &mut self,
        client: client::Client,
        config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
        env: Box<dyn ApplicationEnv>,
    ) -> Result<(), String> {
        self.client = Some(client);
        self.config = Some(config);
        self.host_settings = Some(host_settings);

        match env.as_any().downcast_ref::<RsPrivateEnv>() {
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
    if let Err(e) = Server::start(Box::new(RsPrivateApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}

fn time(
    _worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    _method: &message::Method,
) -> Result<(), String> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    session.respond(json::from(dur.as_secs()))?;
    Ok(())
}

fn counter(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    _method: &message::Method,
) -> Result<(), String> {
    let mut worker = RsPrivateWorker::downcast(worker)?;
    worker.count += 1;
    log::info!(
        "Here's some data from the environment: {}",
        worker.env().some_global_thing
    );
    session.respond(worker.count)?;
    Ok(())
}

fn sleep(
    _worker: &mut Box<dyn ApplicationWorker>,
    _session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    // Param count may be zero
    let secs = match method.params().get(0) {
        Some(p) => p.as_u8().unwrap_or(1),
        _ => 1,
    };

    log::debug!("sleep() waiting for {} seconds", secs);

    thread::sleep(Duration::from_secs(secs as u64));

    Ok(())
}
