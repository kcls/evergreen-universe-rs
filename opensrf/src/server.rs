use super::app;
use super::client::Client;
use super::conf;
use super::init;
use super::message;
use super::method;
use super::sclient::{HostSettings, SettingsClient};
use super::session;
use super::worker::{Worker, WorkerState, WorkerStateEvent};
use signal_hook;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Warn when there are fewer than this many idle threads
const IDLE_THREAD_WARN_THRESHOLD: usize = 1;
const CHECK_COMMANDS_TIMEOUT: u64 = 1;

#[derive(Debug)]
struct WorkerThread {
    state: WorkerState,
    join_handle: thread::JoinHandle<()>,
}

pub struct Server {
    application: Box<dyn app::Application>,
    methods: Option<Arc<HashMap<String, method::Method>>>,
    config: Arc<conf::Config>,
    client: Client,
    // Worker threads are tracked via their bus address.
    workers: HashMap<u64, WorkerThread>,
    // Each thread gets a simple numeric ID.
    worker_id_gen: u64,
    to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    to_parent_rx: mpsc::Receiver<WorkerStateEvent>,
    stopping: Arc<AtomicBool>,
    host_settings: Arc<HostSettings>,
    // TODO min/max idle
    min_workers: usize,
    max_workers: usize,
}

impl Server {
    pub fn start(application: Box<dyn app::Application>) -> Result<(), String> {
        let service = application.name();

        let config = match init::init() {
            Ok(c) => c,
            Err(e) => panic!("Cannot start server for {}: {}", service, e),
        };

        // We're done editing our Config. Wrap it in an Arc.
        let config = config.into_shared();

        let mut client = match Client::connect(config.clone()) {
            Ok(c) => c,
            Err(e) => panic!("Server cannot connect to bus: {}", e),
        };

        let host_settings = match SettingsClient::get_host_settings(&mut client, true) {
            Ok(s) => s,
            Err(e) => panic!("Cannot fetch host setttings: {}", e),
        };

        let min_workers = host_settings
            .value(&format!("apps/{service}/unix_config/min_workers"))
            .as_usize()
            .unwrap_or(1);

        let max_workers = host_settings
            .value(&format!("apps/{service}/unix_config/max_workers"))
            .as_usize()
            .unwrap_or(20);

        // We have a single to-parent channel whose trasmitter is cloned
        // per thread.  Communication from worker threads to the parent
        // are synchronous so the parent always knows exactly how many
        // threads are active.
        let (tx, rx): (
            mpsc::SyncSender<WorkerStateEvent>,
            mpsc::Receiver<WorkerStateEvent>,
        ) = mpsc::sync_channel(0);

        let mut server = Server {
            config,
            client,
            application,
            min_workers,
            max_workers,
            methods: None,
            worker_id_gen: 0,
            to_parent_tx: tx,
            to_parent_rx: rx,
            workers: HashMap::new(),
            stopping: Arc::new(AtomicBool::new(false)),
            host_settings: host_settings.into_shared(),
        };

        server.listen()
    }

    fn config(&self) -> &Arc<conf::Config> {
        &self.config
    }

    fn host_settings(&self) -> &Arc<HostSettings> {
        &self.host_settings
    }

    fn app(&self) -> &Box<dyn app::Application> {
        &self.application
    }

    fn app_mut(&mut self) -> &mut Box<dyn app::Application> {
        &mut self.application
    }

    fn service(&self) -> &str {
        self.app().name()
    }

    fn next_worker_id(&mut self) -> u64 {
        self.worker_id_gen += 1;
        self.worker_id_gen
    }

    fn spawn_threads(&mut self) {
        let mut worker_count = self.workers.len();

        while worker_count < self.min_workers {
            self.spawn_one_thread();
            worker_count = self.workers.len();
        }
    }

    fn spawn_one_thread(&mut self) {
        let worker_id = self.next_worker_id();
        let methods = self.methods.as_ref().unwrap().clone();
        let confref = self.config().clone();
        let to_parent_tx = self.to_parent_tx.clone();
        let service = self.service().to_string();
        let factory = self.app().worker_factory();
        let env = self.app().env();
        let host_settings = self.host_settings.clone();

        log::trace!("server: spawning a new worker {worker_id}");

        let handle = thread::spawn(move || {
            Server::start_worker_thread(
                env,
                host_settings,
                factory,
                service,
                worker_id,
                confref,
                methods,
                to_parent_tx,
            );
        });

        self.workers.insert(
            worker_id,
            WorkerThread {
                state: WorkerState::Idle,
                join_handle: handle,
            },
        );
    }

    fn start_worker_thread(
        env: Box<dyn app::ApplicationEnv>,
        host_settings: Arc<HostSettings>,
        factory: app::ApplicationWorkerFactory,
        service: String,
        worker_id: u64,
        config: Arc<conf::Config>,
        methods: Arc<HashMap<String, method::Method>>,
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    ) {
        log::trace!("Creating new worker {worker_id}");

        let mut worker = match Worker::new(
            service,
            worker_id,
            config,
            host_settings,
            methods,
            to_parent_tx,
        ) {
            Ok(w) => w,
            Err(e) => {
                log::error!("Cannot create worker: {e}. Exiting.");

                // If a worker dies during creation, likely they all
                // will.  Add a sleep here to avoid a storm of new
                // worker threads spinning up and failing.
                thread::sleep(Duration::from_secs(5));
                return;
            }
        };

        log::trace!("Worker {worker_id} going into listen()");

        match worker.create_app_worker(factory, env) {
            Ok(w) => worker.listen(w),
            Err(e) => {
                log::error!("Cannot create app worker: {e}. Exiting.");
                return;
            }
        }
    }

    /// List of domains where our service is allowed to run and
    /// therefore whose routers with whom our presence should be registered.
    fn hosting_domains(&self) -> Vec<String> {
        let mut domains: Vec<String> = Vec::new();
        for router in self.config().client().routers() {
            match router.services() {
                Some(services) => {
                    if services.contains(&self.service().to_string()) {
                        domains.push(router.domain().to_string());
                    }
                }
                None => {
                    // A domain with no specific set of hosted services
                    // hosts all services
                    domains.push(router.domain().to_string());
                }
            }
        }

        domains
    }

    fn register_routers(&mut self) -> Result<(), String> {
        for domain in self.hosting_domains().iter() {
            log::info!("server: registering with router at {domain}");

            self.client
                .send_router_command(domain, "register", Some(self.service()), false)?;
        }

        Ok(())
    }

    fn unregister_routers(&mut self) -> Result<(), String> {
        for domain in self.hosting_domains().iter() {
            log::info!("server: un-registering with router at {domain}");

            self.client
                .send_router_command(domain, "unregister", Some(self.service()), false)?;
        }
        Ok(())
    }

    fn setup_signal_handlers(&self) -> Result<(), String> {
        // If any of these signals occur, our self.stopping flag will be set to true
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            if let Err(e) = signal_hook::flag::register(sig, self.stopping.clone()) {
                return Err(format!("Cannot register signal handler: {e}"));
            }
        }

        Ok(())
    }

    fn register_methods(&mut self) -> Result<(), String> {
        let client = self.client.clone();
        let config = self.config().clone();
        let host_settings = self.host_settings().clone();
        let list = self
            .app_mut()
            .register_methods(client, config, host_settings)?;
        let mut hash: HashMap<String, method::Method> = HashMap::new();
        for m in list {
            hash.insert(m.name().to_string(), m);
        }
        self.add_system_methods(&mut hash);
        self.methods = Some(Arc::new(hash));
        Ok(())
    }

    fn add_system_methods(&self, hash: &mut HashMap<String, method::Method>) {
        let name = "opensrf.system.echo";
        hash.insert(
            name.to_string(),
            method::Method::new(name, method::ParamCount::Any, system_method_echo),
        );
    }

    pub fn listen(&mut self) -> Result<(), String> {
        self.register_methods()?;
        self.register_routers()?;
        self.spawn_threads();
        self.setup_signal_handlers()?;

        let duration = Duration::from_secs(CHECK_COMMANDS_TIMEOUT);

        loop {
            // Wait for worker thread state updates

            // Wait up to 'duration' seconds before looping around and
            // trying again.  This leaves room for other potential
            // housekeeping between recv calls.
            //
            // This will return an Err on timeout or a
            // failed/disconnected thread.
            if let Ok(evt) = self.to_parent_rx.recv_timeout(duration) {
                self.handle_worker_event(&evt);
            }

            self.check_failed_threads();

            // Did a signal set our "stopping" flag?
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("We received a stop signal, exiting");
                break;
            }
        }

        self.unregister_routers()
    }

    // Check for threads that panic!ed and were unable to send any
    // worker state info to us.
    fn check_failed_threads(&mut self) {
        let failed: Vec<u64> = self
            .workers
            .iter()
            .filter(|(_, v)| v.join_handle.is_finished())
            .map(|(k, _)| *k) // k is a &u64
            .collect();

        for worker_id in failed {
            log::info!("Found a thread that exited ungracefully: {worker_id}");
            self.remove_thread(&worker_id);
        }
    }

    fn remove_thread(&mut self, worker_id: &u64) {
        log::trace!("server: removing thread {}", worker_id);
        self.workers.remove(worker_id);
        self.spawn_threads();
    }

    /// Set the state of our thread worker based on the state reported
    /// to us by the thread.
    fn handle_worker_event(&mut self, evt: &WorkerStateEvent) {
        log::trace!("server received WorkerStateEvent: {:?}", evt);

        let worker_id = evt.worker_id();

        let worker: &mut WorkerThread = match self.workers.get_mut(&worker_id) {
            Some(w) => w,
            None => {
                log::error!("No worker found with id {worker_id}");
                return;
            }
        };

        if evt.state() == WorkerState::Done {
            // Worker is done -- remove it and fire up new ones as needed.
            self.remove_thread(&worker_id);
        } else {
            log::trace!("server: updating thread state: {:?}", worker_id);
            worker.state = evt.state();
        }

        let idle = self.idle_thread_count();
        let active = self.active_thread_count();

        log::trace!("server: workers idle={idle} active={active}");

        if idle == 0 {
            // TODO min idle, etc.
            if active < self.max_workers {
                self.spawn_one_thread();
            } else {
                log::warn!("server: reached max workers!");
            }
        }

        if idle < IDLE_THREAD_WARN_THRESHOLD {
            log::warn!(
                "server: idle thread count={} is below warning threshold={}",
                idle,
                IDLE_THREAD_WARN_THRESHOLD
            );
        }
    }

    fn active_thread_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Active)
            .count()
    }

    fn idle_thread_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Idle)
            .count()
    }
}

// Toss our system method handlers down here.
fn system_method_echo(
    _worker: &mut Box<dyn app::ApplicationWorker>,
    session: &mut session::ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    for p in method.params() {
        session.respond(p.clone())?;
    }
    Ok(())
}
