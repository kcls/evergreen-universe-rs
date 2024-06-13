use crate::init;
use crate::osrf::app;
use crate::osrf::client::Client;
use crate::osrf::conf;
use crate::osrf::message;
use crate::osrf::method;
use crate::osrf::sclient::HostSettings;
use crate::osrf::session;
use crate::osrf::worker::{Worker, WorkerState, WorkerStateEvent};
use crate::util;
use crate::EgResult;
use mptc::signals::SignalTracker;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

/// Warn when there are fewer than this many idle threads
const IDLE_THREAD_WARN_THRESHOLD: usize = 1;
/// How often do we wake to check for shutdown, etc. signals when
/// no other activity is occurring.
const IDLE_WAKE_TIME: u64 = 3;
/// Max time in seconds to allow active workers to finish their tasks.
const SHUTDOWN_MAX_WAIT: i32 = 30;
const DEFAULT_MIN_WORKERS: usize = 3;
const DEFAULT_MAX_WORKERS: usize = 30;
const DEFAULT_MIN_IDLE_WORKERS: usize = 1;
/// How often do we log our idle/active thread counts.
const LOG_THREAD_STATS_FREQUENCY: i32 = 10;

#[derive(Debug)]
pub struct WorkerThread {
    pub state: WorkerState,
    pub join_handle: thread::JoinHandle<()>,
}

pub struct Server {
    application: Box<dyn app::Application>,
    methods: Option<Arc<HashMap<String, method::MethodDef>>>,
    client: Client,
    // Worker threads are tracked via their bus address.
    workers: HashMap<u64, WorkerThread>,
    // Each thread gets a simple numeric ID.
    worker_id_gen: u64,
    to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    to_parent_rx: mpsc::Receiver<WorkerStateEvent>,
    min_workers: usize,
    max_workers: usize,

    sig_tracker: SignalTracker,

    /// Minimum number of idle workers.  Note we don't support
    /// max_idle_workers at this time -- it would require adding an
    /// additional mpsc channel for every thread to deliver the shutdown
    /// request to individual threads.  Hardly seems worth it -- maybe.
    /// For comparision, the OSRF C code has no min/max idle support
    /// either.
    min_idle_workers: usize,
}

impl Server {
    pub fn start(application: Box<dyn app::Application>) -> EgResult<()> {
        let service = application.name();

        let mut options = init::InitOptions::new();
        options.appname = Some(service.to_string());

        let client = init::osrf_init(&options)?;

        let min_workers = HostSettings::get(&format!("apps/{service}/unix_config/min_children"))?
            .as_usize()
            .unwrap_or(DEFAULT_MIN_WORKERS);

        let min_idle_workers =
            HostSettings::get(&format!("apps/{service}/unix_config/min_spare_children"))?
                .as_usize()
                .unwrap_or(DEFAULT_MIN_IDLE_WORKERS);

        let max_workers = HostSettings::get(&format!("apps/{service}/unix_config/max_children"))?
            .as_usize()
            .unwrap_or(DEFAULT_MAX_WORKERS);

        // We have a single to-parent channel whose trasmitter is cloned
        // per thread.  Communication from worker threads to the parent
        // are synchronous so the parent always knows exactly how many
        // threads are active.  With a sync_channel queue size of 0,
        // workers will block after posting their state events to
        // the server until the server receives the event.
        let (tx, rx): (
            mpsc::SyncSender<WorkerStateEvent>,
            mpsc::Receiver<WorkerStateEvent>,
        ) = mpsc::sync_channel(0);

        let mut server = Server {
            client,
            application,
            min_workers,
            max_workers,
            min_idle_workers,
            methods: None,
            worker_id_gen: 0,
            to_parent_tx: tx,
            to_parent_rx: rx,
            workers: HashMap::new(),
            sig_tracker: SignalTracker::new(),
        };

        server.listen()
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
        if self.sig_tracker.any_shutdown_requested() {
            return;
        }
        while self.workers.len() < self.min_workers {
            self.spawn_one_thread();
        }
    }

    fn spawn_one_thread(&mut self) {
        let worker_id = self.next_worker_id();
        let methods = self.methods.as_ref().unwrap().clone();
        let to_parent_tx = self.to_parent_tx.clone();
        let service = self.service().to_string();
        let factory = self.app().worker_factory();
        let sig_tracker = self.sig_tracker.clone();

        log::trace!("server: spawning a new worker {worker_id}");

        let handle = thread::spawn(move || {
            Server::start_worker_thread(
                sig_tracker,
                factory,
                service,
                worker_id,
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
        sig_tracker: SignalTracker,
        factory: app::ApplicationWorkerFactory,
        service: String,
        worker_id: u64,
        methods: Arc<HashMap<String, method::MethodDef>>,
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    ) {
        log::trace!("Creating new worker {worker_id}");

        let mut worker = match Worker::new(service, worker_id, sig_tracker, methods, to_parent_tx) {
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

        worker.listen(factory);
    }

    /// List of domains where our service is allowed to run and
    /// therefore whose routers with whom our presence should be registered.
    fn hosting_domains(&self) -> Vec<(String, String)> {
        let mut domains: Vec<(String, String)> = Vec::new();
        for router in conf::config().client().routers() {
            match router.services() {
                Some(services) => {
                    if services.iter().any(|s| s.eq(self.service())) {
                        domains.push((router.username().to_string(), router.domain().to_string()));
                    }
                }
                None => {
                    // A domain with no specific set of hosted services
                    // hosts all services
                    domains.push((router.username().to_string(), router.domain().to_string()));
                }
            }
        }

        domains
    }

    fn register_routers(&mut self) -> EgResult<()> {
        for (username, domain) in self.hosting_domains().iter() {
            log::info!("server: registering with router at {domain}");

            self.client
                .send_router_command(username, domain, "register", Some(self.service()))?;
        }

        Ok(())
    }

    fn unregister_routers(&mut self) -> EgResult<()> {
        for (username, domain) in self.hosting_domains().iter() {
            log::info!("server: un-registering with router at {domain}");

            self.client.send_router_command(
                username,
                domain,
                "unregister",
                Some(self.service()),
            )?;
        }
        Ok(())
    }

    fn service_init(&mut self) -> EgResult<()> {
        let client = self.client.clone();
        self.app_mut().init(client)
    }

    fn register_methods(&mut self) -> EgResult<()> {
        let client = self.client.clone();
        let list = self.app_mut().register_methods(client)?;
        let mut hash: HashMap<String, method::MethodDef> = HashMap::new();
        for m in list {
            hash.insert(m.name().to_string(), m);
        }
        self.add_system_methods(&mut hash);
        //self.add_atomic_methods(&mut hash);
        self.methods = Some(Arc::new(hash));
        Ok(())
    }

    // currently unused
    /*
    fn add_atomic_methods(&self, hash: &mut HashMap<String, method::MethodDef>) {
        let mut atomic_hash: HashMap<String, method::MethodDef> = HashMap::new();

        for method in hash.values() {
            let mut atomic_method = method.clone();
            let name = method.name();
            let atomic_name = format!("{name}.atomic");
            atomic_method.set_atomic(true);
            atomic_method.set_name(&atomic_name);
            atomic_hash.insert(atomic_name, atomic_method);
        }

        hash.extend(atomic_hash);
    }
    */

    fn add_system_methods(&self, hash: &mut HashMap<String, method::MethodDef>) {
        let name = "opensrf.system.echo";
        let mut method = method::MethodDef::new(name, method::ParamCount::Any, system_method_echo);
        method.set_desc("Echo back any values sent");
        hash.insert(name.to_string(), method);

        let name = "opensrf.system.time";
        let mut method = method::MethodDef::new(name, method::ParamCount::Zero, system_method_time);
        method.set_desc("Respond with system time in epoch seconds");
        hash.insert(name.to_string(), method);

        let name = "opensrf.system.method.all";
        let mut method = method::MethodDef::new(
            name,
            method::ParamCount::Range(0, 1),
            system_method_introspect,
        );
        method.set_desc("List published API definitions");

        method.add_param(method::Param {
            name: String::from("prefix"),
            datatype: method::ParamDataType::String,
            desc: Some(String::from("API name prefix filter")),
        });

        hash.insert(name.to_string(), method);

        let name = "opensrf.system.method.all.summary";
        let mut method = method::MethodDef::new(
            name,
            method::ParamCount::Range(0, 1),
            system_method_introspect,
        );
        method.set_desc("Summary list published API definitions");

        method.add_param(method::Param {
            name: String::from("prefix"),
            datatype: method::ParamDataType::String,
            desc: Some(String::from("API name prefix filter")),
        });

        hash.insert(name.to_string(), method);
    }

    pub fn listen(&mut self) -> EgResult<()> {
        self.service_init()?;
        self.register_methods()?;
        self.register_routers()?;
        self.spawn_threads();
        self.sig_tracker.track_graceful_shutdown();
        self.sig_tracker.track_fast_shutdown();
        self.sig_tracker.track_reload();

        let duration = Duration::from_secs(IDLE_WAKE_TIME);
        let mut log_timer = util::Timer::new(LOG_THREAD_STATS_FREQUENCY);

        loop {
            // Wait for worker thread state updates

            let mut work_performed = false;

            // Wait up to 'duration' seconds before looping around and
            // trying again.  This leaves room for other potential
            // housekeeping between recv calls.
            //
            // This will return an Err on timeout or a
            // failed/disconnected thread.
            if let Ok(evt) = self.to_parent_rx.recv_timeout(duration) {
                self.handle_worker_event(&evt);
                work_performed = true;
            }

            // Always check for failed threads.
            work_performed = self.check_failed_threads() || work_performed;

            if self.sig_tracker.any_shutdown_requested() {
                log::info!("We received a stop signal, exiting");
                break;
            }

            if !work_performed {
                // Only perform idle worker maintenance if no other
                // tasks were performed during this loop iter.
                self.perform_idle_worker_maint();
            }

            self.log_thread_counts(&mut log_timer);
        }

        self.unregister_routers()?;
        self.shutdown();

        Ok(())
    }

    /// Periodically report our active/idle thread disposition
    /// so monitoring tools can keep track.
    ///
    /// Nothing is logged if all threads are idle.
    ///
    /// You can also do things via command line like: $ ps huH p $pid
    fn log_thread_counts(&self, timer: &mut util::Timer) {
        if !timer.done() {
            return;
        }

        let active_count = self.active_thread_count();

        if active_count == 0 {
            return;
        }

        log::info!(
            "Service {} max-threads={} active-threads={} idle-threads={}",
            self.application.name(),
            self.max_workers,
            active_count,
            self.idle_thread_count(),
        );

        timer.reset();
    }

    /// Add additional idle workers if needed.
    ///
    /// Spawn at most one worker per maintenance cycle.
    fn perform_idle_worker_maint(&mut self) {
        let idle_workers = self.idle_thread_count();

        if self.min_idle_workers > 0
            && self.workers.len() < self.max_workers
            && idle_workers < self.min_idle_workers
        {
            self.spawn_one_thread();
            log::debug!("Sawned idle worker; idle={idle_workers}");
        }
    }

    fn shutdown(&mut self) {
        let timer = util::Timer::new(SHUTDOWN_MAX_WAIT);
        let duration = Duration::from_secs(1);

        while !timer.done() && self.workers.len() > 0 {
            let info = format!(
                "{} shutdown: {} threads; {} active; time remaining {}",
                self.application.name(),
                self.workers.len(),
                self.active_thread_count(),
                timer.remaining(),
            );

            // Nod to anyone control-C'ing from the command line.
            println!("{info}...");

            log::info!("{info}");

            if let Ok(evt) = self.to_parent_rx.recv_timeout(duration) {
                self.handle_worker_event(&evt);
            }

            self.check_failed_threads();
        }

        // Timer may have completed before all working threads reported
        // as finished.  Force-kill all of our threads at this point.
        std::process::exit(0);
    }

    /// Check for threads that panic!ed and were unable to send any
    /// worker state info to us.
    ///
    /// Returns true if work was done.
    fn check_failed_threads(&mut self) -> bool {
        let failed: Vec<u64> = self
            .workers
            .iter()
            .filter(|(_, v)| v.join_handle.is_finished())
            .map(|(k, _)| *k) // k is a &u64
            .collect();

        let mut handled = false;
        for worker_id in failed {
            handled = true;
            log::info!("Found a thread that exited ungracefully: {worker_id}");
            self.remove_thread(&worker_id);
        }

        handled
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

        if evt.state() == WorkerState::Exiting {
            // Worker is done -- remove it and fire up new ones as needed.
            self.remove_thread(&worker_id);
        } else {
            log::trace!("server: updating thread state: {:?}", worker_id);
            worker.state = evt.state();
        }

        let idle = self.idle_thread_count();
        let active = self.active_thread_count();

        log::trace!("server: workers idle={idle} active={active}");

        if self.sig_tracker.any_shutdown_requested() {
            return;
        }

        if idle == 0 {
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
    method: message::MethodCall,
) -> EgResult<()> {
    let count = method.params().len();
    for (idx, val) in method.params().iter().enumerate() {
        if idx == count - 1 {
            // Package the final response and the COMPLETE message
            // into the same transport message for consistency
            // with the Perl code for load testing, etc. comparisons.
            session.respond_complete(val.clone())?;
        } else {
            session.respond(val.clone())?;
        }
    }
    Ok(())
}

fn system_method_time(
    _worker: &mut Box<dyn app::ApplicationWorker>,
    session: &mut session::ServerSession,
    _method: message::MethodCall,
) -> EgResult<()> {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => session.respond_complete(t.as_secs()),
        Err(e) => Err(format!("System time error: {e}").into()),
    }
}

fn system_method_introspect(
    worker: &mut Box<dyn app::ApplicationWorker>,
    session: &mut session::ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let prefix = match method.params().get(0) {
        Some(p) => p.as_str(),
        None => None,
    };

    // Collect the names first so we can sort them
    let mut names: Vec<&str> = match prefix {
        // If a prefix string is provided, only return methods whose
        // name starts with the provided prefix.
        Some(pfx) => worker
            .methods()
            .keys()
            .filter(|n| n.starts_with(pfx))
            .map(|n| n.as_str())
            .collect(),
        None => worker.methods().keys().map(|n| n.as_str()).collect(),
    };

    names.sort();

    for name in names {
        if let Some(meth) = worker.methods().get(name) {
            if method.method().contains("summary") {
                session.respond(meth.to_summary_string())?;
            } else {
                session.respond(meth.to_eg_value())?;
            }
        }
    }

    Ok(())
}
