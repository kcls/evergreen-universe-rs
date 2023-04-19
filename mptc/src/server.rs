use super::worker::{Worker, WorkerInstance, WorkerState, WorkerStateEvent};
use super::{Request, RequestStream};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct Server {
    worker_id_gen: u64,
    workers: HashMap<u64, WorkerInstance>,

    to_parent_rx: mpsc::Receiver<WorkerStateEvent>,
    to_parent_tx: mpsc::Sender<WorkerStateEvent>,

    min_workers: usize,
    max_workers: usize,
    max_worker_reqs: usize,

    shutdown: Arc<AtomicBool>,
    reload: Arc<AtomicBool>,

    /// All inbound requests arrive via this stream.
    stream: Box<dyn RequestStream>,
}

impl Server {
    pub fn new(stream: Box<dyn RequestStream>) -> Server {
        let (tx, rx): (
            mpsc::Sender<WorkerStateEvent>,
            mpsc::Receiver<WorkerStateEvent>,
        ) = mpsc::channel();

        Server {
            stream,
            workers: HashMap::new(),
            worker_id_gen: 0,
            to_parent_tx: tx,
            to_parent_rx: rx,
            min_workers: super::DEFAULT_MIN_WORKERS,
            max_workers: super::DEFAULT_MAX_WORKERS,
            max_worker_reqs: super::DEFAULT_MAX_WORKER_REQS,
            shutdown: Arc::new(AtomicBool::new(false)),
            reload: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_min_workers(&mut self, v: usize) {
        self.min_workers = v;
    }
    pub fn set_max_workers(&mut self, v: usize) {
        self.max_workers = v;
    }
    pub fn set_max_worker_requests(&mut self, v: usize) {
        self.max_worker_reqs = v;
    }

    fn next_worker_id(&mut self) -> u64 {
        self.worker_id_gen += 1;
        self.worker_id_gen
    }

    fn start_workers(&mut self) {
        while self.workers.len() < self.min_workers {
            self.start_one_worker();
        }
    }

    fn start_one_worker(&mut self) -> u64 {
        let worker_id = self.next_worker_id();
        let to_parent_tx = self.to_parent_tx.clone();
        let max_reqs = self.max_worker_reqs;
        let shutdown = self.shutdown.clone();
        let handler = self.stream.new_handler();

        let (tx, rx): (
            mpsc::Sender<Box<dyn Request>>,
            mpsc::Receiver<Box<dyn Request>>,
        ) = mpsc::channel();

        let handle = thread::spawn(move || {
            let mut w = Worker::new(worker_id, max_reqs, to_parent_tx, rx, shutdown, handler);
            w.run();
        });

        let instance = WorkerInstance {
            worker_id,
            state: WorkerState::Idle,
            join_handle: handle,
            to_worker_tx: tx,
        };

        self.workers.insert(worker_id, instance);

        worker_id
    }

    fn active_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Active)
            .count()
    }

    fn idle_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Idle)
            .count()
    }

    fn remove_worker(&mut self, worker_id: &u64, respawn: bool) {
        log::trace!("server: removing worker {}", worker_id);

        if let Some(worker) = self.workers.remove(worker_id) {
            if let Err(e) = worker.join_handle.join() {
                log::error!("Worker join failed with: {e:?}");
            }
        }
        if respawn {
            self.start_workers();
        }
    }

    /// Set the state of our thread worker based on the state reported
    /// to us by the thread.
    fn handle_worker_event(&mut self, evt: &WorkerStateEvent) {
        log::trace!("server received WorkerStateEvent: {evt}");

        let worker_id = evt.worker_id();

        let worker = match self.workers.get_mut(&worker_id) {
            Some(w) => w,
            None => {
                log::error!("No worker found with id {worker_id}");
                return;
            }
        };

        if evt.state() == &WorkerState::Done {
            // Worker is done -- remove it and fire up new ones as needed.
            self.remove_worker(&worker_id, true);
        } else {
            log::trace!("Updating thread state for worker: {}", worker_id);
            worker.state = evt.state().clone();
        }

        let idle = self.idle_worker_count();
        let active = self.active_worker_count();

        log::trace!("Workers idle={idle} active={active}");

        if idle == 0 {
            // Try to keep at least one spare worker.
            if active < self.max_workers {
                self.start_one_worker();
            } else {
                log::warn!("server: reached max workers.  Cannot create spare worker");
            }
        }
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
            log::debug!("Found a thread that exited ungracefully: {worker_id}");
            self.remove_worker(&worker_id, true);
        }
    }

    /// Returns true if the it's time to shut down.
    ///
    /// * `block` - Continue performing housekeeping until an idle worker
    /// becomes available or a shutdown signal is received.
    fn housekeeping(&mut self, block: bool) -> bool {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("We received a stop signal, exiting");
                return true;
            }

            if self.reload.load(Ordering::Relaxed) {
                log::info!("Reload request received.  Reloading config");
                // TODO
                // This will have to be passed to our RequestStream
            }

            if block {
                log::debug!("Waiting for a worker to become available...");

                // Wait up to 1 second for a worker state event, then
                // resume housekeeping.
                if let Ok(evt) = self.to_parent_rx.recv_timeout(Duration::from_secs(1)) {
                    self.handle_worker_event(&evt);
                }
            }

            // Pull all state events from the channel.
            loop {
                if let Ok(evt) = self.to_parent_rx.try_recv() {
                    self.handle_worker_event(&evt);
                } else {
                    break;
                }
            }

            // Finally clean up any threads that panic!ed before they
            // could send a state event.
            self.check_failed_threads();

            if !block || self.idle_worker_count() > 0 {
                return false;
            }
        }
    }

    pub fn run(&mut self) {
        if let Err(e) = self.setup_signal_handlers() {
            log::error!("Cannot setup signal handlers: {e}");
            return;
        }

        self.start_workers();

        loop {
            if self.housekeeping(false) {
                break;
            }

            // pull request data from something

            match self.stream.next(super::SIGNAL_POLL_INTERVAL) {
                Ok(op) => match op {
                    Some(s) => self.dispatch_request(s),
                    None => {} // timed out.
                },
                Err(e) => {
                    log::error!("Exiting on stream error: {e}");
                    // TODO set shutdown?
                    return;
                }
            }

            if self.housekeeping(false) {
                break;
            }
        }

        loop {
            let id = match self.workers.keys().next() {
                Some(i) => *i,
                None => break,
            };
            log::debug!("Server cleaning up worker {}", id);
            self.remove_worker(&id, false);
        }
    }

    fn dispatch_request(&mut self, request: Box<dyn Request>) {
        let wid = self.next_idle_worker();
        if let Some(worker) = self.workers.get_mut(&wid) {
            worker.state = WorkerState::Active;

            if let Err(e) = worker.to_worker_tx.send(request) {
                // If sending to the worker fails, which really should
                // not happen, since this worker was just verified idle,
                // then the request as a whole is dropped.  We could
                // handle this in a more robust way, but the assumption
                // this should in effect never happen.  The logs will tell.
                log::error!("Error sending data to worker: {e}");
            }
        }
    }

    fn next_idle_worker(&mut self) -> u64 {
        // 1. Find an idle worker
        if let Some((k, _)) = self
            .workers
            .iter()
            .filter(|(_, w)| w.state() == &WorkerState::Idle)
            .next()
        {
            return *k; // &u64
        }

        // 2. Create an idle worker if we can
        if self.workers.len() < self.max_workers {
            return self.start_one_worker();
        }

        log::warn!("Max workers reached.  Cannot spawn new worker");

        loop {
            // 3. Wait for a worker to become idle.
            self.housekeeping(true);

            if let Some((k, _)) = self
                .workers
                .iter()
                .filter(|(_, w)| w.state() == &WorkerState::Idle)
                .next()
            {
                return *k; // &u64
            }
        }
    }

    fn setup_signal_handlers(&self) -> Result<(), String> {
        // TERM and INT result in a graceful shutdown
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            if let Err(e) = signal_hook::flag::register(sig, self.shutdown.clone()) {
                Err(format!("Cannot register signal handler: {e}"))?;
            }
        }

        // HUP causes us to reload our configuration.
        if let Err(e) =
            signal_hook::flag::register(signal_hook::consts::SIGHUP, self.reload.clone())
        {
            Err(format!("Cannot register HUP signal: {e}"))?;
        }

        Ok(())
    }
}
