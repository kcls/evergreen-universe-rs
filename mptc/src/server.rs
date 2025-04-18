use super::signals::SignalTracker;
use super::worker::{Worker, WorkerInstance, WorkerState, WorkerStateEvent};
use super::{Request, RequestStream};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// How often do we log our idle/active thread counts.
const LOG_THREAD_STATS_FREQUENCY: i32 = 5;

/// Only log thread stats if at least this many threads are active.
const LOG_THREAD_MIN_ACTIVE: usize = 5;

type RequestSendChannel = mpsc::Sender<Box<dyn Request>>;
type RequestReceiveChannel = mpsc::Receiver<Box<dyn Request>>;

type StateEventSendChannel = mpsc::Sender<WorkerStateEvent>;
type StateEventReceiveChannel = mpsc::Receiver<WorkerStateEvent>;

pub struct Server {
    worker_id_gen: u64,
    workers: HashMap<u64, WorkerInstance>,

    to_parent_rx: StateEventReceiveChannel,
    to_parent_tx: StateEventSendChannel,

    min_workers: usize,
    max_workers: usize,
    min_idle_workers: usize,
    max_worker_requests: usize,

    sig_tracker: SignalTracker,

    /// All inbound requests arrive via this stream.
    stream: Box<dyn RequestStream>,
}

impl Server {
    pub fn new(stream: Box<dyn RequestStream>) -> Server {
        let (tx, rx): (StateEventSendChannel, StateEventReceiveChannel) = mpsc::channel();

        Server {
            stream,
            workers: HashMap::new(),
            sig_tracker: SignalTracker::new(),
            worker_id_gen: 0,
            to_parent_tx: tx,
            to_parent_rx: rx,
            min_workers: super::DEFAULT_MIN_WORKERS,
            min_idle_workers: super::DEFAULT_MIN_IDLE_WORKERS,
            max_workers: super::DEFAULT_MAX_WORKERS,
            max_worker_requests: super::DEFAULT_MAX_WORKER_REQUESTS,
        }
    }

    pub fn set_min_workers(&mut self, v: usize) {
        self.min_workers = v;
    }
    pub fn set_min_idle_workers(&mut self, v: usize) {
        self.min_idle_workers = v;
    }
    pub fn set_max_workers(&mut self, v: usize) {
        self.max_workers = v;
    }
    pub fn set_max_worker_requests(&mut self, v: usize) {
        self.max_worker_requests = v;
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

    fn stop_workers(&mut self) {
        while let Some(id) = self.workers.keys().next().copied() {
            log::debug!("Server cleaning up worker {}", id);
            self.remove_worker(&id, false);
        }
    }

    fn start_one_worker(&mut self) -> u64 {
        let worker_id = self.next_worker_id();
        let to_parent_tx = self.to_parent_tx.clone();
        let max_reqs = self.max_worker_requests;
        let handler = self.stream.new_handler();
        let sig_tracker = self.sig_tracker.clone();

        log::debug!(
            "Starting worker with idle={} active={}",
            self.idle_worker_count(),
            self.active_worker_count(),
        );

        let (tx, rx): (RequestSendChannel, RequestReceiveChannel) = mpsc::channel();

        let handle = thread::spawn(move || {
            let mut w = Worker::new(worker_id, max_reqs, sig_tracker, to_parent_tx, rx, handler);
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

    /// Add additional idle workers if needed.
    ///
    /// Spawn at most one worker per maintenance cycle.
    fn perform_idle_worker_maint(&mut self) {
        let idle_workers = self.idle_worker_count();

        if self.min_idle_workers > 0
            && self.workers.len() < self.max_workers
            && idle_workers < self.min_idle_workers
        {
            self.start_one_worker();
            log::debug!("Sawned idle worker; idle={idle_workers}");
        }
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
        log::debug!("server: removing worker {}", worker_id);

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
    ///   becomes available.
    fn housekeeping(&mut self, block: bool) -> bool {
        loop {
            if self.sig_tracker.reload_requested() {
                log::info!("Reload request received.");
                self.sig_tracker.handle_reload_requested();

                if let Err(e) = self.stream.reload() {
                    log::error!("Reload command failed, exiting. {e}");
                    return true;
                }
            }

            if self.sig_tracker.any_shutdown_requested() {
                log::info!("Shutdown request received.");
                self.stream.shutdown();
                return true;
            }

            if block {
                log::debug!("Waiting for a worker to become available...");

                // Wait up to 1 second for a worker state event, then
                // resume housekeeping, looping back around and trying
                // again if necessary.
                if let Ok(evt) = self.to_parent_rx.recv_timeout(Duration::from_secs(1)) {
                    self.handle_worker_event(&evt);
                }
            }

            // Pull all state events from the channel.
            while let Ok(evt) = self.to_parent_rx.try_recv() {
                self.handle_worker_event(&evt);
            }

            // Finally clean up any threads that panic!ed before they
            // could send a state event.
            self.check_failed_threads();

            self.perform_idle_worker_maint();

            if !block || self.idle_worker_count() > 0 {
                return false;
            }
        }
    }

    pub fn run(&mut self) {
        self.sig_tracker.track_graceful_shutdown();
        self.sig_tracker.track_fast_shutdown();
        self.sig_tracker.track_reload();

        log::debug!(
            "server: starting workers min-workers={} mid-idle-workers={} max-workers={} max-worker-requests={}",
            self.min_workers,
            self.min_idle_workers,
            self.max_workers,
            self.max_worker_requests,
        );

        self.start_workers();

        let mut log_timer = Instant::now();

        loop {
            match self.stream.next() {
                Ok(req_op) => {
                    if let Some(req) = req_op {
                        self.dispatch_request(req);
                    }
                }
                Err(e) => {
                    log::error!("Exiting on stream error: {e}");
                    break;
                }
            }

            if self.housekeeping(false) {
                break;
            }

            self.log_thread_counts(&mut log_timer);
        }

        self.stop_workers();
    }

    /// Periodically report our active/idle thread disposition
    /// so monitoring tools can keep track.
    ///
    /// Nothing is logged if all threads are idle.
    ///
    /// You can also do things via command line like: $ ps huH p $pid
    fn log_thread_counts(&self, timer: &mut Instant) {
        let elapsed = timer.elapsed().as_secs() as i32;

        if LOG_THREAD_STATS_FREQUENCY - elapsed > 0 {
            return;
        }

        let active_count = self.active_worker_count();

        if active_count < LOG_THREAD_MIN_ACTIVE {
            return;
        }

        log::info!(
            "MPTC max-threads={} active-threads={} idle-threads={}",
            self.max_workers,
            active_count,
            self.idle_worker_count(),
        );

        *timer = Instant::now();
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
            .find(|(_, w)| w.state() == &WorkerState::Idle)
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
                .find(|(_, w)| w.state() == &WorkerState::Idle)
            {
                return *k; // &u64
            }
        }
    }
}
