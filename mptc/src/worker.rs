use super::signals::SignalTracker;
use super::{Request, RequestHandler};
use std::fmt;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

const SHUTDOWN_POLL_INTERVAL: u64 = 5;

#[derive(Debug, Clone, PartialEq)]
pub enum WorkerState {
    Idle,
    Active,
    Done,
}

/// # Examples
///
/// ```
/// use mptc::worker::WorkerState;
///
/// let state = WorkerState::Active;
/// assert_eq!(state.to_string(), "Active");
/// ```
impl From<&WorkerState> for &'static str {
    fn from(e: &WorkerState) -> &'static str {
        match e {
            WorkerState::Idle => "Idle",
            WorkerState::Active => "Active",
            WorkerState::Done => "Done",
        }
    }
}

impl fmt::Display for WorkerState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: &str = self.into();
        write!(f, "{s}")
    }
}

pub struct WorkerStateEvent {
    worker_id: u64,
    state: WorkerState,
}

impl fmt::Display for WorkerStateEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WorkerStateEvent worker={} state={}",
            self.worker_id, self.state
        )
    }
}

impl WorkerStateEvent {
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
    pub fn state(&self) -> &WorkerState {
        &self.state
    }
}

/// Data for tracking a specific worker thread.
pub struct WorkerInstance {
    pub worker_id: u64,
    pub state: WorkerState,
    pub join_handle: thread::JoinHandle<()>,
    pub to_worker_tx: mpsc::Sender<Box<dyn Request>>,
}

impl WorkerInstance {
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
    pub fn state(&self) -> &WorkerState {
        &self.state
    }
    pub fn join_handle(&self) -> &thread::JoinHandle<()> {
        &self.join_handle
    }
}

impl fmt::Display for WorkerInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WorkerInstance id={} state={}",
            self.worker_id, self.state
        )
    }
}

pub struct Worker {
    worker_id: u64,
    max_requests: usize,
    request_count: usize,
    start_time_epoch: u64,
    to_parent_tx: mpsc::Sender<WorkerStateEvent>,
    to_worker_rx: mpsc::Receiver<Box<dyn Request>>,
    handler: Box<dyn RequestHandler>,
    sig_tracker: SignalTracker,
}

impl Worker {
    pub fn new(
        worker_id: u64,
        max_requests: usize,
        sig_tracker: SignalTracker,
        to_parent_tx: mpsc::Sender<WorkerStateEvent>,
        to_worker_rx: mpsc::Receiver<Box<dyn Request>>,
        handler: Box<dyn RequestHandler>,
    ) -> Worker {
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Worker {
            worker_id,
            max_requests,
            sig_tracker,
            start_time_epoch: epoch,
            to_parent_tx,
            to_worker_rx,
            request_count: 0,
            handler,
        }
    }

    fn set_as_idle(&mut self) -> Result<(), String> {
        self.set_state(WorkerState::Idle)
    }

    fn set_as_done(&mut self) -> Result<(), String> {
        self.set_state(WorkerState::Done)
    }

    fn set_state(&mut self, state: WorkerState) -> Result<(), String> {
        let evt = WorkerStateEvent {
            worker_id: self.worker_id,
            state,
        };

        if let Err(e) = self.to_parent_tx.send(evt) {
            // If we're here, our parent server has exited or failed in
            // some unrecoverable way.  Tell our fellow workers it's
            // time to shut down.
            self.sig_tracker.request_fast_shutdown();

            Err(format!("Error notifying parent of state change: {e}"))
        } else {
            Ok(())
        }
    }

    fn should_shut_down(&self) -> bool {
        if self.sig_tracker.any_shutdown_requested() {
            log::debug!("{self} received shutdown, exiting run loop");
            println!("{self} received shutdown, exiting run loop");
            return true;
        }

        let reload_time = self.sig_tracker.reload_request_time();
        if reload_time > self.start_time_epoch {
            log::info!("{self} shutdown_before of {reload_time} issued.  That includes us");
            return true;
        }

        false
    }

    pub fn run(&mut self) {
        log::trace!("{self} starting");

        if let Err(e) = self.handler.worker_start() {
            log::error!("Error starting worker: {e}.  Exiting");
            return;
        }

        loop {
            if self.should_shut_down() {
                break;
            }

            let work_done = match self.process_one_request() {
                Ok(b) => b,
                Err(e) => {
                    log::error!("{self} error processing request: {e}; exiting");
                    // If we're here, our parent server has exited
                    // or failed in some unrecoverable way.
                    // Tell our fellow workers it's time to shut down.
                    self.sig_tracker.request_graceful_shutdown();
                    break;
                }
            };

            if !work_done {
                // Go back and keep listening for requests.
                continue;
            }

            self.request_count += 1;

            if self.max_requests > 0 && self.request_count == self.max_requests {
                // All done
                // No need to set_as_idle here since we're just
                // about to set_as_done.
                break;
            }

            // Request complete.  Set ourselves as idle, but only if
            // we're going back into the listen pool.
            if let Err(e) = self.set_as_idle() {
                log::debug!("{self} exiting on set_as_idle() failure: {e}");
                break;
            }
        }

        self.set_as_done().ok(); // we're done.  ignore errors.

        log::debug!("{self} exiting main listen loop");

        if let Err(e) = self.handler.worker_end() {
            log::error!("{self} handler returned on error on exit: {e}");
        }
    }

    /// Returns result of true if this worker performed any work.
    fn process_one_request(&mut self) -> Result<bool, String> {
        let recv_result = self
            .to_worker_rx
            .recv_timeout(Duration::from_secs(SHUTDOWN_POLL_INTERVAL));

        let request = match recv_result {
            Ok(r) => r,
            Err(e) => {
                match e {
                    // Timeouts are expected.
                    std::sync::mpsc::RecvTimeoutError::Timeout => return Ok(false),
                    // Other errors are not.
                    _ => return Err(format!("Error receiving request from parent: {e}")),
                }
            }
        };

        // NOTE no need to report our status as Active to the parent
        // server, since it applies the Active state to this worker's
        // metadata just before sending us this request.

        if let Err(e) = self.handler.process(request) {
            // This is not necessarily an existential crisis, probably
            // just a malformed request, etc.
            log::error!("{self} error processing request: {e}");
        }

        Ok(true)
    }
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Worker id={} requests={}",
            self.worker_id, self.request_count
        )
    }
}
