use super::{Request, RequestHandler};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum WorkerState {
    Idle,
    Active,
    Done,
}

impl From<&WorkerState> for &'static str {
    fn from(e: &WorkerState) -> &'static str {
        match e {
            &WorkerState::Idle => "Idle",
            &WorkerState::Active => "Active",
            &WorkerState::Done => "Done",
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

    /// Channel for sending request data to a specific worker.
    /// TODO String will be some other type/trait.
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
    to_parent_tx: mpsc::Sender<WorkerStateEvent>,
    to_worker_rx: mpsc::Receiver<Box<dyn Request>>,
    shutdown: Arc<AtomicBool>,
    handler: Box<dyn RequestHandler>,
}

impl Worker {
    pub fn new(
        worker_id: u64,
        max_requests: usize,
        to_parent_tx: mpsc::Sender<WorkerStateEvent>,
        to_worker_rx: mpsc::Receiver<Box<dyn Request>>,
        shutdown: Arc<AtomicBool>,
        handler: Box<dyn RequestHandler>,
    ) -> Worker {
        Worker {
            worker_id,
            max_requests,
            to_parent_tx,
            to_worker_rx,
            shutdown,
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
            state: state,
        };

        if let Err(e) = self.to_parent_tx.send(evt) {
            Err(format!("Error notifying parent of state change: {e}"))
        } else {
            Ok(())
        }
    }

    pub fn run(&mut self) {
        log::trace!("{self} starting");

        self.handler.thread_start().unwrap(); // TODO

        while self.request_count < self.max_requests {
            self.request_count += 1;

            if let Err(e) = self.set_as_idle() {
                log::debug!("{self} exiting on set_as_idle() failure: {e}");
                break;
            }

            match self.process_one_request() {
                Ok(shutdown) => {
                    if shutdown {
                        log::debug!("{self} exiting listen loop on shutdown");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("{self} Request failed: {e}");
                    break;
                }
            }
        }

        log::debug!("{self} exiting on shutdown / max requests");

        self.set_as_done().ok(); // we're done.  ignore errors.

        if let Err(e) = self.handler.thread_end() {
            log::error!("{self} handler returned on error on exit: {e}");
        }
    }

    /// Returns result of true of this worker should exit.
    fn process_one_request(&mut self) -> Result<bool, String> {
        let duration = Duration::from_secs(super::SIGNAL_POLL_INTERVAL);
        let request;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                log::debug!("We received a stop signal, exiting");
                return Ok(true);
            }

            request = match self.to_worker_rx.recv_timeout(duration) {
                Ok(r) => r,
                Err(e) => match e {
                    mpsc::RecvTimeoutError::Timeout => continue,
                    _ => return Err(format!("{self} exiting on failed receive: {e}")),
                },
            };

            break;
        }

        self.handler.process(request)?;

        Ok(false)
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
