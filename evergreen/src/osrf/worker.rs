use crate::osrf::addr::BusAddress;
use crate::osrf::app;
use crate::osrf::client::{Client, ClientSingleton};
use crate::osrf::conf;
use crate::osrf::logging::Logger;
use crate::osrf::message;
use crate::osrf::message::Message;
use crate::osrf::message::MessageStatus;
use crate::osrf::message::MessageType;
use crate::osrf::message::Payload;
use crate::osrf::message::TransportMessage;
use crate::osrf::method;
use crate::osrf::method::ParamCount;
use crate::osrf::sclient::HostSettings;
use crate::osrf::session::ServerSession;
use crate::util;
use crate::EgResult;
use mptc::signals::SignalTracker;
use std::cell::RefMut;
use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time;

// How often each worker wakes to check for shutdown signals, etc.
const IDLE_WAKE_TIME: i32 = 5;

/// Each worker thread is in one of these states.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum WorkerState {
    Idle,
    Active,
    Exiting,
}

#[derive(Debug)]
pub struct WorkerStateEvent {
    pub worker_id: u64,
    pub state: WorkerState,
}

impl WorkerStateEvent {
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
    pub fn state(&self) -> WorkerState {
        self.state
    }
}

/// A Worker runs in its own thread and responds to API requests.
pub struct Worker {
    service: String,

    /// Watches for signals
    sig_tracker: SignalTracker,

    client: Client,

    /// True if the caller has requested a stateful conversation.
    connected: bool,

    methods: Arc<HashMap<String, method::MethodDef>>,

    /// Currently active session.
    /// A worker can only have one active session at a time.
    /// For stateless requests, each new thread results in a new session.
    /// Starting a new thread/session in a stateful conversation
    /// results in an error.
    session: Option<ServerSession>,

    /// Unique ID for tracking/logging each working.
    worker_id: u64,

    /// Channel for sending worker state info to our parent.
    to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Worker ({})", self.worker_id)
    }
}

impl Worker {
    pub fn new(
        service: String,
        worker_id: u64,
        sig_tracker: SignalTracker,
        methods: Arc<HashMap<String, method::MethodDef>>,
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    ) -> EgResult<Worker> {
        let client = Client::connect()?;

        Ok(Worker {
            sig_tracker,
            service,
            worker_id,
            methods,
            client,
            to_parent_tx,
            session: None,
            connected: false,
        })
    }

    /// Mutable Ref to our under-the-covers client singleton.
    fn client_internal_mut(&self) -> RefMut<ClientSingleton> {
        self.client.singleton().borrow_mut()
    }

    /// Current session
    ///
    /// Panics of session on None.
    fn session(&self) -> &ServerSession {
        self.session.as_ref().unwrap()
    }

    fn session_mut(&mut self) -> &mut ServerSession {
        self.session.as_mut().unwrap()
    }

    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// Wait for and process inbound API calls.
    pub fn listen(&mut self, factory: app::ApplicationWorkerFactory) {
        let selfstr = format!("{self}");

        let mut app_worker = (factory)();

        if let Err(e) = app_worker.worker_start(self.client.clone(), self.methods.clone()) {
            log::error!("{selfstr} worker_start failed {e}.  Exiting");
            return;
        }

        let max_requests: usize =
            HostSettings::get(&format!("apps/{}/unix_config/max_requests", self.service))
                .expect("Host Settings Not Retrieved")
                .as_usize()
                .unwrap_or(5000);

        let keepalive: usize =
            HostSettings::get(&format!("apps/{}/unix_config/keepalive", self.service))
                .expect("Host Settings Not Retrieved")
                .as_usize()
                .unwrap_or(5);

        let mut requests: usize = 0;

        // We listen for API calls at an addressed scoped to our
        // username and domain.
        let username = self.client.address().username();
        let domain = self.client.address().domain();

        let service_addr = BusAddress::for_service(username, domain, &self.service);
        let service_addr = service_addr.as_str().to_string();

        let my_addr = self.client.address().as_str().to_string();

        while requests < max_requests {
            let timeout: i32;
            let sent_to: &str;

            if self.connected {
                // We're in the middle of a stateful conversation.
                // Listen for messages sent specifically to our bus
                // address and only wait up to keeplive seconds for
                // subsequent messages.
                sent_to = &my_addr;
                timeout = keepalive as i32;
            } else {
                // If we are not within a stateful conversation, clear
                // our bus data and message backlogs since any remaining
                // data is no longer relevant.
                if let Err(e) = self.reset() {
                    log::error!("{selfstr} could not reset {e}.  Exiting");
                    break;
                }

                sent_to = &service_addr;
                timeout = IDLE_WAKE_TIME;
            }

            // work_occurred will be true if we handled a message or
            // had to address a stateful session timeout.
            let (work_occurred, msg_handled) =
                match self.handle_recv(&mut app_worker, timeout, sent_to) {
                    Ok(w) => w,
                    Err(e) => {
                        log::error!("Error in main loop error: {e}");
                        break;
                    }
                };

            // If we are connected, we remain Active and avoid counting
            // subsequent requests within this stateful converstation
            // toward our overall request count.
            if self.connected {
                continue;
            }

            if work_occurred {
                // also true if msg_handled

                // If we performed any work and we are outside of a
                // keepalive loop, let our worker know a stateless
                // request or stateful conversation has just completed.
                if let Err(e) = app_worker.end_session() {
                    log::error!("end_session() returned an error: {e}");
                    break;
                }

                if self.set_idle().is_err() {
                    break;
                }

                if msg_handled {
                    // Increment our message handled count.
                    // Each connected session counts as 1 "request".
                    requests += 1;

                    // An inbound message may have modified our
                    // thread-scoped locale.  Reset our locale back
                    // to the default so the previous locale does not
                    // affect future messages.
                    message::reset_thread_locale();
                }
            } else {
                // Let the worker know we woke up and nothing interesting
                // happened.
                if let Err(e) = app_worker.worker_idle_wake(self.connected) {
                    log::error!("worker_idle_wake() returned an error: {e}");
                    break;
                }
            }

            // Did we get a shutdown signal?  Check this after
            // "end_session()" so we don't interrupt a conversation to
            // shutdown.
            if self.sig_tracker.any_shutdown_requested() {
                log::info!("{selfstr} received a stop signal");
                break;
            }
        }

        log::debug!("{self} exiting listen loop and cleaning up");

        if let Err(e) = app_worker.worker_end() {
            log::error!("{selfstr} worker_end failed {e}");
        }

        self.notify_state(WorkerState::Exiting).ok(); // ignore errors

        // Clear our worker-specific bus address of any lingering data.
        self.reset().ok();
    }

    /// Call recv() on our message bus and process the response.
    ///
    /// Return value consists of (work_occurred, msg_handled).
    fn handle_recv(
        &mut self,
        app_worker: &mut Box<dyn app::ApplicationWorker>,
        timeout: i32,
        sent_to: &str,
    ) -> EgResult<(bool, bool)> {
        let selfstr = format!("{self}");

        let recv_result = self
            .client_internal_mut()
            .bus_mut()
            .recv(timeout, Some(sent_to));

        let msg_op = match recv_result {
            Ok(o) => o,
            Err(e) => {
                // There's a good chance an error in recv() means the
                // thread/system is unusable, so let the worker exit.
                //
                // Avoid a tight thread respawn loop with a short pause.
                thread::sleep(time::Duration::from_secs(1));
                Err(e)?
            }
        };

        let tmsg = match msg_op {
            Some(v) => v,
            None => {
                if !self.connected {
                    // No new message to handle and no timeout to address.
                    return Ok((false, false));
                }

                // Caller failed to send a message within the keepliave interval.
                log::warn!("{selfstr} timeout waiting on request while connected");

                if let Err(e) = self.reply_with_status(MessageStatus::Timeout, "Timeout") {
                    Err(format!("server: could not reply with Timeout message: {e}"))?;
                }

                self.set_active()?;

                return Ok((true, false)); // work occurred
            }
        };

        self.set_active()?;

        if !self.connected {
            // Any message received in a non-connected state represents
            // the start of a session.  For stateful convos, the
            // current message will be a CONNECT.  Otherwise, it will
            // be a one-off request.
            app_worker.start_session()?;
        }

        if let Err(e) = self.handle_transport_message(tmsg, app_worker) {
            // An error within our worker's method handler is not enough
            // to shut down the worker.  Log, force a disconnect on the
            // session (if applicable) and move on.
            log::error!("{selfstr} error handling message: {e}");
            self.connected = false;
        }

        Ok((true, true)) // work occurred, message handled
    }

    /// Tell our parent we're about to perform some work.
    fn set_active(&mut self) -> EgResult<()> {
        if let Err(e) = self.notify_state(WorkerState::Active) {
            Err(format!(
                "{self} failed to notify parent of Active state. Exiting. {e}"
            ))?;
        }

        Ok(())
    }

    /// Tell our parent we're available to perform work.
    fn set_idle(&mut self) -> EgResult<()> {
        if let Err(e) = self.notify_state(WorkerState::Idle) {
            Err(format!(
                "{self} failed to notify parent of Idle state. Exiting. {e}"
            ))?;
        }

        Ok(())
    }

    fn handle_transport_message(
        &mut self,
        mut tmsg: message::TransportMessage,
        app_worker: &mut Box<dyn app::ApplicationWorker>,
    ) -> EgResult<()> {
        // Always adopt the log trace of an inbound API call.
        Logger::set_log_trace(tmsg.osrf_xid());

        if self.session.is_none() || self.session().thread().ne(tmsg.thread()) {
            log::trace!("server: creating new server session for {}", tmsg.thread());

            self.session = Some(ServerSession::new(
                self.client.clone(),
                &self.service,
                tmsg.thread(),
                0, // thread trace -- updated later as needed
                BusAddress::from_str(tmsg.from())?,
            ));
        }

        for msg in tmsg.body_mut().drain(..) {
            self.handle_message(msg, app_worker)?;
        }

        Ok(())
    }

    // Clear our local message bus and reset state maintenance values.
    fn reset(&mut self) -> EgResult<()> {
        self.connected = false;
        self.session = None;
        self.client.clear()
    }

    fn handle_message(
        &mut self,
        msg: message::Message,
        app_worker: &mut Box<dyn app::ApplicationWorker>,
    ) -> EgResult<()> {
        self.session_mut().set_last_thread_trace(msg.thread_trace());
        self.session_mut().clear_responded_complete();

        log::trace!("{self} received message of type {:?}", msg.mtype());

        match msg.mtype() {
            message::MessageType::Disconnect => {
                log::trace!("{self} received a DISCONNECT");
                self.reset()?;
                Ok(())
            }

            message::MessageType::Connect => {
                log::trace!("{self} received a CONNECT");

                if self.connected {
                    return self.reply_bad_request("Worker is already connected");
                }

                self.connected = true;
                self.reply_with_status(MessageStatus::Ok, "OK")
            }

            message::MessageType::Request => {
                log::trace!("{self} received a REQUEST");
                self.handle_request(msg, app_worker)
            }

            _ => self.reply_bad_request("Unexpected message type"),
        }
    }

    fn reply_with_status(&mut self, stat: MessageStatus, stat_text: &str) -> EgResult<()> {
        let tmsg = TransportMessage::with_body(
            self.session().sender().as_str(),
            self.client.address().as_str(),
            self.session().thread(),
            Message::new(
                MessageType::Status,
                self.session().last_thread_trace(),
                Payload::Status(message::Status::new(stat, stat_text, "osrfStatus")),
            ),
        );

        self.client_internal_mut()
            .get_domain_bus(self.session().sender().domain())?
            .send(tmsg)
    }

    fn handle_request(
        &mut self,
        mut msg: message::Message,
        app_worker: &mut Box<dyn app::ApplicationWorker>,
    ) -> EgResult<()> {
        let method_call = match msg.take_payload() {
            message::Payload::Method(m) => m,
            _ => return self.reply_bad_request("Request sent without a MethoCall payload"),
        };

        let param_count = method_call.params().len();
        let api_name = method_call.method().to_string();

        let log_params = util::stringify_params(
            &api_name,
            method_call.params(),
            conf::config().log_protect(),
        );

        // Log the API call
        log::info!("CALL: {} {}", api_name, log_params);

        // Before we begin processing a service-level request, clear our
        // local message bus to avoid encountering any stale messages
        // lingering from the previous conversation.
        if !self.connected {
            self.client.clear()?;
        }

        // Clone the method since we have mutable borrows below.  Note
        // this is the method definition, not the param-laden request.
        let mut method_def = self.methods.get(&api_name).map(|m| m.clone());

        if method_def.is_none() {
            // Atomic methods are not registered/published in advance
            // since every method has an atomic variant.
            // Find the root method and use it.
            if api_name.ends_with(".atomic") {
                let meth = api_name.replace(".atomic", "");
                if let Some(m) = self.methods.get(&meth) {
                    method_def = Some(m.clone());

                    // Creating a new queue tells our session to treat
                    // this as an atomic request.
                    self.session_mut().new_atomic_resp_queue();
                }
            }
        }

        if method_def.is_none() {
            log::warn!("Method not found: {}", api_name);

            return self.reply_with_status(
                MessageStatus::MethodNotFound,
                &format!("Method not found: {}", api_name),
            );
        }

        let method_def = method_def.unwrap();
        let pcount = method_def.param_count();

        // Make sure the number of params sent by the caller matches the
        // parameter count for the method.
        if !ParamCount::matches(&pcount, param_count as u8) {
            return self.reply_bad_request(&format!(
                "Invalid param count sent: method={} sent={} needed={}",
                api_name, param_count, &pcount,
            ));
        }

        // Verify paramter types are correct, at least superficially.
        // Do this after deserialization.
        if let Some(param_defs) = method_def.params() {
            for (idx, param_def) in param_defs.iter().enumerate() {
                // There may be more param defs than parameters if
                // some param are optional.
                if let Some(param_val) = method_call.params().get(idx) {
                    if idx >= pcount.minimum().into() && param_val.is_null() {
                        // NULL placeholders for non-required parameters are
                        // allowed.
                        continue;
                    }
                    if !param_def.datatype.matches(param_val) {
                        return self.reply_bad_request(&format!(
                            "Invalid paramter type: wanted={} got={}",
                            param_def.datatype,
                            param_val.clone().dump()
                        ));
                    }
                } else {
                    // More defs than actual params. Verification complete.
                    break;
                }
            }
        }

        // Call the API
        if let Err(err) = (method_def.handler())(app_worker, self.session_mut(), method_call) {
            let msg = format!("{self} method {api_name} exited: \"{err}\"");
            log::error!("{msg}");
            app_worker.api_call_error(&api_name, err);
            self.reply_server_error(&msg)?;
            Err(msg)?;
        }

        if !self.session().responded_complete() {
            self.session_mut().send_complete()
        } else {
            Ok(())
        }
    }

    fn reply_server_error(&mut self, text: &str) -> EgResult<()> {
        self.connected = false;

        let msg = Message::new(
            MessageType::Status,
            self.session().last_thread_trace(),
            Payload::Status(message::Status::new(
                MessageStatus::InternalServerError,
                &format!("Internal Server Error: {text}"),
                "osrfStatus",
            )),
        );

        let tmsg = TransportMessage::with_body(
            self.session().sender().as_str(),
            self.client.address().as_str(),
            self.session().thread(),
            msg,
        );

        self.client_internal_mut()
            .get_domain_bus(self.session().sender().domain())?
            .send(tmsg)
    }

    fn reply_bad_request(&mut self, text: &str) -> EgResult<()> {
        self.connected = false;

        let msg = Message::new(
            MessageType::Status,
            self.session().last_thread_trace(),
            Payload::Status(message::Status::new(
                MessageStatus::BadRequest,
                &format!("Bad Request: {text}"),
                "osrfStatus",
            )),
        );

        let tmsg = TransportMessage::with_body(
            self.session().sender().as_str(),
            self.client.address().as_str(),
            self.session().thread(),
            msg,
        );

        self.client_internal_mut()
            .get_domain_bus(self.session().sender().domain())?
            .send(tmsg)
    }

    /// Notify the parent process of this worker's active state.
    fn notify_state(&self, state: WorkerState) -> EgResult<()> {
        log::trace!("{self} notifying parent of state change => {state:?}");

        self.to_parent_tx
            .send(WorkerStateEvent {
                worker_id: self.worker_id(),
                state: state,
            })
            .map_err(|e| format!("mpsc::SendError: {e}").into())
    }
}
