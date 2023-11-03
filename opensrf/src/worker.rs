use super::addr::BusAddress;
use super::app;
use super::client::{Client, ClientSingleton};
use super::conf;
use super::message;
use super::message::Message;
use super::message::MessageStatus;
use super::message::MessageType;
use super::message::Payload;
use super::message::TransportMessage;
use super::method;
use super::method::ParamCount;
use super::sclient::HostSettings;
use super::session::ServerSession;
use std::cell::RefMut;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
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
    Done,
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

    config: Arc<conf::Config>,

    stopping: Arc<AtomicBool>,

    // Settings from opensrf.settings
    host_settings: Arc<HostSettings>,

    client: Client,

    // True if the caller has requested a stateful conversation.
    connected: bool,

    methods: Arc<HashMap<String, method::Method>>,

    // Currently active session.
    // A worker can only have one active session at a time.
    // For stateless requests, each new thread results in a new session.
    // Starting a new thread/session in a stateful conversation
    // results in an error.
    session: Option<ServerSession>,

    worker_id: u64,

    // Channel for sending worker state info to our parent.
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
        config: Arc<conf::Config>,
        host_settings: Arc<HostSettings>,
        stopping: Arc<AtomicBool>,
        methods: Arc<HashMap<String, method::Method>>,
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    ) -> Result<Worker, String> {
        let client = Client::connect(config.clone())?;

        Ok(Worker {
            config,
            host_settings,
            stopping,
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

    pub fn create_app_worker(
        &mut self,
        factory: app::ApplicationWorkerFactory,
        env: Box<dyn app::ApplicationEnv>,
    ) -> Result<Box<dyn app::ApplicationWorker>, String> {
        let mut app_worker = (factory)();
        app_worker.absorb_env(
            self.client.clone(),
            self.config.clone(),
            self.host_settings.clone(),
            self.methods.clone(),
            env,
        )?;
        Ok(app_worker)
    }

    pub fn listen(&mut self, mut appworker: Box<dyn app::ApplicationWorker>) {
        let selfstr = format!("{self}");

        if let Err(e) = appworker.worker_start() {
            log::error!("{selfstr} worker_start failed {e}.  Exiting");
            return;
        }

        let max_requests: u32 = self
            .host_settings
            .value(&format!("apps/{}/unix_config/max_requests", self.service))
            .as_u32()
            .unwrap_or(5000);

        let keepalive: i32 = self
            .host_settings
            .value(&format!("apps/{}/unix_config/keepalive", self.service))
            .as_i32()
            .unwrap_or(5);

        let mut requests: u32 = 0;

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
                timeout = keepalive;
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
                match self.handle_recv(&mut appworker, timeout, sent_to) {
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
                if let Err(e) = appworker.end_session() {
                    log::error!("end_session() returned an error: {e}");
                    break;
                }

                if self.set_idle().is_err() {
                    break;
                }

                // Increment our message handled count if a message
                // was handled.  Each connected session counts as 1
                // "request".
                if msg_handled {
                    requests += 1;
                }
            } else {
                // Let the worker know we woke up and nothing interesting
                // happened.
                if let Err(e) = appworker.worker_idle_wake(self.connected) {
                    log::error!("worker_idle_wake() returned an error: {e}");
                    break;
                }
            }

            // Did we get a shutdown signal?  Check this after
            // "end_session()" so we don't interrupt a conversation to
            // shutdown.
            if self.stopping.load(Ordering::Relaxed) {
                log::info!("{selfstr} received a stop signal");
                break;
            }
        }

        log::trace!("{self} exiting listen loop");

        if let Err(e) = appworker.worker_end() {
            log::error!("{selfstr} worker_end failed {e}");
        }

        self.notify_state(WorkerState::Done).ok(); // ignore errors

        // Clear our worker-specific bus address of any lingering data.
        self.reset().ok();
    }

    /// Call recv() on our message bus and process the response.
    ///
    /// Return value consists of (work_occurred, msg_handled).
    fn handle_recv(
        &mut self,
        appworker: &mut Box<dyn app::ApplicationWorker>,
        timeout: i32,
        sent_to: &str,
    ) -> Result<(bool, bool), String> {
        let selfstr = format!("{self}");

        let recv_result = self
            .client_internal_mut()
            .bus_mut()
            .recv(timeout, Some(sent_to));

        let msg_op = match recv_result {
            Ok(o) => o,
            Err(ref e) => {
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

                self.set_active()?;

                if let Err(e) = self.reply_with_status(MessageStatus::Timeout, "Timeout") {
                    Err(format!("server: could not reply with Timeout message: {e}"))?;
                }

                return Ok((true, false)); // work occurred
            }
        };

        self.set_active()?;

        if !self.connected {
            // Any message received in a non-connected state represents
            // the start of a session.  For stateful convos, the
            // current message will be a CONNECT.  Otherwise, it will
            // be a one-off request.
            appworker.start_session()?;
        }

        if let Err(e) = self.handle_transport_message(&tmsg, appworker) {
            // An error within our worker's method handler is not enough
            // to shut down the worker.  Log, force a disconnect on the
            // session (if applicable) and move on.
            log::error!("{selfstr} error handling message: {e}");
            self.connected = false;
        }

        Ok((true, true)) // work occurred, message handled
    }

    fn set_active(&mut self) -> Result<(), String> {
        if let Err(e) = self.notify_state(WorkerState::Active) {
            Err(format!(
                "{self} failed to notify parent of Active state. Exiting. {e}"
            ))?;
        }

        Ok(())
    }

    fn set_idle(&mut self) -> Result<(), String> {
        if let Err(e) = self.notify_state(WorkerState::Idle) {
            Err(format!(
                "{self} failed to notify parent of Idle state. Exiting. {e}"
            ))?;
        }

        Ok(())
    }

    fn handle_transport_message(
        &mut self,
        tmsg: &message::TransportMessage,
        appworker: &mut Box<dyn app::ApplicationWorker>,
    ) -> Result<(), String> {
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

        for msg in tmsg.body().iter() {
            self.handle_message(msg, appworker)?;
        }

        Ok(())
    }

    // Clear our local message bus and reset state maintenance values.
    fn reset(&mut self) -> Result<(), String> {
        self.connected = false;
        self.session = None;
        self.client.clear()
    }

    fn handle_message(
        &mut self,
        msg: &message::Message,
        appworker: &mut Box<dyn app::ApplicationWorker>,
    ) -> Result<(), String> {
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
                self.handle_request(msg, appworker)
            }

            _ => self.reply_bad_request("Unexpected message type"),
        }
    }

    fn reply_with_status(&mut self, stat: MessageStatus, stat_text: &str) -> Result<(), String> {
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
            .send(&tmsg)
    }

    fn handle_request(
        &mut self,
        msg: &message::Message,
        appworker: &mut Box<dyn app::ApplicationWorker>,
    ) -> Result<(), String> {
        let request = match msg.payload() {
            message::Payload::Method(m) => m,
            _ => return self.reply_bad_request("Request sent without payload"),
        };

        let mut log_params: Option<String> = None;

        if self
            .config
            .log_protect()
            .iter()
            .filter(|m| request.method().starts_with(&m[..]))
            .next()
            .is_none()
        {
            log_params = Some(
                request
                    .params()
                    .iter()
                    .map(|p| p.dump())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        };

        let log_params = log_params.as_deref().unwrap_or("**PARAMS REDACTED**");

        // Log the API call
        log::info!("CALL: {} {}", request.method(), log_params);

        // Before we begin processing a service-level request, clear our
        // local message bus to avoid encountering any stale messages
        // lingering from the previous conversation.
        if !self.connected {
            self.client.clear()?;
        }

        let method = match self.methods.get(request.method()) {
            Some(m) => {
                // Clone the method since we have mutable borrows below.
                // Note this is the method definition, not the request,
                // which may have non-clone-friendly bulky parameters.
                m.clone()
            }
            None => {
                log::warn!("Method not found: {}", request.method());
                return self.reply_with_status(
                    MessageStatus::MethodNotFound,
                    &format!("Method not found: {}", request.method()),
                );
            }
        };

        if method.atomic() {
            self.session_mut().new_atomic_resp_queue();
        }

        let pcount = method.param_count();

        // Make sure the number of params sent by the caller matches the
        // parameter count for the method.
        if !ParamCount::matches(&pcount, request.params().len() as u8) {
            return self.reply_bad_request(&format!(
                "Invalid param count sent: method={} sent={} needed={}",
                request.method(),
                request.params().len(),
                &pcount,
            ));
        }

        // De-serialize the inbound parameters.
        let mut unpacked_params = Vec::new();
        if let Some(s) = self.client.singleton().borrow().serializer() {
            for p in request.params() {
                // TODO if these are unpacked at message create time,
                // we could avoid the clone.  Not a huge deal since
                // inbound params are typically small, but still.
                //
                // TODO we could verify at this point whether each
                // paramater matches its documented ParamDataType.
                unpacked_params.push(s.unpack(p.clone()));
            }
        }
        let request = message::Method::new(request.method(), unpacked_params);

        if let Err(ref err) = (method.handler())(appworker, self.session_mut(), &request) {
            let msg = format!("{self} method {} failed with {err}", request.method());
            log::error!("{msg}");
            appworker.api_call_error(&request, err);
            self.reply_server_error(&msg)?;
            Err(msg)?;
        }

        if !self.session().responded_complete() {
            self.session_mut().send_complete()
        } else {
            Ok(())
        }
    }

    fn reply_server_error(&mut self, text: &str) -> Result<(), String> {
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
            .send(&tmsg)
    }

    fn reply_bad_request(&mut self, text: &str) -> Result<(), String> {
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
            .send(&tmsg)
    }

    /// Notify the parent process of this worker's active state.
    fn notify_state(&self, state: WorkerState) -> Result<(), mpsc::SendError<WorkerStateEvent>> {
        log::trace!("{self} notifying parent of state change => {state:?}");

        self.to_parent_tx.send(WorkerStateEvent {
            worker_id: self.worker_id(),
            state: state,
        })
    }
}
