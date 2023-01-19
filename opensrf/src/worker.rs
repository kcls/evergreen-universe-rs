use super::addr::{ClientAddress, ServiceAddress};
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
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time;

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
        methods: Arc<HashMap<String, method::Method>>,
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
    ) -> Result<Worker, String> {
        let client = Client::connect(config.clone())?;

        Ok(Worker {
            config,
            host_settings,
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
        let service_addr = ServiceAddress::new(&self.service).full().to_string();
        let local_addr = self.client.address().full().to_string();

        while requests < max_requests {
            let timeout: i32;
            let sent_to: &str;

            if self.connected {
                // We're in the middle of a stateful conversation.
                // Listen for messages sent specifically to our bus
                // address and only wait up to keeplive seconds for
                // subsequent messages.
                sent_to = &local_addr;
                timeout = keepalive;
            } else {
                if let Err(e) = self.reset() {
                    log::error!("{selfstr} could not reset {e}.  Exiting");
                    break;
                }

                // Wait indefinitely for top-level service messages.
                sent_to = &service_addr;
                timeout = -1;
            }

            log::trace!(
                "{selfstr} calling recv() timeout={} sent_to={}",
                timeout,
                sent_to
            );

            let recv_op = self
                .client_internal_mut()
                .bus_mut()
                .recv(timeout, Some(sent_to));

            if let Err(e) = self.notify_state(WorkerState::Active) {
                log::error!("{self} failed to notify parent of Active state. Exiting. {e}");
                break;
            }

            match recv_op {
                Err(e) => {
                    log::error!("{selfstr} recv() in listen produced an error: {e}");

                    // If an error occurs here, we can get stuck in a tight
                    // loop that's hard to break.  Add a sleep so we can
                    // more easily control-c out of the loop.
                    thread::sleep(time::Duration::from_secs(1));
                    self.connected = false;
                }

                Ok(recv_op) => {
                    match recv_op {
                        None => {
                            // See if the caller failed to send a follow-up
                            // request within the keepalive timeout.
                            if self.connected {
                                log::warn!("{selfstr} timeout waiting on request while connected");
                                self.connected = false;
                                if let Err(e) =
                                    self.reply_with_status(MessageStatus::Timeout, "Timeout")
                                {
                                    log::error!(
                                        "server: could not reply with Timeout message: {e}"
                                    );
                                }
                            }
                        }

                        Some(msg) => {
                            if let Err(e) = self.handle_transport_message(&msg, &mut appworker) {
                                log::error!("{selfstr} error handling message: {e}");
                                self.connected = false;
                            }
                        }
                    }
                }
            }

            // If we are connected, we remain Active and avoid counting
            // subsequent requests within this stateful converstation
            // toward our overall request count.
            if self.connected {
                continue;
            }

            if let Err(e) = self.notify_state(WorkerState::Idle) {
                // If we can't notify our parent, it means the parent
                // thread is no longer running.  Get outta here.
                log::error!("{selfstr} could not notify parent of Idle state. Exiting. {e}");
                break;
            }

            requests += 1;
        }

        log::trace!("{self} exiting on max requests or early break");

        if let Err(_) = self.notify_state(WorkerState::Done) {
            log::error!("{self} failed to notify parent of Done state");
        }

        if let Err(e) = appworker.worker_end() {
            log::error!("{selfstr} worker_start failed {e}.  Exiting");
            return;
        }
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
                ClientAddress::from_string(tmsg.from())?,
            ));
        }

        for msg in tmsg.body().iter() {
            self.handle_message(&msg, appworker)?;
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
            self.session().sender().full(),
            self.client.address().full(),
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
            _ => {
                return self.reply_bad_request("Request sent without payload");
            }
        };

        let logp = request
            .params()
            .iter()
            .map(|p| p.dump())
            .collect::<Vec<_>>()
            .join(", ");

        // Log the API call
        log::debug!("CALL: {} {}", request.method(), logp);

        // Before we begin processing a service-level request, clear our
        // local message bus to avoid encountering any stale messages
        // lingering from the previous conversation.
        if !self.connected {
            self.client.clear()?;
        }

        let method = match self.methods.get(request.method()) {
            Some(m) => m,
            None => {
                return self.reply_with_status(
                    MessageStatus::MethodNotFound,
                    &format!("Method not found: {}", request.method()),
                );
            }
        };

        let pcount = method.param_count().clone();

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

        if let Err(err) = (method.handler())(appworker, self.session_mut(), &request) {
            // TODO reply internal server error
            return Err(format!(
                "{self} method {} failed with {err}",
                request.method()
            ));
        }

        if !self.session().responded_complete() {
            self.session_mut().send_complete()
        } else {
            Ok(())
        }
    }

    fn reply_bad_request(&mut self, text: &str) -> Result<(), String> {
        self.connected = false;

        let tmsg = TransportMessage::with_body(
            self.session().sender().full(),
            self.client.address().full(),
            self.session().thread(),
            Message::new(
                MessageType::Status,
                self.session().last_thread_trace(),
                Payload::Status(message::Status::new(
                    MessageStatus::BadRequest,
                    &format!("Bad Request: {text}"),
                    "osrfStatus",
                )),
            ),
        );

        self.client
            .singleton()
            .borrow_mut()
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
