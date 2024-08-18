use crate::init;
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
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static REGISTERED_METHODS: OnceLock<HashMap<String, method::MethodDef>> = OnceLock::new();

// How often each worker wakes to check for shutdown signals, etc.
const IDLE_WAKE_TIME: u64 = 5;

pub struct Microservice {
    application: Box<dyn app::Application>,

    /// Watches for signals
    sig_tracker: SignalTracker,

    /// OpenSRF bus connection
    client: Client,

    /// True if the caller has requested a stateful conversation.
    connected: bool,

    /// Currently active session.
    /// A worker can only have one active session at a time.
    /// For stateless requests, each new thread results in a new session.
    /// Starting a new thread/session in a stateful conversation
    /// results in an error.
    session: Option<ServerSession>,
}

impl fmt::Display for Microservice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Micro")
    }
}

impl Microservice {
    pub fn start(application: Box<dyn app::Application>) -> EgResult<()> {
        let mut options = init::InitOptions::new();
        options.appname = Some(application.name().to_string());

        let client = init::osrf_init(&options)?;

        let mut tracker = SignalTracker::new();

        tracker.track_graceful_shutdown();
        tracker.track_fast_shutdown();
        tracker.track_reload();

        let mut service = Microservice {
            application,
            client,
            sig_tracker: tracker,
            connected: false,
            session: None,
        };

        let client = service.client.clone();

        service.application.init(client)?;

        service.register_methods()?;

        service.register_routers()?;

        service.listen();

        service.unregister_routers()?;

        Ok(())
    }

    /// Mark ourselves as currently idle.
    ///
    /// This is a NO-OP for now, but may be useful in the future.
    fn set_idle(&mut self) -> EgResult<()> {
        Ok(())
    }

    /// Mark ourselves as currently active.
    ///
    /// This is a NO-OP for now, but may be useful in the future.
    fn set_active(&mut self) -> EgResult<()> {
        Ok(())
    }

    fn methods() -> &'static HashMap<String, method::MethodDef> {
        if let Some(h) = REGISTERED_METHODS.get() {
            h
        } else {
            log::error!("Cannot call methods() prior to registration");
            panic!("Cannot call methods() prior to registration");
        }
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

    /// Wait for and process inbound API calls.
    fn listen(&mut self) {
        let factory = self.application.worker_factory();
        let mut app_worker = (factory)();

        if let Err(e) = app_worker.worker_start(self.client.clone()) {
            log::error!("worker_start failed {e}.  Exiting");
            return;
        }

        let max_requests: usize = HostSettings::get(&format!(
            "apps/{}/unix_config/max_requests",
            self.application.name()
        ))
        .expect("Host Settings Not Retrieved")
        .as_usize()
        .unwrap_or(5000);

        let keepalive = HostSettings::get(&format!(
            "apps/{}/unix_config/keepalive",
            self.application.name()
        ))
        .expect("Host Settings Not Retrieved")
        .as_u64()
        .unwrap_or(5);

        let mut requests: usize = 0;

        // We listen for API calls at an addressed scoped to our
        // username and domain.
        let username = self.client.address().username();
        let domain = self.client.address().domain();

        let service_addr = BusAddress::for_service(username, domain, self.application.name());
        let service_addr = service_addr.as_str().to_string();

        let my_addr = self.client.address().as_str().to_string();

        while requests < max_requests {
            let timeout: u64;
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
                    log::error!("could not reset {e}.  Exiting");
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
                log::info!("received a stop signal");
                break;
            }
        }

        log::debug!("{self} exiting listen loop and cleaning up");

        if let Err(e) = app_worker.worker_end() {
            log::error!("worker_end failed {e}");
        }

        // Clear our worker-specific bus address of any lingering data.
        self.reset().ok();
    }

    /// Call recv() on our message bus and process the response.
    ///
    /// Return value consists of (work_occurred, msg_handled).
    fn handle_recv(
        &mut self,
        app_worker: &mut Box<dyn app::ApplicationWorker>,
        timeout: u64,
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
                thread::sleep(Duration::from_secs(1));
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

                self.set_active()?;

                // Caller failed to send a message within the keepliave interval.
                log::warn!("{selfstr} timeout waiting on request while connected");

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
                self.application.name(),
                tmsg.thread(),
                0, // thread trace -- updated later as needed
                BusAddress::parse_str(tmsg.from())?,
            ));
        }

        for msg in tmsg.body_mut().drain(..) {
            self.handle_message(msg, app_worker)?;
        }

        Ok(())
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
        let mut method_def = Microservice::methods().get(&api_name).cloned();

        if method_def.is_none() {
            // Atomic methods are not registered/published in advance
            // since every method has an atomic variant.
            // Find the root method and use it.
            if api_name.ends_with(".atomic") {
                let meth = api_name.replace(".atomic", "");
                if let Some(m) = Microservice::methods().get(&meth) {
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
        if !ParamCount::matches(pcount, param_count as u8) {
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

    fn register_methods(&mut self) -> EgResult<()> {
        let client = self.client.clone();
        let list = self.application.register_methods(client)?;
        let mut hash = HashMap::new();
        for m in list {
            hash.insert(m.name().to_string(), m);
        }
        self.add_system_methods(&mut hash);

        if REGISTERED_METHODS.set(hash).is_err() {
            return Err("Cannot call register_methods() more than once".into());
        }

        Ok(())
    }

    fn add_system_methods(&mut self, hash: &mut HashMap<String, method::MethodDef>) {
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

    /// List of domains where our service is allowed to run and
    /// therefore whose routers with whom our presence should be registered.
    fn hosting_domains(&self) -> Vec<(String, String)> {
        let mut domains: Vec<(String, String)> = Vec::new();
        for router in conf::config().client().routers() {
            match router.services() {
                Some(services) => {
                    if services.iter().any(|s| s.eq(self.application.name())) {
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

            self.client.send_router_command(
                username,
                domain,
                "register",
                Some(self.application.name()),
            )?;
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
                Some(self.application.name()),
            )?;
        }
        Ok(())
    }

    // Clear our local message bus and reset state maintenance values.
    fn reset(&mut self) -> EgResult<()> {
        self.connected = false;
        self.session = None;
        self.client.clear()
    }
}

// Toss our system method handlers down here.
fn system_method_echo(
    _worker: &mut Box<dyn app::ApplicationWorker>,
    session: &mut ServerSession,
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
    session: &mut ServerSession,
    _method: message::MethodCall,
) -> EgResult<()> {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => session.respond_complete(t.as_secs()),
        Err(e) => Err(format!("System time error: {e}").into()),
    }
}

fn system_method_introspect(
    _worker: &mut Box<dyn app::ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let prefix = match method.params().first() {
        Some(p) => p.as_str(),
        None => None,
    };

    // Collect the names first so we can sort them
    let mut names: Vec<&str> = match prefix {
        // If a prefix string is provided, only return methods whose
        // name starts with the provided prefix.
        Some(pfx) => Microservice::methods()
            .keys()
            .filter(|n| n.starts_with(pfx))
            .map(|n| n.as_str())
            .collect(),
        None => Microservice::methods().keys().map(|n| n.as_str()).collect(),
    };

    names.sort();

    for name in names {
        if let Some(meth) = Microservice::methods().get(name) {
            if method.method().contains("summary") {
                session.respond(meth.to_summary_string())?;
            } else {
                session.respond(meth.to_eg_value())?;
            }
        }
    }

    Ok(())
}
