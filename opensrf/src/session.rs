use super::addr::BusAddress;
use super::client::{Client, ClientSingleton};
use super::message;
use super::message::Message;
use super::message::MessageStatus;
use super::message::MessageType;
use super::message::Method;
use super::message::Payload;
use super::message::Status;
use super::message::TransportMessage;
use super::params::ApiParams;
use super::util;
use json::JsonValue;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;

const CONNECT_TIMEOUT: i32 = 10;
pub const DEFAULT_REQUEST_TIMEOUT: i32 = 60;

/// Response data propagated from a session to the calling Request.
#[derive(Debug)]
struct Response {
    /// Response from an API call as a JsonValue.
    value: Option<JsonValue>,
    /// True if our originating Request is complete.
    complete: bool,
    /// True if this is a partial response
    partial: bool,
}

/// Models a single API call through which the caller can receive responses.
#[derive(Clone)]
pub struct Request {
    /// Link to our session so we can ask it for bus data.
    session: Rc<RefCell<Session>>,

    /// Have we received all of the replies yet?
    complete: bool,

    /// Unique ID per thread/session.
    thread_trace: usize,

    /// Having a local copy of the thread can be handy since our
    /// session is only accessible via temporary borrow().
    thread: String,
}

impl Request {
    fn new(thread: String, session: Rc<RefCell<Session>>, thread_trace: usize) -> Request {
        Request {
            session,
            thread,
            complete: false,
            thread_trace,
        }
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }

    pub fn thread_trace(&self) -> usize {
        self.thread_trace
    }

    /// True if we have received a COMPLETE message from the server.
    ///
    /// This does not guarantee all responses have been read.
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// True if we have received a COMPLETE message from the server
    /// and all responses from our network backlog have been read.
    ///
    /// It's possible to read the COMPLETE message before the caller
    /// pulls all the data.
    pub fn exhausted(&self) -> bool {
        self.complete() && self.session.borrow().backlog.is_empty()
    }

    /// Pull all responses from the bus and return the first.
    ///
    /// Handy if you are expecting exactly one result, or only care
    /// about the first, but want to pull all data off the bus until the
    /// message is officially marked as complete.
    pub fn first(&mut self) -> Result<Option<JsonValue>, String> {
        self.first_with_timeout(DEFAULT_REQUEST_TIMEOUT)
    }

    pub fn first_with_timeout(&mut self, timeout: i32) -> Result<Option<JsonValue>, String> {
        let mut resp: Option<JsonValue> = None;
        while !self.complete {
            if let Some(r) = self.recv_with_timeout(timeout)? {
                if resp.is_none() {
                    resp = Some(r);
                } // else discard the non-first response.
            }
        }

        Ok(resp)
    }

    /// Receive the next response to this Request
    ///
    /// timeout:
    ///     <0 == wait indefinitely
    ///      0 == do not wait/block
    ///     >0 == wait up to this many seconds for a reply.
    pub fn recv_with_timeout(&mut self, mut timeout: i32) -> Result<Option<JsonValue>, String> {
        if self.complete {
            // If we are marked complete, we've pulled all of our
            // resposnes from the bus.  However, we could still have
            // data in the session backlog.
            timeout = 0;
        }

        loop {
            let response = self.session.borrow_mut().recv(self.thread_trace, timeout)?;

            if let Some(r) = response {
                if r.partial {
                    // Keep calling receive until our partial message is
                    // complete.  This effectively resets the receive
                    // timeout on the assumption that once we start
                    // receiving data we want to keep at it until we
                    // receive all of it, regardless of the origianl
                    // timeout value.
                    continue;
                }
                if r.complete {
                    self.complete = true;
                }
                return Ok(r.value);
            } else {
                return Ok(None);
            }
        }
    }

    pub fn recv(&mut self) -> Result<Option<JsonValue>, String> {
        self.recv_with_timeout(DEFAULT_REQUEST_TIMEOUT)
    }
}

/// Client communication state maintenance.
struct Session {
    /// Client so we can ask it to pull data from the Bus for us.
    client: Client,

    /// Each session is identified on the network by a random thread string.
    thread: String,

    /// Have we successfully established a connection withour
    /// destination service?
    connected: bool,

    /// Service name.
    service: String,

    /// Top-level bus address for the service we're making requests of.
    service_addr: BusAddress,

    /// Routed messages go here.
    router_addr: BusAddress,

    /// Worker-specific bus address for our session.
    ///
    /// Set any time a response arrives so we know who sent it.
    worker_addr: Option<BusAddress>,

    /// Most recently used per-thread request id.
    ///
    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    last_thread_trace: usize,

    /// Replies to this thread which have not yet been pulled by
    /// any requests.  Using VecDeque since it's optimized for
    /// queue-like behavior (push back / pop front).
    backlog: VecDeque<Message>,

    /// Staging ground for "partial" messages arriving in chunks.
    partial_buffer: Option<String>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session({} {})", self.service(), self.thread())
    }
}

impl Session {
    fn new(client: Client, service: &str) -> Session {
        let router_addr =
            BusAddress::for_router(client.config().client().router_name(), client.domain());

        let service_addr = BusAddress::for_bare_service(service);

        Session {
            client,
            router_addr,
            service_addr,
            worker_addr: None,
            service: String::from(service),
            connected: false,
            last_thread_trace: 0,
            partial_buffer: None,
            backlog: VecDeque::new(),
            thread: util::random_number(16),
        }
    }

    fn service(&self) -> &str {
        &self.service
    }

    fn thread(&self) -> &str {
        &self.thread
    }

    fn connected(&self) -> bool {
        self.connected
    }

    fn reset(&mut self) {
        log::trace!("{self} resetting...");
        self.worker_addr = None;
        self.connected = false;
        self.backlog.clear();
    }

    fn router_addr(&self) -> &BusAddress {
        &self.router_addr
    }

    fn worker_addr(&self) -> Option<&BusAddress> {
        self.worker_addr.as_ref()
    }

    fn service_addr(&self) -> &BusAddress {
        &self.service_addr
    }

    /// Mutable Ref to our under-the-covers client singleton.
    fn client_internal_mut(&self) -> RefMut<ClientSingleton> {
        self.client.singleton().borrow_mut()
    }

    /// Returns the underlying address of the remote end if we have
    /// a remote client address (i.e. we are connected).  Otherwise,
    /// returns the underlying BusAddress for our service-level address.
    fn destination_addr(&self) -> &BusAddress {
        match self.worker_addr() {
            Some(a) => a,
            None => self.service_addr(),
        }
    }

    fn recv_from_backlog(&mut self, thread_trace: usize) -> Option<Message> {
        if let Some(index) = self
            .backlog
            .iter()
            .position(|m| m.thread_trace() == thread_trace)
        {
            log::trace!("{self} found a reply in the backlog for request {thread_trace}");

            self.backlog.remove(index)
        } else {
            None
        }
    }

    fn recv(&mut self, thread_trace: usize, timeout: i32) -> Result<Option<Response>, String> {
        let mut timer = util::Timer::new(timeout);

        let mut first_loop = true;
        loop {
            log::trace!(
                "{self} in recv() for trace {thread_trace} with {} remaining",
                timer.remaining()
            );

            if let Some(msg) = self.recv_from_backlog(thread_trace) {
                return self.unpack_reply(&mut timer, msg);
            }

            if first_loop {
                first_loop = false;
            } else if timer.done() {
                // Avoid exiting on first loop so we have at least
                // one chance to pull data from the network before exiting.
                return Ok(None);
            }

            let mut tmsg = match self
                .client_internal_mut()
                .recv_session(&mut timer, self.thread())?
            {
                Some(m) => m,
                None => continue, // timeout, etc.
            };

            // Look Who's Talking (Too?).
            self.worker_addr = Some(BusAddress::from_str(tmsg.from())?);

            // Toss the messages onto our backlog as we receive them.
            for msg in tmsg.body_mut().drain(..) {
                self.backlog.push_back(msg);
            }

            // Loop back around and see if we can pull the message
            // we want from our backlog.
        }
    }

    /// Unpack one opensrf message -- there may be multiple opensrf
    /// messages inside a single transport message.
    fn unpack_reply(
        &mut self,
        timer: &mut util::Timer,
        mut msg: Message,
    ) -> Result<Option<Response>, String> {

        if let Payload::Result(resp) = msg.payload_mut() {

            log::trace!("unpack_reply() status={}", resp.status());

            // take_content() because this message is about to get dropped.
            let mut value = resp.take_content();

            if resp.status() == &MessageStatus::Partial {
                let buf = match self.partial_buffer.as_mut() {
                    Some(b) => b,
                    None => {
                        self.partial_buffer = Some(String::new());
                        self.partial_buffer.as_mut().unwrap()
                    }
                };

                // The content of a partial message is a raw JSON string,
                // representing a subset of the JSON value response as a whole.
                if let Some(chunk) = value.as_str() {
                    buf.push_str(chunk);
                }

                return Ok(Some(Response {
                    value: None,
                    complete: false,
                    partial: true,
                }));
            } else if resp.status() == &MessageStatus::PartialComplete {
                // Take + clear the partial buffer.
                let mut buf = match self.partial_buffer.take() {
                    Some(b) => b,
                    None => String::new(),
                };

                // Append any trailing content if available.
                if let Some(chunk) = value.as_str() {
                    buf.push_str(chunk);
                }

                // Compile the collected JSON chunks into a single value,
                // which is the final response value.
                value = json::parse(&buf)
                    .or_else(|e| Err(format!("Error reconstituting partial message: {e}")))?;

                log::trace!("Partial message is now complete");
            }

            if let Some(s) = self.client.singleton().borrow().serializer() {
                value = s.unpack(value);
            }

            return Ok(Some(Response {
                value: Some(value),
                complete: false,
                partial: false,
            }));
        }

        let err_msg;
        let trace = msg.thread_trace();

        if let Payload::Status(stat) = msg.payload() {
            match self.unpack_status_message(trace, timer, &stat) {
                Ok(v) => {
                    return Ok(v);
                }
                Err(e) => err_msg = e,
            }
        } else {
            err_msg = format!("{self} unexpected response for request {trace}: {msg:?}");
        }

        self.reset();

        return Err(err_msg);
    }

    fn unpack_status_message(
        &mut self,
        trace: usize,
        timer: &mut util::Timer,
        statmsg: &Status,
    ) -> Result<Option<Response>, String> {
        let stat = statmsg.status();

        match stat {
            MessageStatus::Ok => {
                log::trace!("{self} Marking self as connected");
                self.connected = true;
                Ok(None)
            }
            MessageStatus::Continue => {
                timer.reset();
                Ok(None)
            }
            MessageStatus::Complete => {
                log::trace!("{self} request {trace} complete");
                Ok(Some(Response {
                    value: None,
                    complete: true,
                    partial: false,
                }))
            }
            _ => {
                self.reset();
                return Err(format!("{self} request {trace} failed: {}", statmsg));
            }
        }
    }

    fn incr_thread_trace(&mut self) -> usize {
        self.last_thread_trace += 1;
        self.last_thread_trace
    }

    /// Issue a new API call and return the thread_trace of the sent request.
    fn request<T>(&mut self, method: &str, params: T) -> Result<usize, String>
    where
        T: Into<ApiParams>,
    {
        log::debug!("{self} sending request {method}");

        let trace = self.incr_thread_trace();

        // Turn params into a ApiParams object.
        let mut params = params.into();

        let params = params.serialize(&self.client);

        if !self.connected() {
            // Discard any knowledge about previous communication
            // with a specific worker since we are not connected.
            self.worker_addr = None;
        }

        let tmsg = TransportMessage::with_body(
            self.destination_addr().as_str(),
            self.client.address().as_str(),
            self.thread(),
            Message::new(
                MessageType::Request,
                trace,
                Payload::Method(Method::new(method, params)),
            ),
        );

        if !self.connected() {
            // Top-level API calls always go through the router on
            // our primary domain

            let router_addr = self.router_addr().as_str();
            self.client_internal_mut()
                .bus_mut()
                .send_to(&tmsg, router_addr)?;
        } else {
            if let Some(a) = self.worker_addr() {
                // Requests directly to client addresses must be routed
                // to the domain of the client address.
                self.client_internal_mut()
                    .get_domain_bus(a.domain())?
                    .send(&tmsg)?;
            } else {
                self.reset();
                return Err(format!("We are connected, but have no worker_addr()"));
            }
        }

        Ok(trace)
    }

    /// Establish a connected session with a remote worker.
    fn connect(&mut self) -> Result<(), String> {
        if self.connected() {
            log::warn!("{self} is already connected");
            return Ok(());
        }

        // Discard any knowledge about previous communication
        // with a specific worker since we are not connected.
        self.worker_addr = None;

        log::debug!("{self} sending CONNECT");

        let trace = self.incr_thread_trace();

        let tm = TransportMessage::with_body(
            self.destination_addr().as_str(),
            self.client.address().as_str(),
            self.thread(),
            Message::new(MessageType::Connect, trace, Payload::NoPayload),
        );

        // Connect calls always go to our router.
        self.client
            .singleton()
            .borrow_mut()
            .bus_mut()
            .send_to(&tm, self.router_addr().as_str())?;

        self.recv(trace, CONNECT_TIMEOUT)?;

        if self.connected() {
            log::trace!("{self} connected OK");
            Ok(())
        } else {
            self.reset();
            Err(format!("CONNECT timed out"))
        }
    }

    /// Send a DISCONNECT to our remote worker.
    ///
    /// Does not wait for any response.  NO-OP if not connected.
    fn disconnect(&mut self) -> Result<(), String> {
        if !self.connected() || self.worker_addr().is_none() {
            self.reset();
            return Ok(());
        }

        let trace = self.incr_thread_trace();

        let dest_addr = self.worker_addr().unwrap(); // verified above

        log::debug!("{self} sending DISCONNECT");

        let tmsg = TransportMessage::with_body(
            dest_addr.as_str(),
            self.client.address().as_str(),
            self.thread(),
            Message::new(MessageType::Disconnect, trace, Payload::NoPayload),
        );

        self.client_internal_mut()
            .get_domain_bus(dest_addr.domain())?
            .send(&tmsg)?;

        self.reset();

        Ok(())
    }
}

/// Public-facing Session wrapper which exports the needed session API.
pub struct SessionHandle {
    session: Rc<RefCell<Session>>,
}

impl SessionHandle {
    pub fn new(client: Client, service: &str) -> SessionHandle {
        let ses = Session::new(client, service);

        log::trace!("Created new session {ses}");

        SessionHandle {
            session: Rc::new(RefCell::new(ses)),
        }
    }

    /// Issue a new API call and return the Request
    ///
    /// params is a JSON-able thing.  E.g. vec![1,2,3], json::object!{"a": "b"}, etc.
    pub fn request<T>(&mut self, method: &str, params: T) -> Result<Request, String>
    where
        T: Into<ApiParams>,
    {
        let thread = self.session.borrow().thread().to_string();

        Ok(Request::new(
            thread,
            self.session.clone(),
            self.session.borrow_mut().request(method, params)?,
        ))
    }

    /// Send a request and receive a ResponseIterator for iterating
    /// the responses to the method.
    ///
    /// Uses the default request timeout DEFAULT_REQUEST_TIMEOUT.
    pub fn send_recv<T>(&mut self, method: &str, params: T) -> Result<ResponseIterator, String>
    where
        T: Into<ApiParams>,
    {
        Ok(ResponseIterator::new(self.request(method, params)?))
    }

    pub fn connect(&self) -> Result<(), String> {
        self.session.borrow_mut().connect()
    }

    pub fn disconnect(&self) -> Result<(), String> {
        self.session.borrow_mut().disconnect()
    }

    pub fn connected(&self) -> bool {
        self.session.borrow().connected()
    }
}

/// Iterates over a series of replies to an API request.
pub struct ResponseIterator {
    request: Request,
}

impl Iterator for ResponseIterator {
    type Item = Result<JsonValue, String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.request.recv().transpose()
    }
}

impl ResponseIterator {
    pub fn new(request: Request) -> Self {
        ResponseIterator { request }
    }
}

/// Minimal multi-session implementation.
///
/// Primary use is to blast a series of requests in parallel without
/// having to be concerned about tracking them all or interacting
/// with the underlying sessions.
///
/// Connecting sessions is not supported, because each session is
/// responsible for exactly one request.
///
/// Maybe later:
///     Max parallel / throttling
pub struct MultiSession {
    client: Client,
    service: String,
    requests: Vec<Request>,
}

impl MultiSession {
    pub fn new(client: Client, service: &str) -> MultiSession {
        MultiSession {
            client,
            service: service.to_string(),
            requests: Vec::new(),
        }
    }

    /// Create a new underlying session and send a request via the session.
    ///
    /// Returns the session thead so the caller can link specific
    /// request to their responses (see recv()) if needed.
    pub fn request<T>(&mut self, method: &str, params: T) -> Result<String, String>
    where
        T: Into<ApiParams>,
    {
        let mut ses = self.client.session(&self.service);
        let req = ses.request(method, params)?;
        let thread = req.thread().to_string();

        self.requests.push(req);

        Ok(thread)
    }

    /// True if all requests have been marked complete and have
    /// empty reply backlogs.
    ///
    /// May mark additional requests as complete as a side effect.
    pub fn complete(&mut self) -> bool {
        self.remove_completed();
        self.requests.len() == 0
    }

    /// Wait up to `timeout` seconds for a response to arrive for any
    /// of our outstanding requests.
    ///
    /// Returns (Thread, Response) if found
    pub fn recv(&mut self, timeout: i32) -> Result<Option<(String, JsonValue)>, String> {
        // Wait for replies to any sessions on this client to appear
        // then see if we can find one related specfically to the
        // requests we are managing.

        if self.client.wait(timeout)? {
            for req in self.requests.iter_mut() {
                if let Some(resp) = req.recv_with_timeout(0)? {
                    return Ok(Some((req.thread.to_string(), resp)));
                }
            }
        }

        self.remove_completed();

        Ok(None)
    }

    fn remove_completed(&mut self) {
        // We consider a request to be complete only when it has
        // received a COMPLETE messsage and its backlog has been
        // drained.
        let test = |r: &Request| r.exhausted();

        loop {
            let pos = match self.requests.iter().position(test) {
                Some(p) => p,
                None => break,
            };

            self.requests.remove(pos);
        }
    }
}

pub struct ServerSession {
    /// Service name.
    service: String,

    /// Link to our ClientSingleton so we can ask it to pull data from the Bus.
    client: Client,

    /// Each session is identified on the network by a random thread string.
    thread: String,

    /// Who sent us a request.
    sender: BusAddress,

    /// True if we have already sent a COMPLETE message to the caller.
    /// Use this to avoid sending replies after a COMPLETE.
    responded_complete: bool,

    /// Most recently used per-thread request id.
    ///
    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    last_thread_trace: usize,

    /// Responses collected to be packed into an "atomic" response array.
    atomic_resp_queue: Option<Vec<JsonValue>>,
}

impl fmt::Display for ServerSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServerSession({} {})", self.service(), self.thread())
    }
}

impl ServerSession {
    pub fn new(
        client: Client,
        service: &str,
        thread: &str,
        last_thread_trace: usize,
        sender: BusAddress,
    ) -> ServerSession {
        ServerSession {
            client,
            sender,
            last_thread_trace,
            service: service.to_string(),
            responded_complete: false,
            thread: thread.to_string(),
            atomic_resp_queue: None,
        }
    }

    pub fn last_thread_trace(&self) -> usize {
        self.last_thread_trace
    }

    pub fn set_last_thread_trace(&mut self, trace: usize) {
        self.last_thread_trace = trace
    }

    pub fn clear_responded_complete(&mut self) {
        self.responded_complete = false;
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn sender(&self) -> &BusAddress {
        &self.sender
    }

    pub fn new_atomic_resp_queue(&mut self) {
        log::debug!("{self} starting new atomic queue...");
        self.atomic_resp_queue = Some(Vec::new());
    }

    /// Mutable Ref to our under-the-covers client singleton.
    fn client_internal_mut(&self) -> RefMut<ClientSingleton> {
        self.client.singleton().borrow_mut()
    }

    pub fn responded_complete(&self) -> bool {
        self.responded_complete
    }

    pub fn respond<T>(&mut self, value: T) -> Result<(), String>
    where
        T: Into<JsonValue>,
    {
        let mut value = json::from(value);
        if let Some(s) = self.client.singleton().borrow().serializer() {
            value = s.pack(value);
        }

        if let Some(queue) = &mut self.atomic_resp_queue {
            queue.push(value);
            return Ok(());
        }

        let msg = Message::new(
            MessageType::Result,
            self.last_thread_trace(),
            Payload::Result(message::Result::new(
                MessageStatus::Ok,
                "OK",
                "osrfResult",
                value,
            )),
        );

        let tmsg = TransportMessage::with_body(
            self.sender.as_str(),
            self.client.address().as_str(),
            self.thread(),
            msg,
        );

        let domain = self.sender.domain();

        self.client_internal_mut()
            .get_domain_bus(domain)?
            .send(&tmsg)
    }

    pub fn respond_complete<T>(&mut self, value: T) -> Result<(), String>
    where
        T: Into<JsonValue>,
    {
        if self.responded_complete {
            log::warn!(
                r#"respond_complete() called multiple times for
                thread {}.  Dropping trailing responses"#,
                self.thread()
            );
            return Ok(());
        }

        self.respond(value)?;
        self.send_complete()
    }

    /// Send the Request Complete status message to our caller.
    ///
    /// This is the same as respond_complete() without a response value.
    pub fn send_complete(&mut self) -> Result<(), String> {
        self.responded_complete = true;

        if let Some(queue) = self.atomic_resp_queue.take() {
            log::debug!("{self} respding with contents of atomic queue");
            // Clear the resposne queue and send the whole list
            // back to the caller.
            self.respond(queue)?;
        }

        let msg = Message::new(
            MessageType::Status,
            self.last_thread_trace(),
            Payload::Status(message::Status::new(
                MessageStatus::Complete,
                "Request Complete",
                "osrfStatus",
            )),
        );

        let tmsg = TransportMessage::with_body(
            self.sender.as_str(),
            self.client.address().as_str(),
            self.thread(),
            msg,
        );

        let domain = self.sender.domain();

        self.client_internal_mut()
            .get_domain_bus(domain)?
            .send(&tmsg)
    }
}
