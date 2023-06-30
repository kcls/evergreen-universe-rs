use super::addr::{BusAddress, ClientAddress, RouterAddress, ServiceAddress};
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
use json::Value;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;

const CONNECT_TIMEOUT: i32 = 10;
pub const DEFAULT_REQUEST_TIMEOUT: i32 = 60;

/// Response data propagated from a session to the calling Request.
struct Response {
    /// Response from an API call as a json::Value.
    value: Option<json::Value>,
    /// True if the Request we are a response to is complete.
    complete: bool,
    /// True if this is a partial response
    partial: bool,
}

/// Models a single API call through which the caller can receive responses.
pub struct Request {
    /// Link to our session so we can ask it for bus data.
    session: Rc<RefCell<Session>>,

    /// Have we received all of the replies yet?
    complete: bool,

    /// Unique ID per thread/session.
    thread_trace: usize,
}

impl Request {
    fn new(session: Rc<RefCell<Session>>, thread_trace: usize) -> Request {
        Request {
            session,
            complete: false,
            thread_trace,
        }
    }

    /// Pull all responses from the bus and return the first.
    ///
    /// Handy if you are expecting exactly one result, or only care
    /// about the first, but want to pull all data off the bus until the
    /// message is officially marked as complete.
    pub fn first(&mut self) -> Result<Option<json::Value>, String> {
        self.first_with_timeout(DEFAULT_REQUEST_TIMEOUT)
    }

    pub fn first_with_timeout(&mut self, timeout: i32) -> Result<Option<json::Value>, String> {
        let mut resp: Option<json::Value> = None;
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
    pub fn recv_with_timeout(&mut self, timeout: i32) -> Result<Option<json::Value>, String> {
        if self.complete {
            // If we are marked complete, it means we've read all the
            // replies, the last of which was a request-complete message.
            // Nothing left to read.
            return Ok(None);
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

    pub fn recv(&mut self) -> Result<Option<json::Value>, String> {
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
    service_addr: ServiceAddress,

    /// Routed messages go here.
    router_addr: RouterAddress,

    /// Worker-specific bus address for our session.
    ///
    /// Set any time a response arrives so we know who sent it.
    worker_addr: Option<ClientAddress>,

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
        let router_addr = RouterAddress::new(client.domain());
        Session {
            client,
            router_addr,
            service: String::from(service),
            worker_addr: None,
            service_addr: ServiceAddress::new(&service),
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

    fn router_addr(&self) -> &RouterAddress {
        &self.router_addr
    }

    fn worker_addr(&self) -> Option<&ClientAddress> {
        self.worker_addr.as_ref()
    }

    fn service_addr(&self) -> &ServiceAddress {
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
            Some(a) => a.addr(),
            None => self.service_addr().addr(),
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

        loop {
            log::trace!(
                "{self} in recv() for trace {thread_trace} with {} remaining",
                timer.remaining()
            );

            if let Some(msg) = self.recv_from_backlog(thread_trace) {
                return self.unpack_reply(&mut timer, msg);
            }

            if timer.done() {
                // Nothing in the backlog and all out of time.
                return Ok(None);
            }

            let tmsg = match self
                .client_internal_mut()
                .recv_session(&mut timer, self.thread())?
            {
                Some(m) => m,
                None => continue, // timeout, etc.
            };

            // Look Who's Talking (Too?).
            self.worker_addr = Some(ClientAddress::from_string(tmsg.from())?);

            // Toss the messages onto our backlog as we receive them.
            for msg in tmsg.body() {
                self.backlog.push_back(msg.to_owned());
            }

            // Loop back around and see if we can pull the message
            // we want from our backlog.
        }
    }

    fn unpack_reply(
        &mut self,
        timer: &mut util::Timer,
        msg: Message,
    ) -> Result<Option<Response>, String> {
        if let Payload::Result(resp) = msg.payload() {
            log::trace!("unpack_reply() status={}", resp.status());

            // .to_owned() because this message is about to get dropped.
            let mut value = resp.content().to_owned();

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

            let router_addr = RouterAddress::new(self.client.domain());
            self.client_internal_mut()
                .bus_mut()
                .send_to(&tmsg, router_addr.as_str())?;
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
    /// params is a Vec of JSON-able things.  E.g. vec![1,2,3], vec![json::object!{a: "b"}]
    pub fn request<T>(&mut self, method: &str, params: T) -> Result<Request, String>
    where
        T: Into<ApiParams>,
    {
        Ok(Request::new(
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
    type Item = json::Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self.request.recv() {
            Ok(op) => op,
            Err(e) => {
                log::error!("ResponseIterator failed with {e}");
                None
            }
        }
    }
}

impl ResponseIterator {
    pub fn new(request: Request) -> Self {
        ResponseIterator { request }
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
    sender: ClientAddress,

    /// True if we have already sent a COMPLETE message to the caller.
    /// Use this to avoid sending replies after a COMPLETE.
    responded_complete: bool,

    /// Most recently used per-thread request id.
    ///
    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    last_thread_trace: usize,

    /// Responses collected to be packaed into an "atomic" response array.
    atomic_resp_queue: Option<Vec<json::Value>>,
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
        sender: ClientAddress,
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

    pub fn sender(&self) -> &ClientAddress {
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
        T: Into<json::Value>,
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
        T: Into<json::Value>,
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
