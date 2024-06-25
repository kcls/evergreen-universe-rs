use crate::osrf::addr::BusAddress;
use crate::osrf::client::{Client, ClientSingleton};
use crate::osrf::conf;
use crate::osrf::message;
use crate::osrf::message::Message;
use crate::osrf::message::MessageStatus;
use crate::osrf::message::MessageType;
use crate::osrf::message::MethodCall;
use crate::osrf::message::Payload;
use crate::osrf::message::Status;
use crate::osrf::message::TransportMessage;
use crate::osrf::params::ApiParams;
use crate::util;
use crate::{EgResult, EgValue};
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
    /// Response from an API call as a EgValue.
    value: Option<EgValue>,
    /// True if our originating Request is complete.
    complete: bool,
    /// True if this is a partial response
    partial: bool,
}

/// Models a single API call through which the caller can receive responses.
#[derive(Clone)]
pub struct Request {
    /// Link to our session so we can ask it for bus data.
    session: Rc<RefCell<ClientSessionInternal>>,

    /// Have we received all of the replies yet?
    complete: bool,

    /// Unique ID per thread/session.
    thread_trace: usize,

    /// Having a local copy of the thread can be handy since our
    /// session is only accessible via temporary borrow().
    thread: String,
}

impl Request {
    fn new(
        thread: String,
        session: Rc<RefCell<ClientSessionInternal>>,
        thread_trace: usize,
    ) -> Request {
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
    pub fn first(&mut self) -> EgResult<Option<EgValue>> {
        self.first_with_timeout(DEFAULT_REQUEST_TIMEOUT)
    }

    /// Returns the first response.
    ///
    /// This still waits for all responses to arrive so the request can
    /// be marked as complete and no responses are left lingering on the
    /// message bus.
    pub fn first_with_timeout(&mut self, timeout: i32) -> EgResult<Option<EgValue>> {
        let mut resp: Option<EgValue> = None;
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
    pub fn recv_with_timeout(&mut self, mut timeout: i32) -> EgResult<Option<EgValue>> {
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

    pub fn recv(&mut self) -> EgResult<Option<EgValue>> {
        self.recv_with_timeout(DEFAULT_REQUEST_TIMEOUT)
    }
}

/// Client communication state maintenance.
struct ClientSessionInternal {
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
    /// Each new Request within a ClientSessionInternal gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    last_thread_trace: usize,

    /// Replies to this thread which have not yet been pulled by
    /// any requests.  Using VecDeque since it's optimized for
    /// queue-like behavior (push back / pop front).
    backlog: VecDeque<Message>,

    /// Staging ground for "partial" messages arriving in chunks.
    partial_buffer: Option<String>,
}

impl fmt::Display for ClientSessionInternal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session({} {})", self.service(), self.thread())
    }
}

impl ClientSessionInternal {
    fn new(client: Client, service: &str) -> ClientSessionInternal {
        let router_addr =
            BusAddress::for_router(conf::config().client().router_name(), client.domain());

        let service_addr = BusAddress::for_bare_service(service);

        ClientSessionInternal {
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
        //log::trace!("{self} resetting...");
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
            //log::trace!("{self} found a reply in the backlog for request {thread_trace}");

            self.backlog.remove(index)
        } else {
            None
        }
    }

    fn recv(&mut self, thread_trace: usize, timeout: i32) -> EgResult<Option<Response>> {
        let mut timer = util::Timer::new(timeout);

        let mut first_loop = true;
        loop {
            /*
            log::trace!(
                "{self} in recv() for trace {thread_trace} with {} remaining",
                timer.remaining()
            );
            */

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
    ) -> EgResult<Option<Response>> {
        if let Payload::Result(resp) = msg.payload_mut() {
            log::trace!("{self} Unpacking osrf message status={}", resp.status());

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
                let jval = json::parse(&buf)
                    .or_else(|e| Err(format!("Error reconstituting partial message: {e}")))?;

                // Avoid exiting with an error on receipt of invalid data
                // from the network.  See also Bus::recv().
                value = match EgValue::from_json_value(jval) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!("Error translating JSON value into EgValue: {e}");
                        EgValue::Null
                    }
                };

                log::trace!("Partial message is now complete");
            }

            return Ok(Some(Response {
                value: Some(value),
                complete: false,
                partial: false,
            }));
        }

        let trace = msg.thread_trace();

        if let Payload::Status(stat) = msg.payload() {
            self.unpack_status_message(trace, timer, &stat)
                .map_err(|e| {
                    self.reset();
                    e
                })
        } else {
            self.reset();
            Err(format!("{self} unexpected response for request {trace}: {msg:?}").into())
        }
    }

    fn unpack_status_message(
        &mut self,
        trace: usize,
        timer: &mut util::Timer,
        statmsg: &Status,
    ) -> EgResult<Option<Response>> {
        let stat = statmsg.status();

        match stat {
            MessageStatus::Ok => {
                //log::trace!("{self} Marking self as connected");
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
                return Err(format!("{self} request {trace} failed: {}", statmsg).into());
            }
        }
    }

    fn incr_thread_trace(&mut self) -> usize {
        self.last_thread_trace += 1;
        self.last_thread_trace
    }

    /// Issue a new API call and return the thread_trace of the sent request.
    fn request(&mut self, method: &str, params: impl Into<ApiParams>) -> EgResult<usize> {
        log::debug!("{self} sending request {method}");

        let trace = self.incr_thread_trace();

        let mut params: ApiParams = params.into();
        let params: Vec<EgValue> = params.take_params();

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
                Payload::Method(MethodCall::new(method, params)),
            ),
        );

        if !self.connected() {
            // Top-level API calls always go through the router on
            // our primary domain

            let router_addr = self.router_addr().as_str();
            self.client_internal_mut()
                .bus_mut()
                .send_to(tmsg, router_addr)?;
        } else if let Some(a) = self.worker_addr() {
            // Requests directly to client addresses must be routed
            // to the domain of the client address.
            self.client_internal_mut()
                .get_domain_bus(a.domain())?
                .send(tmsg)?;
        } else {
            self.reset();
            return Err(format!("We are connected, but have no worker_addr()").into());
        }

        Ok(trace)
    }

    /// Establish a connected session with a remote worker.
    fn connect(&mut self) -> EgResult<()> {
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
            .send_to(tm, self.router_addr().as_str())?;

        self.recv(trace, CONNECT_TIMEOUT)?;

        if self.connected() {
            log::trace!("{self} connected OK");
            Ok(())
        } else {
            self.reset();
            Err(format!("CONNECT timed out").into())
        }
    }

    /// Send a DISCONNECT to our remote worker.
    ///
    /// Does not wait for any response.  NO-OP if not connected.
    fn disconnect(&mut self) -> EgResult<()> {
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
            .send(tmsg)?;

        self.reset();

        Ok(())
    }
}

/// Public-facing Session wrapper which exports the needed session API.
pub struct ClientSession {
    session: Rc<RefCell<ClientSessionInternal>>,
}

impl ClientSession {
    pub fn new(client: Client, service: &str) -> ClientSession {
        let ses = ClientSessionInternal::new(client, service);

        log::trace!("Created new session {ses}");

        ClientSession {
            session: Rc::new(RefCell::new(ses)),
        }
    }

    /// Issue a new API call and return the Request
    ///
    /// params is a JSON-able thing.  E.g. vec![1,2,3], json::object!{"a": "b"}, etc.
    pub fn request(&mut self, method: &str, params: impl Into<ApiParams>) -> EgResult<Request> {
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
    pub fn send_recv(
        &mut self,
        method: &str,
        params: impl Into<ApiParams>,
    ) -> EgResult<ResponseIterator> {
        Ok(ResponseIterator::new(self.request(method, params)?))
    }

    pub fn connect(&self) -> EgResult<()> {
        self.session.borrow_mut().connect()
    }

    pub fn disconnect(&self) -> EgResult<()> {
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
    type Item = EgResult<EgValue>;

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
    pub fn request(&mut self, method: &str, params: impl Into<ApiParams>) -> EgResult<String> {
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
    pub fn recv(&mut self, timeout: i32) -> EgResult<Option<(String, EgValue)>> {
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
    atomic_resp_queue: Option<Vec<EgValue>>,
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

    /// Compiles a MessageType::Result Message with the provided
    /// respone value, taking into account whether a response
    /// should even be sent if this the result to an atomic request.
    fn build_result_message(
        &mut self,
        mut result: Option<EgValue>,
        complete: bool,
    ) -> EgResult<Option<Message>> {
        let result_value;

        if self.atomic_resp_queue.is_some() {
            // Add the reply to the queue.
            let q = self.atomic_resp_queue.as_mut().unwrap();

            if let Some(res) = result.take() {
                q.push(res);
            }

            if complete {
                // If we're completing the call and we have an atomic
                // response queue, return the entire contents of the
                // queue to the caller and leave the queue cleared
                // [take() above].

                result_value = self.atomic_resp_queue.take().unwrap().into();
            } else {
                // Nothing left to do since this atmoic request
                // is still producing results.
                return Ok(None);
            }
        } else {
            // Non-atomic request.  Just return the value as is.
            if let Some(res) = result.take() {
                result_value = res;
            } else {
                return Ok(None);
            }
        }

        Ok(Some(Message::new(
            MessageType::Result,
            self.last_thread_trace(),
            Payload::Result(message::Result::new(
                MessageStatus::Ok,
                "OK",
                "osrfResult",
                result_value,
            )),
        )))
    }

    /// Respond with a value and/or a complete message.
    fn respond_with_parts(&mut self, value: Option<EgValue>, complete: bool) -> EgResult<()> {
        if self.responded_complete {
            log::warn!(
                r#"Dropping trailing replies after already sending a
                Request Complete message for thread {}"#,
                self.thread()
            );
            return Ok(());
        }

        let mut complete_msg = None;

        let mut result_msg = self.build_result_message(value, complete)?;

        if complete {
            // Add a Request Complete message
            self.responded_complete = true;

            complete_msg = Some(Message::new(
                MessageType::Status,
                self.last_thread_trace(),
                Payload::Status(message::Status::new(
                    MessageStatus::Complete,
                    "Request Complete",
                    "osrfConnectStatus",
                )),
            ));
        }

        if result_msg.is_none() && complete_msg.is_none() {
            // Nothing to send to the caller.
            return Ok(());
        }

        // We have at least one message to return.
        // Pack what we have into a single transport message.

        let mut tmsg = TransportMessage::new(
            self.sender.as_str(),
            self.client.address().as_str(),
            self.thread(),
        );

        if let Some(msg) = result_msg.take() {
            tmsg.body_mut().push(msg);
        }

        if let Some(msg) = complete_msg.take() {
            tmsg.body_mut().push(msg);
        }

        self.client_internal_mut()
            .get_domain_bus(self.sender.domain())?
            .send(tmsg)
    }

    pub fn send_complete(&mut self) -> EgResult<()> {
        self.respond_with_parts(None, true)
    }

    pub fn respond(&mut self, value: impl Into<EgValue>) -> EgResult<()> {
        self.respond_with_parts(Some(value.into()), false)
    }

    pub fn respond_complete(&mut self, value: impl Into<EgValue>) -> EgResult<()> {
        self.respond_with_parts(Some(value.into()), true)
    }
}
