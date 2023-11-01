use super::addr::{ClientAddress, RouterAddress};
use super::bus;
use super::conf;
use super::message;
use super::params::ApiParams;
use super::session::ResponseIterator;
use super::session::SessionHandle;
use super::util;
use json::JsonValue;
use log::{info, trace};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

const DEFAULT_ROUTER_COMMAND_TIMEOUT: i32 = 10;

pub trait DataSerializer {
    fn pack(&self, value: JsonValue) -> JsonValue;
    fn unpack(&self, value: JsonValue) -> JsonValue;
}

/// Generally speaking, we only need 1 ClientSingleton per thread (hence
/// the name).  This manages one bus connection per domain and stores
/// messages pulled from the bus that have not yet been processed by
/// higher-up modules.
pub struct ClientSingleton {
    /// Make it possible to clear our Bus so the caller may take it
    /// back once they are done with this client.
    bus: Option<bus::Bus>,

    /// Our primary domain
    domain: String,

    /// Connections to remote domains
    remote_bus_map: HashMap<String, bus::Bus>,

    config: Arc<conf::Config>,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,

    /// If present, JsonValue's will be passed through its pack() and
    /// unpack() methods before/after data hits the network.
    serializer: Option<Arc<dyn DataSerializer>>,
}

impl ClientSingleton {
    fn new(config: Arc<conf::Config>) -> Result<ClientSingleton, String> {
        let bus = bus::Bus::new(config.client())?;
        Ok(ClientSingleton::from_bus(bus, config))
    }

    /// Create a new singleton instance from a previously setup Bus.
    fn from_bus(bus: bus::Bus, config: Arc<conf::Config>) -> ClientSingleton {
        let domain = config.client().domain().name().to_string();

        ClientSingleton {
            config,
            domain,
            bus: Some(bus),
            backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
            serializer: None,
        }
    }

    pub fn serializer(&self) -> &Option<Arc<dyn DataSerializer>> {
        &self.serializer
    }

    fn clear_backlog(&mut self) {
        self.backlog.clear();
    }

    /// Full bus address as a string
    fn address(&self) -> &str {
        self.bus().address().as_str()
    }

    /// Our primary bus domain
    fn domain(&self) -> &str {
        &self.domain
    }

    /// Ref to our Bus.
    ///
    /// Panics if bus is unset.
    pub fn bus(&self) -> &bus::Bus {
        match self.bus.as_ref() {
            Some(b) => b,
            None => panic!("Client has no Bus connection!"),
        }
    }

    /// Mut ref to our Bus.
    ///
    /// Panics if our Bus is unset.
    pub fn bus_mut(&mut self) -> &mut bus::Bus {
        match self.bus.as_mut() {
            Some(b) => b,
            None => panic!("Client has no Bus connection!"),
        }
    }

    /// Clear and return our Bus connection.
    ///
    /// Panics if our Bus is unset.
    ///
    /// Generally, take/set_bus are only used in unique scenarios.
    /// Use with caution, since an unset Bus means the client cannot
    /// be used and the thread will exit if the client is used.
    pub fn take_bus(&mut self) -> bus::Bus {
        match self.bus.take() {
            Some(b) => b,
            None => panic!("Client has to Bus connection!"),
        }
    }

    /// Give this client a bus to use.
    pub fn set_bus(&mut self, bus: bus::Bus) {
        self.bus = Some(bus);
    }

    pub fn get_domain_bus(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        log::trace!("Loading bus connection for domain: {domain}");

        if domain.eq(self.domain()) {
            Ok(self.bus_mut())
        } else {
            if self.remote_bus_map.contains_key(domain) {
                return Ok(self.remote_bus_map.get_mut(domain).unwrap());
            }

            self.add_connection(domain)
        }
    }

    /// Add a connection to a new remote domain.
    ///
    /// Panics if our configuration has no primary domain.
    fn add_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        // When adding a connection to a remote domain, assume the same
        // connection type, etc. is used and just change the domain.
        let mut conf = self.config.client().clone();

        conf.set_domain(domain);

        let bus = bus::Bus::new(&conf)?;

        info!("Opened connection to new domain: {}", domain);

        self.remote_bus_map.insert(domain.to_string(), bus);
        self.get_domain_bus(domain)
    }

    /// Returns the first transport message pulled from the transport
    /// message backlog that matches the provided thread.
    fn recv_session_from_backlog(&mut self, thread: &str) -> Option<message::TransportMessage> {
        if let Some(index) = self.backlog.iter().position(|tm| tm.thread() == thread) {
            trace!("Found a backlog reply for thread {thread}");
            Some(self.backlog.remove(index))
        } else {
            None
        }
    }

    /// Returns true if any data exists in the backlog within the
    /// timeout provided.  This is useful for checking network activity
    /// across multiple active sessions in lieu of polling each
    /// session for responses.
    pub fn wait(&mut self, timeout: i32) -> Result<bool, String> {
        if !self.backlog.is_empty() {
            return Ok(true);
        }

        let timer = util::Timer::new(timeout);

        while self.backlog.is_empty() && !timer.done() {
            if let Some(tm) = self.bus_mut().recv(timer.remaining(), None)? {
                self.backlog.push(tm);
                break;
            }
        }

        Ok(!self.backlog.is_empty())
    }

    pub fn recv_session(
        &mut self,
        timer: &mut util::Timer,
        thread: &str,
    ) -> Result<Option<message::TransportMessage>, String> {
        loop {
            if let Some(tm) = self.recv_session_from_backlog(thread) {
                return Ok(Some(tm));
            }

            if timer.done() {
                // Nothing in the backlog and all out of time.
                return Ok(None);
            }

            // See what we can pull from the message bus

            if let Some(tm) = self.bus_mut().recv(timer.remaining(), None)? {
                self.backlog.push(tm);
            }

            // Loop back around and see if we can pull a transport
            // message from the backlog matching the requested thread.
        }
    }

    fn send_router_command(
        &mut self,
        username: &str,
        domain: &str,
        router_command: &str,
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        let addr = RouterAddress::new(username, domain);

        // Always use the address of our primary Bus
        let mut tmsg = message::TransportMessage::new(
            addr.as_str(),
            self.bus().address().as_str(),
            &util::random_number(16),
        );

        tmsg.set_router_command(router_command);
        if let Some(rc) = router_class {
            tmsg.set_router_class(rc);
        }

        let bus = self.get_domain_bus(domain)?;
        bus.send(&tmsg)?;

        if !await_reply {
            return Ok(None);
        }

        // Always listen on our primary bus.
        // TODO rethink this.  If we have replies from other requests
        // sitting in the bus, they may be received here instead
        // of the expected router response.  self.bus.clear() before
        // send is one option, but pretty heavy-handed.
        match self.bus_mut().recv(DEFAULT_ROUTER_COMMAND_TIMEOUT, None)? {
            Some(tm) => match tm.router_reply() {
                Some(reply) => match json::parse(reply) {
                    Ok(jv) => Ok(Some(jv)),
                    Err(e) => Err(format!(
                        "Router command {} return unparseable content: {} {}",
                        router_command, reply, e
                    )),
                },
                _ => Err(format!(
                    "Router command {} returned without reply_content",
                    router_command
                )),
            },
            _ => Err(format!(
                "Router command {} returned no results in {} seconds",
                router_command, DEFAULT_ROUTER_COMMAND_TIMEOUT
            )),
        }
    }
}

impl fmt::Display for ClientSingleton {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientSingleton({})", self.address())
    }
}

/// Wrapper around our ClientSingleton Ref so we can easily share a client
/// within a given thread.
///
/// Wrapping the Ref in a struct allows us to present a client-like
/// API to the caller.  I.e. the caller is not required to .borrow() /
/// .borrow_mut() directly when performing actions against the client Ref.
///
/// When a new client Ref is needed, clone the Client.
#[derive(Clone)]
pub struct Client {
    singleton: Rc<RefCell<ClientSingleton>>,
    address: ClientAddress,
    domain: String,
}

impl Client {
    pub fn connect(config: Arc<conf::Config>) -> Result<Client, String> {
        // This performs the actual bus-level connection.
        let singleton = ClientSingleton::new(config)?;

        let address = singleton.bus().address().clone();
        let domain = singleton.domain().to_string();

        Ok(Client {
            address,
            domain,
            singleton: Rc::new(RefCell::new(singleton)),
        })
    }

    pub fn from_bus(bus: bus::Bus, config: Arc<conf::Config>) -> Client {
        // This performs the actual bus-level connection.
        let singleton = ClientSingleton::from_bus(bus, config);

        let address = singleton.bus().address().clone();
        let domain = singleton.domain().to_string();

        Client {
            address,
            domain,
            singleton: Rc::new(RefCell::new(singleton)),
        }
    }

    /// Panics if bus is unset.
    ///
    /// Most callers will never need this.
    pub fn take_bus(&self) -> bus::Bus {
        self.singleton.borrow_mut().take_bus()
    }

    /// Apply a new bus.
    ///
    /// Most callers will never need this.
    pub fn set_bus(&self, bus: bus::Bus) {
        self.singleton.borrow_mut().set_bus(bus);
    }

    pub fn singleton(&self) -> &Rc<RefCell<ClientSingleton>> {
        &self.singleton
    }

    pub fn clone(&self) -> Self {
        Client {
            address: self.address().clone(),
            domain: self.domain().to_string(),
            singleton: self.singleton().clone(),
        }
    }

    pub fn set_serializer(&self, serializer: Arc<dyn DataSerializer>) {
        self.singleton.borrow_mut().serializer = Some(serializer);
    }

    pub fn address(&self) -> &ClientAddress {
        &self.address
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Create a new client session for the requested service.
    pub fn session(&self, service: &str) -> SessionHandle {
        SessionHandle::new(self.clone(), service)
    }

    /// Discard any unprocessed messages from our backlog and clear our
    /// stream of pending messages on the bus.
    pub fn clear(&self) -> Result<(), String> {
        self.singleton().borrow_mut().clear_backlog();
        self.singleton().borrow_mut().bus_mut().clear_bus()
    }

    pub fn send_router_command(
        &self,
        username: &str,
        domain: &str,
        command: &str,
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        self.singleton().borrow_mut().send_router_command(
            username,
            domain,
            command,
            router_class,
            await_reply,
        )
    }

    /// Send a request and receive a ResponseIterator for iterating
    /// the responses to the method.
    ///
    /// Uses the default request timeout DEFAULT_REQUEST_TIMEOUT.
    pub fn send_recv_iter<T>(
        &self,
        service: &str,
        method: &str,
        params: T,
    ) -> Result<ResponseIterator, String>
    where
        T: Into<ApiParams>,
    {
        Ok(ResponseIterator::new(
            self.session(service).request(method, params)?,
        ))
    }

    pub fn config(&self) -> Arc<conf::Config> {
        self.singleton().borrow().config.clone()
    }

    pub fn wait(&self, timeout: i32) -> Result<bool, String> {
        self.singleton().borrow_mut().wait(timeout)
    }

    pub fn send_recv_one<T>(
        &self,
        service: &str,
        method: &str,
        params: T,
    ) -> Result<Option<JsonValue>, String>
    where
        T: Into<ApiParams>,
    {
        let mut ses = self.session(service);
        let mut req = ses.request(method, params)?;

        req.first()
    }
}
