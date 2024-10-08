use crate::osrf::addr::BusAddress;
use crate::osrf::bus;
use crate::osrf::conf;
use crate::osrf::message;
use crate::osrf::params::ApiParams;
use crate::osrf::session::ClientSession;
use crate::osrf::session::ResponseIterator;
use crate::util;
use crate::{EgResult, EgValue};
use log::info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

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

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,
}

impl ClientSingleton {
    fn new() -> EgResult<ClientSingleton> {
        let bus = bus::Bus::new(conf::config().client())?;
        Ok(ClientSingleton::from_bus(bus))
    }

    /// Create a new singleton instance from a previously setup Bus.
    fn from_bus(bus: bus::Bus) -> ClientSingleton {
        let domain = conf::config().client().domain().name();

        ClientSingleton {
            domain: domain.to_string(),
            bus: Some(bus),
            backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
        }
    }

    /// Delete all messages that have been received but not yet pulled
    /// for processing by any higher-up modules.
    fn clear_backlog(&mut self) {
        self.backlog.clear();
    }

    /// Our full bus address as a string
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

    pub fn get_domain_bus(&mut self, domain: &str) -> EgResult<&mut bus::Bus> {
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
    fn add_connection(&mut self, domain: &str) -> EgResult<&mut bus::Bus> {
        // When adding a connection to a remote domain, assume the same
        // connection type, etc. is used and just change the domain.
        let mut conf = conf::config().client().clone();

        conf.set_domain(domain);

        let bus = bus::Bus::new(&conf)?;

        info!("Opened connection to new domain: {}", domain);

        self.remote_bus_map.insert(domain.to_string(), bus);
        self.get_domain_bus(domain)
    }

    /// Removes and returns the first transport message pulled from the
    /// transport message backlog that matches the provided thread.
    fn recv_session_from_backlog(&mut self, thread: &str) -> Option<message::TransportMessage> {
        if let Some(index) = self.backlog.iter().position(|tm| tm.thread() == thread) {
            Some(self.backlog.remove(index))
        } else {
            None
        }
    }

    /// Returns true if any data exists in the backlog within the
    /// timeout provided.
    ///
    /// This is useful for checking network activity
    /// across multiple active sessions in lieu of polling each
    /// session for responses.
    pub fn wait(&mut self, timeout: u64) -> EgResult<bool> {
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

    /// Receive up to one message destined for the specified session.
    pub fn recv_session(
        &mut self,
        timer: &mut util::Timer,
        thread: &str,
    ) -> EgResult<Option<message::TransportMessage>> {
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

    /// Send a command to the router specified by username/domain, like
    /// "register" and "unregister".
    fn send_router_command(
        &mut self,
        username: &str,
        domain: &str,
        router_command: &str,
        router_class: Option<&str>,
    ) -> EgResult<()> {
        let addr = BusAddress::for_router(username, domain);

        // Always use the from address of our primary Bus
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
        bus.send(tmsg)?;

        Ok(())
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
    address: BusAddress,
    domain: String,
}

impl Client {
    /// Create a new Client and connect to the bus.
    ///
    /// NOTE: In most cases, cloning an existing client is the
    /// preferred approach, since that guarantees you are
    /// using an existing Bus connection, instead of creating
    /// a new one, which is generally unnecessary.
    pub fn connect() -> EgResult<Client> {
        // This performs the actual bus-level connection.
        let singleton = ClientSingleton::new()?;

        let address = singleton.bus().address().clone();
        let domain = singleton.domain().to_string();

        Ok(Client {
            address,
            domain,
            singleton: Rc::new(RefCell::new(singleton)),
        })
    }

    /// Create a new Client from an existing Bus connection.
    ///
    /// This can be handy because a Bus is Send-able, but a Client is not.
    pub fn from_bus(bus: bus::Bus) -> Client {
        // This performs the actual bus-level connection.
        let singleton = ClientSingleton::from_bus(bus);

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

    pub fn address(&self) -> &BusAddress {
        &self.address
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Create a new client session for the requested service.
    pub fn session(&self, service: &str) -> ClientSession {
        ClientSession::new(self.clone(), service)
    }

    /// Discard any unprocessed messages from our backlog and clear our
    /// stream of pending messages on the bus.
    pub fn clear(&self) -> EgResult<()> {
        self.singleton().borrow_mut().clear_backlog();
        self.singleton().borrow_mut().bus_mut().clear_bus()
    }

    /// Wrapper for ClientSingleton::send_router_command()
    pub fn send_router_command(
        &self,
        username: &str,
        domain: &str,
        command: &str,
        router_class: Option<&str>,
    ) -> EgResult<()> {
        self.singleton()
            .borrow_mut()
            .send_router_command(username, domain, command, router_class)
    }

    /// Send a request and receive a ResponseIterator for iterating
    /// the responses to the method.
    ///
    /// Uses the default request timeout DEFAULT_REQUEST_TIMEOUT.
    pub fn send_recv_iter(
        &self,
        service: &str,
        method: &str,
        params: impl Into<ApiParams>,
    ) -> EgResult<ResponseIterator> {
        Ok(ResponseIterator::new(
            self.session(service).request(method, params)?,
        ))
    }

    /// Wrapper for ClientSingleton::wait()
    pub fn wait(&self, timeout: u64) -> EgResult<bool> {
        self.singleton().borrow_mut().wait(timeout)
    }

    /// Sends an API request and returns the first response, or None if
    /// the API call times out.
    ///
    /// This still waits for all responses to arrive before returning the
    /// first, so the request can be marked as complete and cleaned up.
    pub fn send_recv_one(
        &self,
        service: &str,
        method: &str,
        params: impl Into<ApiParams>,
    ) -> EgResult<Option<EgValue>> {
        let mut ses = self.session(service);
        let mut req = ses.request(method, params)?;

        req.first()
    }
}
