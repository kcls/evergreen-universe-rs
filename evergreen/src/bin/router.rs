//! OpenSRF Router
//!
//! Starts one thread per routed domain.  Each thread listens
//! for messages at the router address on its domain (e.g.
//! opensrf:router:private.localhost).
//!
//! For API calls, requests are routed to service entry points on the
//! same domain when possible.  Otherwise, requests are routed to
//! service entry points on another domain whose service is locally
//! registered.
//!
//! Once the initial request is routed, the router is no longer involved
//! in the conversation.
use eg::date;
use eg::init;
use eg::osrf::addr::BusAddress;
use eg::osrf::bus::Bus;
use eg::osrf::conf;
use eg::osrf::logging::Logger;
use eg::osrf::message;
use eg::osrf::message::{Message, MessageStatus, MessageType, Payload, Status, TransportMessage};
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::env;
use std::fmt;
use std::thread;
use std::time::Duration;

/// How often do we wake from listening for messages and give shutdown
/// signals a chance to propagate.
const POLL_TIMEOUT: i32 = 5;

/// A service instance.
///
/// This is what we traditionally call a "Listener" in OpenSRF.
/// It represents a single entry point for routing top-level
/// API calls.
///
/// Generally this is the same as the combination of a service name
/// and the domain where it runs.  (However, it's possible for
/// multiple instances of a service to run on a single domain).
#[derive(Debug, Clone)]
struct ServiceInstance {
    /// The unique bus address of the service mananager / listener.
    /// This address will not be used directly, since API calls are
    /// routed to generic service addresses, but it's helpful for
    /// differentiating service instances.
    address: BusAddress,

    /// Address where this instance will respond to API requests.
    listen_address: BusAddress,

    route_count: usize,

    /// When was this instance registered with the router.
    register_time: date::EgDate,
}

impl ServiceInstance {
    fn address(&self) -> &BusAddress {
        &self.address
    }
    fn listen_address(&self) -> &BusAddress {
        &self.listen_address
    }
    fn register_time(&self) -> &date::EgDate {
        &self.register_time
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            "route_count": self.route_count,
            "address": self.address().as_str(),
            "listen_address": self.listen_address().as_str(),
            "register_time": date::to_iso(self.register_time()),
        }
    }
}

/// A named service with a published API.
///
/// E.g. "opensrf.settings"
/// Models a service, which may have one or more registered ServiceInstance's.
#[derive(Debug, Clone)]
struct ServiceEntry {
    /// The service name
    name: String,

    /// Which specific instances of this service are registered.
    instances: Vec<ServiceInstance>,

    /// Allows us to round-robin through our instances.
    instance_index: usize,

    /// How many API requests have been routed to this service.
    route_count: usize,
}

impl ServiceEntry {
    fn name(&self) -> &str {
        &self.name
    }

    fn instances(&self) -> &Vec<ServiceInstance> {
        &self.instances
    }

    /// Returns the next round-robin service instanace and
    /// increments our route count if we have an instance to return.
    fn next_instance(&mut self) -> Option<&ServiceInstance> {
        if self.instance_index >= self.instances.len() {
            self.instance_index = 0;
        }

        let instance = match self.instances.get_mut(self.instance_index) {
            Some(i) => i,
            None => return None,
        };

        instance.route_count += 1;
        self.route_count += 1;

        // Now return the non-mut version
        self.instances.get(self.instance_index)
    }

    /// Remove a specific service instance from the set
    /// of registered instances.
    fn remove_instance(&mut self, address: &BusAddress) {
        if let Some(pos) = self
            .instances
            .iter()
            .position(|c| c.address().as_str().eq(address.as_str()))
        {
            log::debug!(
                "Removing instance for service={} address={}",
                self.name,
                address.as_str()
            );
            self.instances.remove(pos);
        } else {
            log::debug!(
                "Cannot remove unknown instance service={} address={}",
                self.name,
                address.as_str()
            );
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            "name": self.name(),
            "route_count": self.route_count,
            "instances": self.instances()
                .iter()
                .map(|s| s.to_json_value())
                .collect::<Vec<json::JsonValue>>()
        }
    }
}

/// One domain entry.
///
/// Every service, including all of its ServicEntry's, are linked to a
/// specific routable domain.  E.g. "public.localhost"
struct RouterDomain {
    /// The domain we route for.  e.g. public.localhost
    domain: String,

    /// Bus connection to the redis instance for this domain.
    ///
    /// A connection is only opened when needed.  Once opened, it's left
    /// open until the connection is shut down on the remote end.
    bus: Option<Bus>,

    /// How many requests have been routed to this domain.
    ///
    /// We count domain-level routing instead of service instance-level
    /// routing, since we can't guarantee which service instance will
    /// pick up any given request routed to a domain.
    route_count: usize,

    /// List of services registered with this router domain instance.
    services: Vec<ServiceEntry>,

    config: conf::BusClient,
}

impl RouterDomain {
    fn new(config: &conf::BusClient) -> Self {
        RouterDomain {
            domain: config.domain().name().to_string(),
            bus: None,
            route_count: 0,
            services: Vec::new(),
            config: config.clone(),
        }
    }

    fn domain(&self) -> &str {
        &self.domain
    }

    fn bus(&self) -> Option<&Bus> {
        self.bus.as_ref()
    }

    fn bus_mut(&mut self) -> Option<&mut Bus> {
        self.bus.as_mut()
    }

    fn route_count(&self) -> usize {
        self.route_count
    }

    fn services(&self) -> &Vec<ServiceEntry> {
        &self.services
    }

    /// Get a service by service name and increment our route count.
    fn get_service_mut(&mut self, name: &str) -> Option<&mut ServiceEntry> {
        let has_any = self.services.iter().any(|s| s.name().eq(name));

        if has_any {
            self.route_count += 1;
            self.services
                .iter_mut().find(|s| s.name().eq(name))
        } else {
            None
        }
    }

    /// Remove a service entry and its linked ServiceInstance's from
    /// our registered services.
    fn remove_service(&mut self, service: &str, address: &BusAddress) {
        if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
            let svc = self.services.get_mut(s_pos).unwrap(); // known OK
            svc.remove_instance(address);

            if svc.instances.is_empty() {
                log::debug!(
                    "Removing registration for service={} on removal of last instance address={}",
                    service,
                    address.as_str()
                );

                if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
                    self.services.remove(s_pos);
                }
            }
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            "domain": self.domain(),
            "route_count": self.route_count(),
            "services": self.services()
                .iter()
                .map(|s| s.to_json_value())
                .collect::<Vec<json::JsonValue>>()
        }
    }

    /// Connect to the Redis instance on our primary domain.
    fn connect(&mut self) -> EgResult<()> {
        if self.bus.is_some() {
            return Ok(());
        }

        let mut bus = match Bus::new(&self.config) {
            Ok(b) => b,
            Err(e) => return Err(format!("Cannot connect bus: {}", e).into()),
        };

        // We don't care about IDL-encoded information in the messages.
        // We just extract a bit of metadata and send it on.
        bus.set_raw_data_mode(true);

        self.bus = Some(bus);

        Ok(())
    }

    /// Send a message to this domain via our domain connection.
    fn send_to_domain(&mut self, tm: TransportMessage) -> EgResult<()> {
        log::trace!(
            "send_to_domain({}) routing message to {}",
            self.domain(),
            tm.to()
        );

        let bus = match &mut self.bus {
            Some(b) => b,
            None => Err(format!("We have no connection to domain {}", self.domain()))?,
        };

        bus.send(tm)
    }
}

/// Routes API requests from clients to services.
struct Router {
    /// Primary domain for this router instance.
    primary_domain: RouterDomain,

    /// Well-known address where API calls should be routed.
    listen_address: BusAddress,

    /// All other domains where services we care about are running.
    /// This value is empty by default and populates as service
    /// registrations arrive from other domains.
    remote_domains: Vec<RouterDomain>,

    /// Which domains can register services with us
    trusted_server_domains: Vec<String>,

    /// Which domains can send requests our way.
    trusted_client_domains: Vec<String>,
}

impl fmt::Display for Router {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Router for {}", self.primary_domain.domain())
    }
}

impl Router {
    /// Create a new router instance.
    ///
    /// * `domain` - Primary domain for this router instance.
    pub fn new(domain: &str) -> Self {
        log::info!("Starting router on domain: {domain}");

        let router_conf = match conf::config().get_router_conf(domain) {
            Some(rc) => rc,
            None => panic!("No router config for domain {}", domain),
        };

        let tsd = router_conf.trusted_server_domains().clone();
        let tcd = router_conf.trusted_client_domains().clone();

        let busconf = router_conf.client();

        let addr = BusAddress::for_router(busconf.username(), busconf.domain().name());
        let primary_domain = RouterDomain::new(busconf);

        log::info!("Router listening for requests at {}", addr.as_str());

        Router {
            primary_domain,
            trusted_server_domains: tsd,
            trusted_client_domains: tcd,
            listen_address: addr,
            remote_domains: Vec::new(),
        }
    }

    /// Connect to the opensrf message bus
    fn init(&mut self) -> EgResult<()> {
        self.primary_domain.connect()?;
        Ok(())
    }

    fn primary_domain(&self) -> &RouterDomain {
        &self.primary_domain
    }

    /// What other domains may we be forwarding requests to.
    fn remote_domains(&self) -> &Vec<RouterDomain> {
        &self.remote_domains
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            "listen_address": self.listen_address.as_str(),
            "primary_domain": self.primary_domain().to_json_value(),
            "remote_domains": self.remote_domains()
                .iter()
                .map(|s| s.to_json_value())
                .collect::<Vec<json::JsonValue>>()
        }
    }

    /// Find or create a new RouterDomain entry.
    fn find_or_create_domain(&mut self, domain: &str) -> EgResult<&mut RouterDomain> {
        if self.primary_domain.domain.eq(domain) {
            return Ok(&mut self.primary_domain);
        }

        let mut pos_op = self.remote_domains.iter().position(|d| d.domain.eq(domain));

        if pos_op.is_none() {
            log::debug!("Adding new RouterDomain for domain={}", domain);

            // Primary connection is required at this point.
            let mut busconf = conf::config().client().clone();
            busconf.set_domain(domain);

            self.remote_domains.push(RouterDomain::new(&busconf));

            pos_op = Some(self.remote_domains.len() - 1);
        }

        // Here the position is known to have data.
        Ok(self.remote_domains.get_mut(pos_op.unwrap()).unwrap())
    }

    /// Remove the service registration from the domain entry implied by the
    /// caller's address.
    fn handle_unregister(&mut self, address: &BusAddress, service: &str) -> EgResult<()> {
        let domain = address.domain();

        log::info!(
            "De-registering domain={} service={} address={}",
            domain,
            service,
            address.as_str()
        );

        if self.primary_domain.domain.eq(domain) {
            // When removing a service from the primary domain, leave the
            // domain as a whole intact since we'll likely need it again.
            // Remove services and instances as necessary, though.

            self.primary_domain.remove_service(service, address);
            return Ok(());
        }

        // When removing the last service from a remote domain, remove
        // the domain entry as a whole.
        let mut rem_pos_op: Option<usize> = None;
        let mut idx = 0;

        for r_domain in &mut self.remote_domains {
            if r_domain.domain().eq(domain) {
                r_domain.remove_service(service, address);
                if r_domain.services.is_empty() {
                    // Cannot remove here since it would be modifying
                    // self.remote_domains while it's aready mutably borrowed.
                    rem_pos_op = Some(idx);
                }
                break;
            }
            idx += 1;
        }

        if let Some(pos) = rem_pos_op {
            log::debug!("Removing cleared domain entry for domain={}", domain);
            self.remote_domains.remove(pos);
        }

        Ok(())
    }

    /// Add a service registration on the domain implied by the
    /// caller's bus address.
    ///
    /// The domain must be configured as a trusted server domain.
    fn handle_register(&mut self, address: BusAddress, service: &str) -> EgResult<()> {
        let domain = address.domain(); // Known to be a client addr.

        let trusted = self.trusted_server_domains.iter().any(|d| d == domain);

        if !trusted {
            return Err(format!(
                "Domain {} is not a trusted server domain for this router {} : {}",
                domain,
                address.as_str(),
                self
            )
            .into());
        }

        let r_domain = self.find_or_create_domain(domain)?;

        // Where our new instance will listen for routed API calls.
        // opensrf:service:$username:$domain:$service
        let listen_address = BusAddress::for_service(address.username(), address.domain(), service);

        for svc in &mut r_domain.services {
            // See if we have a ServiceEntry for this service on this domain.

            if svc.name.eq(service) {
                for instance in &mut svc.instances {
                    if instance.address.as_str().eq(address.as_str()) {
                        log::warn!(
                            "instance with address {} already registered for service {} and domain {}",
                            address.as_str(), service, domain
                        );
                        return Ok(());
                    }
                }

                log::debug!(
                    "Adding new ServiceInstance domain={} service={} address={}",
                    domain,
                    service,
                    address.as_str()
                );

                svc.instances.push(ServiceInstance {
                    address,
                    listen_address,
                    route_count: 0,
                    register_time: date::now(),
                });

                return Ok(());
            }
        }

        // We have no Service Entry for this domain+service+address.
        // Add a ServiceEntry and a new ServiceInstance

        log::debug!(
            "Adding new ServiceEntry domain={} service={} address={}",
            domain,
            service,
            address.as_str()
        );

        r_domain.services.push(ServiceEntry {
            name: service.to_string(),
            route_count: 0,
            instance_index: 0,
            instances: vec![ServiceInstance {
                address,
                listen_address,
                route_count: 0,
                register_time: date::now(),
            }],
        });

        Ok(())
    }

    /// List of currently active services by service name.
    fn _active_services(&self) -> Vec<&str> {
        let mut services: Vec<&str> = self
            .primary_domain()
            .services()
            .iter()
            .map(|s| s.name())
            .collect();

        for d in self.remote_domains() {
            for s in d.services() {
                if !services.contains(&s.name()) {
                    services.push(s.name());
                }
            }
        }

        services
    }

    /// Listen for inbound messages and dispatch each as needed.
    /// Returns true if the caller should restart the router after exit.
    /// Return false if this is a clean / intentional exit.
    fn listen(&mut self) -> bool {
        // Listen for inbound requests / router commands on our primary
        // domain and route accordingly.

        loop {
            let tm = match self.recv_one() {
                Ok(m) => m,
                Err(s) => {
                    log::error!("Exiting. Error receiving data from primary connection: {s}");
                    return true;
                }
            };

            if let Err(s) = self.route_message(tm) {
                log::error!("Error routing message: {}", s);
            }
        }
    }

    /// Route the provided transport message to the destination service
    /// or process as a router command.
    fn route_message(&mut self, tm: TransportMessage) -> EgResult<()> {
        let to = tm.to();

        log::debug!(
            "Router at {} received message destined for {to}",
            self.primary_domain.domain()
        );

        let addr = BusAddress::from_str(to)?;

        if addr.is_service() {
            self.route_api_request(&addr, tm)
        } else if addr.is_router() {
            return self.handle_router_command(tm);
        } else {
            return Err(format!("Unexpected message recipient: {}", to).into());
        }
    }

    /// Route an API call request to the desired service.
    ///
    /// If the request can be routed locally, do so, otherwise send
    /// the request to one of our remote domains where the service
    /// is registered.
    fn route_api_request(
        &mut self,
        to_addr: &BusAddress,
        mut tm: TransportMessage,
    ) -> EgResult<()> {
        let service = to_addr
            .service()
            .ok_or(format!("Invalid service address: {to_addr}"))?;

        if service.eq("router") {
            return self.handle_router_api_request(tm);
        }

        let client_addr = BusAddress::from_str(tm.from())?;
        let client_domain = client_addr.domain();

        let trusted = self
            .trusted_client_domains
            .iter()
            .any(|d| d == client_domain);

        if !trusted {
            return Err(format!(
                r#"Domain {client_domain} is not a trusted client domain for this
                router {client_addr} : {self}"#
            )
            .into());
        }

        // The recipient address for a routed API call will not include
        // the username or domain of the recipient, trusting that the
        // router will determine the best destination.  Chose a service
        // instance destination below and use its listen_address as the
        // destination.

        if let Some(svc) = self.primary_domain.get_service_mut(service) {
            if let Some(instance) = svc.next_instance() {
                tm.set_to(instance.listen_address().as_str());
                return self.primary_domain.send_to_domain(tm);
            }
        }

        for r_domain in &mut self.remote_domains {
            let has_bus = r_domain.bus.is_some();

            if let Some(svc) = r_domain.get_service_mut(service) {
                if let Some(instance) = svc.next_instance() {
                    tm.set_to(instance.listen_address().as_str());

                    if !has_bus {
                        // We only connect to remote domains when it's
                        // time to send them a message.
                        r_domain.connect()?;
                    }

                    return r_domain.send_to_domain(tm);
                }
            }
        }

        log::error!(
            "Router at {} has no service instances for service {service}",
            self.primary_domain.domain()
        );

        let payload = Payload::Status(Status::new(
            MessageStatus::ServiceNotFound,
            &format!("Service {service} not found"),
            "osrfServiceException",
        ));

        let mut trace = 0;
        if let Some(body) = tm.body().first() {
            // It would be odd, but not impossible to receive a
            // transport message destined for a service that has no
            // messages body.
            trace = body.thread_trace();
        }

        let from = match self.primary_domain.bus() {
            Some(b) => b.address().as_str(),
            None => self.listen_address.as_str(),
        };

        let tm = TransportMessage::with_body(
            tm.from(), // Recipient.  Bounce it back.
            from,
            tm.thread(),
            Message::new(MessageType::Status, trace, payload),
        );

        // Bounce-backs will always be directed back to a client
        // on our primary domain, since clients only ever talk to
        // the router on their own domain.
        self.primary_domain.send_to_domain(tm)
    }

    /// Some Router requests are packaged as method calls.  Handle those here.
    fn handle_router_api_request(&mut self, tm: TransportMessage) -> EgResult<()> {
        let from = tm.from();

        for msg in tm.body().iter() {
            let method = match msg.payload() {
                Payload::Method(m) => m,
                _ => {
                    return Err(format!(
                        "Router cannot process message: {}",
                        tm.into_json_value().clone().dump()
                    )
                    .into())
                }
            };

            let value = self.process_router_api_request(method)?;

            let reply = Message::new(
                MessageType::Result,
                msg.thread_trace(),
                Payload::Result(message::Result::new(
                    MessageStatus::Ok,
                    "OK",
                    "osrfResult",
                    EgValue::from_json_value(value)?,
                )),
            );

            let myaddr = match &self.primary_domain.bus {
                Some(b) => b.address(),
                None => return Err("Primary domain has no bus!".to_string().into()),
            };

            let mut tmsg = TransportMessage::with_body(from, myaddr.as_str(), tm.thread(), reply);

            tmsg.body_mut().push(Message::new(
                MessageType::Status,
                msg.thread_trace(),
                Payload::Status(message::Status::new(
                    MessageStatus::Complete,
                    "Request Complete",
                    "osrfStatus",
                )),
            ));

            self.primary_domain.send_to_domain(tmsg)?;
        }

        Ok(())
    }

    fn process_router_api_request(&mut self, m: &message::MethodCall) -> EgResult<json::JsonValue> {
        match m.method() {
            "opensrf.router.info.class.list" => {
                // Caller wants a list of service names

                let names: Vec<&str> = self
                    .primary_domain
                    .services()
                    .iter()
                    .map(|s| s.name())
                    .collect();

                Ok(json::from(names))
            }
            "opensrf.router.info.summarize" => Ok(self.to_json_value()),
            _ => Err(format!("Router cannot handle api {}", m.method()).into()),
        }
    }

    /// Register, Un-Register, etc. services
    fn handle_router_command(&mut self, tm: TransportMessage) -> EgResult<()> {
        let router_command = match tm.router_command() {
            Some(s) => s,
            None => {
                return Err(
                    format!("No router command present: {}", tm.into_json_value().dump()).into(),
                );
            }
        };

        let from = tm.from();

        let from_addr = BusAddress::from_str(from)?;

        log::debug!(
            "Router command received command={} from={}",
            router_command,
            from
        );

        let router_class = tm
            .router_class()
            .ok_or_else(|| format!("Message has no router class: {tm:?}"))?;

        match router_command {
            "register" => self.handle_register(from_addr, router_class),
            "unregister" => self.handle_unregister(&from_addr, router_class),
            _ => {
                log::warn!("{self} unknown router command: {router_command}");
                Ok(())
            }
        }
    }

    /// Receive the next message destined for this router on this
    /// domain, breaking periodically to check for shutdown, etc.
    /// signals.
    fn recv_one(&mut self) -> EgResult<TransportMessage> {
        let bus = self
            .primary_domain
            .bus_mut()
            .expect("We always maintain a connection on the primary domain");

        loop {
            // Break periodically
            let tm_op = bus.recv(POLL_TIMEOUT, Some(self.listen_address.as_str()))?;

            if let Some(tm) = tm_op {
                return Ok(tm);
            }
        }
    }
}

fn main() {
    // Prefer router-specific logging to the default client logging
    let init_ops = init::InitOptions {
        skip_logging: true,
        skip_host_settings: true,
        appname: Some(String::from("router")),
    };

    init::with_options(&init_ops).unwrap();

    let config = conf::config();

    let mut domains = match env::var("OSRF_ROUTER_DOMAIN") {
        Ok(v) => v.split(',').map(str::to_string).collect(),
        _ => Vec::new(),
    };

    if domains.is_empty() {
        domains = config
            .routers()
            .iter()
            .map(|r| r.client().domain().name().to_string())
            .collect();

        if domains.is_empty() {
            panic!("Router requries at least one domain");
        }
    }

    println!("Starting router for domains: {domains:?}");

    // Our global Logger is configured with the settings for the
    // router for the first domain found.
    let domain0 = &domains[0];
    let rconf = match config.get_router_conf(domain0) {
        Some(c) => c,
        None => panic!("No router configuration found for domain {}", domain0),
    };

    if let Err(e) = Logger::new(rconf.client().logging()).unwrap().init() {
        panic!("Error initializing logger: {}", e);
    }

    // A router for each specified domain runs within its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    for domain in domains.iter() {
        threads.push(start_one_domain(domain.to_string()));
    }

    // Block here while the routers are running.
    for thread in threads {
        thread.join().ok();
    }
}

fn start_one_domain(domain: String) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            // A router instance will exit if it encounters a
            // non-recoverable bus error.  This can happen, e.g., when
            // resetting the message bus.  Sleep a moment then try
            // to reconnect.  The sleep has a secondary benefit of
            // preventing a flood of repeating error logs.

            let mut router = Router::new(&domain);

            // If init() fails, we're done for.  Let it panic.
            router.init().unwrap();

            if router.listen() {
                log::warn!("Router waiting then restarting after bus disconnect");
                thread::sleep(Duration::from_secs(3));
            } else {
                log::info!("Router exiting");
                break;
            }
        }
    })
}
