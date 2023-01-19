use chrono::prelude::{DateTime, Local};
use getopts;
use opensrf::addr::{BusAddress, ClientAddress, RouterAddress, ServiceAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::init;
use opensrf::logging::Logger;
use opensrf::message;
use opensrf::message::{Message, MessageStatus, MessageType, Payload, Status, TransportMessage};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// A service controller.
///
/// This is what we traditionally call a "Listener" in OpenSRF.
/// A service can have multiple controllers on a single domain.
#[derive(Debug, Clone)]
struct ServiceInstance {
    address: ClientAddress,
    register_time: DateTime<Local>,
}

impl ServiceInstance {
    fn address(&self) -> &ClientAddress {
        &self.address
    }

    fn register_time(&self) -> &DateTime<Local> {
        &self.register_time
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            address: json::from(self.address().full()),
            register_time: json::from(self.register_time().to_rfc3339()),
        }
    }
}

/// An API-producing service.
///
/// E.g. "opensrf.settings"
#[derive(Debug, Clone)]
struct ServiceEntry {
    name: String,
    controllers: Vec<ServiceInstance>,
}

impl ServiceEntry {
    fn name(&self) -> &str {
        &self.name
    }

    fn controllers(&self) -> &Vec<ServiceInstance> {
        &self.controllers
    }

    fn remove_controller(&mut self, address: &ClientAddress) {
        if let Some(pos) = self
            .controllers
            .iter()
            .position(|c| c.address().full().eq(address.full()))
        {
            log::debug!(
                "Removing controller for service={} address={}",
                self.name,
                address
            );
            self.controllers.remove(pos);
        } else {
            log::debug!(
                "Cannot remove unknown controller service={} address={}",
                self.name,
                address
            );
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            name: json::from(self.name()),
            controllers: json::from(
                self.controllers().iter()
                    .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }
}

/// One domain entry.
///
/// A domain will typically host multiple services.
/// E.g. "public.localhost"
struct Routerdomain {
    // e.g. public.localhost
    domain: String,

    /// Bus connection to the redis instance for this domain.
    ///
    /// A connection is only opened when needed.  Once opened, it's left
    /// open until the connection is shut down on the remote end.
    bus: Option<Bus>,

    /// How many requests have been routed to this domain.
    ///
    /// We count domain-level routing instead of service controller-level
    /// routing, since we can't guarantee which service controller will
    /// pick up any given request routed to a domain.
    route_count: usize,

    services: Vec<ServiceEntry>,

    config: conf::BusClient,
}

impl Routerdomain {
    fn new(config: &conf::BusClient) -> Self {
        Routerdomain {
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

    fn has_service(&self, name: &str) -> bool {
        return self.services.iter().filter(|s| s.name().eq(name)).count() > 0;
    }

    fn remove_service(&mut self, service: &str, address: &ClientAddress) {
        if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
            let svc = self.services.get_mut(s_pos).unwrap(); // known OK
            svc.remove_controller(address);

            if svc.controllers.len() == 0 {
                log::debug!(
                    "Removing registration for service={} on removal of last controller address={}",
                    service,
                    address
                );

                if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
                    self.services.remove(s_pos);
                }
            }
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            domain: json::from(self.domain()),
            route_count: json::from(self.route_count()),
            services: json::from(self.services().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Connect to the Redis instance on this domain.
    fn connect(&mut self) -> Result<(), String> {
        if self.bus.is_some() {
            return Ok(());
        }

        let bus = match Bus::new(&self.config) {
            Ok(b) => b,
            Err(e) => return Err(format!("Cannot connect bus: {}", e)),
        };

        self.bus = Some(bus);

        Ok(())
    }

    /// Send a message to this domain via our domain connection.
    fn send_to_domain(&mut self, tm: TransportMessage) -> Result<(), String> {
        log::trace!(
            "send_to_domain({}) routing message to {}",
            self.domain(),
            tm.to()
        );

        let bus = match &mut self.bus {
            Some(b) => b,
            None => {
                return Err(format!("We have no connection to domain {}", self.domain()));
            }
        };

        bus.send(&tm)
    }
}

struct Router {
    /// Primary domain for this router instance.
    primary_domain: Routerdomain,

    /// Well-known address where top-level API calls should be routed.
    listen_address: RouterAddress,

    remote_domains: Vec<Routerdomain>,

    config: Arc<conf::Config>,
}

impl Router {
    pub fn new(config: Arc<conf::Config>, domain: &str) -> Self {
        log::info!("Starting router on domain: {domain}");

        let busconf = match config.get_router_conf(domain) {
            Some(rc) => rc.client(),
            None => panic!("No router config for domain {}", domain),
        };

        let domain = busconf.domain().name().to_string();
        let addr = RouterAddress::new(&domain);
        let primary_domain = Routerdomain::new(&busconf);

        Router {
            config,
            primary_domain,
            listen_address: addr,
            remote_domains: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<(), String> {
        self.primary_domain.connect()?;
        Ok(())
    }

    fn primary_domain(&self) -> &Routerdomain {
        &self.primary_domain
    }

    fn remote_domains(&self) -> &Vec<Routerdomain> {
        &self.remote_domains
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            listen_address: json::from(self.listen_address.full()),
            primary_domain: self.primary_domain().to_json_value(),
            remote_domains: json::from(self.remote_domains().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Find or create a new Routerdomain entry.
    fn find_or_create_domain(&mut self, domain: &str) -> Result<&mut Routerdomain, String> {
        if self.primary_domain.domain.eq(domain) {
            return Ok(&mut self.primary_domain);
        }

        let mut pos_op = self.remote_domains.iter().position(|d| d.domain.eq(domain));

        if pos_op.is_none() {
            log::debug!("Adding new Routerdomain for domain={}", domain);

            // Primary connection is required at this point.
            let mut busconf = self.config.client().clone();
            busconf.set_domain(domain);

            self.remote_domains.push(Routerdomain::new(&busconf));

            pos_op = Some(self.remote_domains.len() - 1);
        }

        // Here the position is known to have data.
        Ok(self.remote_domains.get_mut(pos_op.unwrap()).unwrap())
    }

    fn handle_unregister(&mut self, address: &ClientAddress, service: &str) -> Result<(), String> {
        let domain = address.domain();

        log::info!(
            "De-registering domain={} service={} address={}",
            domain,
            service,
            address
        );

        if self.primary_domain.domain.eq(domain) {
            // When removing a service from the primary domain, leave the
            // domain as a whole intact since we'll likely need it again.
            // Remove services and controllers as necessary, though.

            self.primary_domain.remove_service(service, &address);
            return Ok(());
        }

        // When removing the last service from a remote domain, remove
        // the domain entry as a whole.
        let mut rem_pos_op: Option<usize> = None;
        let mut idx = 0;

        for r_domain in &mut self.remote_domains {
            if r_domain.domain().eq(domain) {
                r_domain.remove_service(service, address);
                if r_domain.services.len() == 0 {
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

    fn handle_register(&mut self, address: ClientAddress, service: &str) -> Result<(), String> {
        let domain = address.domain(); // Known to be a client addr.

        let r_domain = self.find_or_create_domain(domain)?;

        for svc in &mut r_domain.services {
            // See if we have a ServiceEntry for this service on this domain.

            if svc.name.eq(service) {
                for controller in &mut svc.controllers {
                    if controller.address.full().eq(address.full()) {
                        log::warn!(
                            "Controller with address {} already registered for service {} and domain {}",
                            address, service, domain
                        );
                        return Ok(());
                    }
                }

                log::debug!(
                    "Adding new ServiceInstance domain={} service={} address={}",
                    domain,
                    service,
                    address
                );

                svc.controllers.push(ServiceInstance {
                    address: address.clone(),
                    register_time: Local::now(),
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
            address
        );

        r_domain.services.push(ServiceEntry {
            name: service.to_string(),
            controllers: vec![ServiceInstance {
                address: address,
                register_time: Local::now(),
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

        return services;
    }

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

        // This will be reachable once we implement graceful shutdown.
        //return false;
    }

    fn route_message(&mut self, tm: TransportMessage) -> Result<(), String> {
        let to = tm.to();

        log::debug!(
            "Router at {} received message destined for {to}",
            self.primary_domain.domain()
        );

        let addr = BusAddress::new_from_string(to)?;

        if addr.is_service() {
            let addr = ServiceAddress::from_addr(addr)?;
            return self.route_api_request(&addr, tm);
        } else if addr.is_router() {
            return self.handle_router_command(tm);
        } else {
            return Err(format!("Unexpected message recipient: {}", to));
        }
    }

    fn route_api_request(
        &mut self,
        to_addr: &ServiceAddress,
        tm: TransportMessage,
    ) -> Result<(), String> {
        let service = to_addr.service();

        if service.eq("router") {
            return self.handle_router_api_request(tm);
        }

        if self.primary_domain.has_service(service) {
            self.primary_domain.route_count += 1;
            return self.primary_domain.send_to_domain(tm);
        }

        for r_domain in &mut self.remote_domains {
            if r_domain.has_service(service) {
                if r_domain.bus.is_none() {
                    // We only connect to remote domains when it's
                    // time to send them a message.
                    r_domain.connect()?;
                }

                r_domain.route_count += 1;
                return r_domain.send_to_domain(tm);
            }
        }

        log::error!(
            "Router at {} has no service controllers for service {service}",
            self.primary_domain.domain()
        );

        let payload = Payload::Status(Status::new(
            MessageStatus::ServiceNotFound,
            &format!("Service {service} not found"),
            "osrfServiceException",
        ));

        let mut trace = 0;
        if tm.body().len() > 0 {
            // It would be odd, but not impossible to receive a
            // transport message destined for a service that has no
            // messages in its body.
            trace = tm.body()[0].thread_trace();
        }

        let from = match self.primary_domain.bus() {
            Some(b) => b.address().full(),
            None => self.listen_address.full(),
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
    fn handle_router_api_request(&mut self, tm: TransportMessage) -> Result<(), String> {
        let from = tm.from();

        for msg in tm.body().iter() {
            let method = match msg.payload() {
                Payload::Method(m) => m,
                _ => {
                    return Err(format!(
                        "Router cannot process message: {}",
                        tm.to_json_value().dump()
                    ))
                }
            };

            let value = self.process_router_api_request(&method)?;

            let reply = Message::new(
                MessageType::Result,
                msg.thread_trace(),
                Payload::Result(message::Result::new(
                    MessageStatus::Ok,
                    "osrfResponse",
                    "OK",
                    value,
                )),
            );

            let myaddr = match &self.primary_domain.bus {
                Some(b) => b.address(),
                None => return Err(format!("Primary domain has no bus!")),
            };

            let mut tmsg = TransportMessage::with_body(from, myaddr.full(), tm.thread(), reply);

            tmsg.body_as_mut().push(Message::new(
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

    fn process_router_api_request(
        &mut self,
        m: &message::Method,
    ) -> Result<json::JsonValue, String> {
        match m.method() {
            "opensrf.router.info.class.list" => {
                // Caller wants a list of service names

                let names: Vec<String> = self
                    .primary_domain
                    .services()
                    .iter()
                    .map(|s| s.name().to_string())
                    .collect();

                Ok(json::from(names))
            }
            _ => Err(format!("Router cannot handle api {}", m.method())),
        }
    }

    fn handle_router_command(&mut self, tm: TransportMessage) -> Result<(), String> {
        let router_command = match tm.router_command() {
            Some(s) => s,
            None => {
                return Err(format!(
                    "No router command present: {}",
                    tm.to_json_value().dump()
                ));
            }
        };

        let from = tm.from();

        let from_addr = ClientAddress::from_string(from)?;

        log::debug!(
            "Router command received command={} from={}",
            router_command,
            from
        );

        // Not all router commands require a router class.
        let router_class = || {
            if let Some(rc) = tm.router_class() {
                return Ok(rc);
            } else {
                return Err(format!(
                    "Message has no router class: {}",
                    tm.to_json_value().dump()
                ));
            }
        };

        match router_command {
            "register" => self.handle_register(from_addr, router_class()?),
            "unregister" => self.handle_unregister(&from_addr, router_class()?),
            _ => self.deliver_information(from_addr, tm),
        }
    }

    /// Deliver stats, etc. to clients that request it.
    fn deliver_information(
        &mut self,
        from_addr: ClientAddress,
        mut tm: TransportMessage,
    ) -> Result<(), String> {
        let router_command = tm.router_command().unwrap(); // known exists
        log::debug!("Handling info router command : {router_command}");

        match router_command {
            "summarize" => tm.set_router_reply(&self.to_json_value().dump()),
            _ => {
                return Err(format!("Unsupported router command: {router_command}"));
            }
        }

        // Bounce the message back to the caller with the requested data.
        // Should our FROM address be our unique bus address or the router
        // address? Does it matter?
        tm.set_from(self.primary_domain.bus().unwrap().address().full());
        tm.set_to(from_addr.full());

        let r_domain = self.find_or_create_domain(from_addr.domain())?;

        if r_domain.bus.is_none() {
            r_domain.connect()?;
        }

        r_domain.send_to_domain(tm)
    }

    fn recv_one(&mut self) -> Result<TransportMessage, String> {
        let bus = self
            .primary_domain
            .bus_mut()
            .expect("We always maintain a connection on the primary domain");

        loop {
            // Break periodically
            if let Some(tm) = bus.recv(10, Some(self.listen_address.full()))? {
                return Ok(tm);
            }
        }
    }
}

// TODO notify connected service coordinators when we shut down?

fn main() {
    let mut ops = getopts::Options::new();

    ops.optmulti("d", "domain", "Domain", "DOMAIN");

    let initops = init::InitOptions { skip_logging: true };

    let (config, params) = init::init_with_more_options(&mut ops, &initops).unwrap();

    let config = config.into_shared();

    let mut domains = params.opt_strs("domain");

    if domains.len() == 0 {
        domains = config
            .routers()
            .iter()
            .map(|r| r.client().domain().name().to_string())
            .collect();

        if domains.len() == 0 {
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
        threads.push(start_one_domain(config.clone(), domain.to_string()));
    }

    // Block here while the routers are running.
    for thread in threads {
        thread.join().ok();
    }
}

fn start_one_domain(conf: Arc<conf::Config>, domain: String) -> thread::JoinHandle<()> {
    return thread::spawn(move || {
        loop {
            // A router instance will exit if it encounters a
            // non-recoverable bus error.  This can happen, e.g., when
            // resetting the message bus.  Sleep a moment then try
            // to reconnect.  The sleep has a secondary benefit of
            // preventing a flood of repeating error logs.

            let mut router = Router::new(conf.clone(), &domain);

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
    });
}
