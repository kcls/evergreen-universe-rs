use gethostname::gethostname;
use roxmltree;
use std::fmt;
use std::fs;
use std::str::FromStr;
use std::sync::OnceLock;
use syslog;

static GLOBAL_OSRF_CONFIG: OnceLock<Config> = OnceLock::new();

/// Returns a ref to the globab OpenSRF config.
///
/// Panics if no configuration has been loaded.
pub fn config() -> &'static Config {
    if let Some(conf) = GLOBAL_OSRF_CONFIG.get() {
        conf
    } else {
        log::error!("OpenSRF Config Required");
        panic!("OpenSRF Config Required")
    }
}

const DEFAULT_BUS_PORT: u16 = 6379;

#[derive(Debug, Clone, PartialEq)]
pub enum LogFile {
    Syslog,
    Stdout,
    Filename(String),
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    log_level: Option<log::LevelFilter>,
    log_file: Option<LogFile>,
    syslog_facility: Option<syslog::Facility>,
    activity_log_facility: Option<syslog::Facility>,
}

impl LogOptions {
    pub fn syslog_facility(&self) -> Option<syslog::Facility> {
        self.syslog_facility
    }
    pub fn set_syslog_facility(&mut self, facility: &str) -> Result<(), String> {
        if let Ok(ff) = syslog::Facility::from_str(facility) {
            self.syslog_facility = Some(ff);
            Ok(())
        } else {
            Err(format!("Invalid syslog facility string: {facility}"))
        }
    }

    pub fn activity_log_facility(&self) -> Option<syslog::Facility> {
        self.activity_log_facility
    }
    pub fn log_file(&self) -> &Option<LogFile> {
        &self.log_file
    }
    pub fn set_log_file(&mut self, file: LogFile) {
        self.log_file = Some(file);
    }
    pub fn log_level(&self) -> &Option<log::LevelFilter> {
        &self.log_level
    }
    pub fn set_log_level(&mut self, level: &str) {
        self.log_level = Some(LogOptions::log_level_from_str(level));
    }

    /// Maps log levels as defined in the OpenSRF core configuration
    /// file to syslog levels.
    ///
    /// Defaults to Info
    pub fn log_level_from_str(level: &str) -> log::LevelFilter {
        match level {
            "1" | "error" => log::LevelFilter::Error,
            "2" | "warn" => log::LevelFilter::Warn,
            "3" | "info" => log::LevelFilter::Info,
            "4" | "debug" => log::LevelFilter::Debug,
            "5" | "trace" => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info,
        }
    }
}

/// A single message bus endpoint domain/host.
#[derive(Debug, Clone)]
pub struct BusDomain {
    name: String,
    port: u16,
}

impl BusDomain {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for BusDomain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.port)
    }
}

/// A set of bus login credentials
#[derive(Debug, Clone)]
pub struct BusClient {
    username: String,
    password: String,
    router_name: String,
    domain: BusDomain,
    logging: LogOptions,
    settings_config: Option<String>,
    routers: Vec<ClientRouter>,
}

impl BusClient {
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
    pub fn domain(&self) -> &BusDomain {
        &self.domain
    }
    /// Name of the router running on our domain.
    pub fn router_name(&self) -> &str {
        &self.router_name
    }
    pub fn logging(&self) -> &LogOptions {
        &self.logging
    }
    pub fn logging_mut(&mut self) -> &mut LogOptions {
        &mut self.logging
    }
    pub fn settings_config(&self) -> Option<&str> {
        self.settings_config.as_deref()
    }
    pub fn routers(&self) -> &Vec<ClientRouter> {
        &self.routers
    }
    pub fn set_domain(&mut self, domain: &str) {
        // Assumes other aspects of the domain are identical
        self.domain.name = domain.to_string();
    }
    pub fn set_username(&mut self, username: &str) {
        self.username = username.to_string();
    }
    pub fn set_password(&mut self, password: &str) {
        self.password = password.to_string();
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouter {
    domain: String,
    username: String,
    services: Option<Vec<String>>,
}
impl ClientRouter {
    pub fn services(&self) -> Option<&Vec<String>> {
        self.services.as_ref()
    }
    pub fn domain(&self) -> &str {
        &self.domain
    }
    /// "name" in opensrf_core.xml
    pub fn username(&self) -> &str {
        &self.username
    }
}

#[derive(Debug, Clone)]
pub struct Router {
    client: BusClient,
    trusted_server_domains: Vec<String>,
    trusted_client_domains: Vec<String>,
}

impl Router {
    pub fn client(&self) -> &BusClient {
        &self.client
    }
    pub fn client_mut(&mut self) -> &mut BusClient {
        &mut self.client
    }
    pub fn trusted_server_domains(&self) -> &Vec<String> {
        &self.trusted_server_domains
    }
    pub fn trusted_client_domains(&self) -> &Vec<String> {
        &self.trusted_client_domains
    }
}

#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    client: Option<BusClient>,
    routers: Vec<Router>,
    gateway: Option<BusClient>,
    log_protect: Vec<String>,
}

impl ConfigBuilder {
    pub fn build(self) -> Result<Config, String> {
        if self.client.is_none() {
            return Err("Config has no client settings".to_string());
        }

        Ok(Config {
            hostname: Config::get_os_hostname()?,
            client: self.client.unwrap(),
            routers: self.routers,
            gateway: self.gateway,
            log_protect: self.log_protect,
        })
    }

    /// Load configuration from a YAML file.
    ///
    /// May panic on invalid values (e.g. invalid log level) or unexpected
    /// Yaml config structures.
    pub fn from_file(filename: &str) -> Result<Self, String> {
        match fs::read_to_string(filename) {
            Ok(text) => ConfigBuilder::from_xml_string(&text),
            Err(e) => Err(format!(
                "Error reading configuration file: file='{}' {:?}",
                filename, e
            )),
        }
    }

    pub fn from_xml_string(xml: &str) -> Result<Self, String> {
        let doc = roxmltree::Document::parse(xml).map_err(|e| format!("Error parsing XML: {e}"))?;

        let conf_node = match doc.root().children().find(|n| n.has_tag_name("config")) {
            Some(n) => n,
            None => Err("Missing 'config' element".to_string())?,
        };

        let mut builder = ConfigBuilder {
            client: None,
            gateway: None,
            routers: Vec::new(),
            log_protect: Vec::new(),
        };

        // Start with the Client portion, which will contain values
        // for all connections.
        for node in conf_node.children() {
            match node.tag_name().name() {
                "opensrf" => builder.unpack_opensrf_node(&node)?,
                "routers" => builder.unpack_routers(&node)?,
                "gateway" => builder.unpack_gateway(&node)?,
                "shared" => builder.unpack_shared(&node)?,
                _ => {} // ignore
            }
        }

        Ok(builder)
    }

    fn unpack_gateway(&mut self, node: &roxmltree::Node) -> Result<(), String> {
        self.gateway = Some(self.unpack_client_node(node)?);
        Ok(())
    }

    fn unpack_shared(&mut self, node: &roxmltree::Node) -> Result<(), String> {
        if let Some(lp) = node.children().find(|c| c.has_tag_name("log_protect")) {
            for ms in lp.children().filter(|c| c.has_tag_name("match_string")) {
                if let Some(t) = ms.text() {
                    self.log_protect.push(t.to_string());
                }
            }
        }

        Ok(())
    }

    fn unpack_routers(&mut self, node: &roxmltree::Node) -> Result<(), String> {
        for rnode in node.children().filter(|n| n.has_tag_name("router")) {
            // Router client configs are (mostly) nested in a <transport> element.
            let tnode = match rnode.children().find(|c| c.has_tag_name("transport")) {
                Some(tn) => tn,
                None => Err("Routers require a transport config".to_string())?,
            };

            let mut client = self.unpack_client_node(&tnode)?;

            // The logging configs for the routers sits outside its
            // transport node.
            client.logging = self.unpack_logging_node(&rnode)?;

            let mut router = Router {
                client,
                trusted_server_domains: Vec::new(),
                trusted_client_domains: Vec::new(),
            };

            for tdnode in rnode
                .children()
                .filter(|d| d.has_tag_name("trusted_domains"))
            {
                for snode in tdnode.children().filter(|d| d.has_tag_name("server")) {
                    if let Some(domain) = snode.text() {
                        router.trusted_server_domains.push(domain.to_string());
                    }
                }
                for cnode in tdnode.children().filter(|d| d.has_tag_name("client")) {
                    if let Some(domain) = cnode.text() {
                        router.trusted_client_domains.push(domain.to_string());
                    }
                }
            }

            self.routers.push(router);
        }

        Ok(())
    }

    fn child_node_text(&self, node: &roxmltree::Node, name: &str) -> Option<String> {
        if let Some(tnode) = node.children().find(|n| n.has_tag_name(name)) {
            if let Some(text) = tnode.text() {
                return Some(text.to_string());
            }
        }
        None
    }

    fn unpack_opensrf_node(&mut self, node: &roxmltree::Node) -> Result<(), String> {
        let mut client = self.unpack_client_node(node)?;

        if let Some(routers) = node.children().find(|c| c.has_tag_name("routers")) {
            for rnode in routers.children().filter(|r| r.has_tag_name("router")) {
                self.unpack_client_router_node(&mut client, &rnode)?;
            }
        }

        self.client = Some(client);

        Ok(())
    }

    fn unpack_client_router_node(
        &mut self,
        client: &mut BusClient,
        rnode: &roxmltree::Node,
    ) -> Result<(), String> {
        let domain = match self.child_node_text(rnode, "domain") {
            Some(d) => d.to_string(),
            None => Err(format!("Client router node has no domain: {rnode:?}"))?,
        };

        let username = match self.child_node_text(rnode, "name") {
            Some(d) => d.to_string(),
            None => Err(format!("Client router node has no name: {rnode:?}"))?,
        };

        let mut cr = ClientRouter {
            domain,
            username,
            services: None,
        };

        if let Some(services) = rnode.children().find(|n| n.has_tag_name("services")) {
            let mut svclist = Vec::new();

            for snode in services.children().filter(|n| n.has_tag_name("service")) {
                if let Some(service) = snode.text() {
                    svclist.push(service.to_string());
                }
            }

            cr.services = Some(svclist);
        }

        client.routers.push(cr);

        Ok(())
    }

    fn unpack_client_node(&mut self, node: &roxmltree::Node) -> Result<BusClient, String> {
        let logging = self.unpack_logging_node(node)?;
        let domain = self.unpack_domain_node(node)?;

        let mut username = "";
        let mut password = "";
        let mut router_name = "router";
        let mut settings_config: Option<String> = None;

        for child in node.children() {
            match child.tag_name().name() {
                "username" => {
                    if let Some(t) = child.text() {
                        username = t;
                    }
                }
                "passwd" | "password" => {
                    if let Some(t) = child.text() {
                        password = t;
                    }
                }
                "router_name" => {
                    if let Some(t) = child.text() {
                        router_name = t;
                    }
                }
                "settings_config" => {
                    if let Some(t) = child.text() {
                        settings_config = Some(t.to_string());
                    }
                }
                _ => {}
            }
        }

        Ok(BusClient {
            domain,
            logging,
            settings_config,
            routers: Vec::new(),
            username: username.to_string(),
            password: password.to_string(),
            router_name: router_name.to_string(),
        })
    }

    fn unpack_domain_node(&mut self, node: &roxmltree::Node) -> Result<BusDomain, String> {
        let domain_name = match node.children().find(|c| c.has_tag_name("domain")) {
            Some(n) => match n.text() {
                Some(t) => t,
                None => return Err("'domain' node is empty".to_string()),
            },
            None => match node.children().find(|c| c.has_tag_name("server")) {
                Some(n) => match n.text() {
                    Some(t) => t,
                    None => return Err("'server' node is empty".to_string()),
                },
                None => return Err("Node has no domain or server".to_string()),
            },
        };

        let mut port = DEFAULT_BUS_PORT;
        if let Some(pnode) = node.children().find(|c| c.has_tag_name("port")) {
            if let Some(ptext) = pnode.text() {
                if let Ok(p) = ptext.parse::<u16>() {
                    port = p;
                }
            }
        }

        Ok(BusDomain {
            port,
            name: domain_name.to_string(),
        })
    }

    fn unpack_logging_node(&mut self, node: &roxmltree::Node) -> Result<LogOptions, String> {
        let mut ops = LogOptions {
            log_level: None,
            log_file: None,
            syslog_facility: None,
            activity_log_facility: None,
        };

        for child in node.children() {
            match child.tag_name().name() {
                "logfile" => {
                    if let Some(filename) = child.text() {
                        if filename.eq("syslog") {
                            ops.log_file = Some(LogFile::Syslog);
                        } else if filename.eq("stdout") {
                            ops.log_file = Some(LogFile::Stdout);
                        } else {
                            ops.log_file = Some(LogFile::Filename(filename.to_string()))
                        }
                    }
                }
                "syslog" => {
                    if let Some(f) = child.text() {
                        if let Ok(ff) = syslog::Facility::from_str(f) {
                            ops.syslog_facility = Some(ff);
                        }
                    }
                }
                "actlog" => {
                    if let Some(f) = child.text() {
                        if let Ok(ff) = syslog::Facility::from_str(f) {
                            ops.activity_log_facility = Some(ff);
                        }
                    }
                }
                "loglevel" => {
                    if let Some(level_num) = child.text() {
                        ops.log_level = Some(LogOptions::log_level_from_str(level_num));
                    }
                }
                _ => {}
            }
        }

        Ok(ops)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    hostname: String,
    client: BusClient,
    routers: Vec<Router>,
    gateway: Option<BusClient>,
    log_protect: Vec<String>,
}

impl Config {
    /// Put this Config into the global GLOBAL_OSRF_CONFIG.
    ///
    /// Returns Err if the Config has already been stored.
    pub fn store(self) -> Result<(), String> {
        if GLOBAL_OSRF_CONFIG.set(self).is_err() {
            Err("Cannot initialize OpenSRF Config more than once".to_string())
        } else {
            Ok(())
        }
    }

    pub fn routers(&self) -> &Vec<Router> {
        &self.routers
    }
    pub fn routers_mut(&mut self) -> Vec<&mut Router> {
        self.routers.iter_mut().collect()
    }

    pub fn log_protect(&self) -> &Vec<String> {
        &self.log_protect
    }

    pub fn gateway(&self) -> Option<&BusClient> {
        self.gateway.as_ref()
    }
    pub fn gateway_mut(&mut self) -> Option<&mut BusClient> {
        self.gateway.as_mut()
    }

    pub fn client(&self) -> &BusClient {
        &self.client
    }
    pub fn client_mut(&mut self) -> &mut BusClient {
        &mut self.client
    }
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn get_router_conf(&self, domain: &str) -> Option<&Router> {
        self.routers
            .iter()
            .find(|r| r.client().domain().name().eq(domain))
    }

    /// Manually override the OS hostname, e.g. with "localhost"
    pub fn set_hostname(&mut self, hostname: &str) {
        self.hostname = hostname.to_string();
    }

    fn get_os_hostname() -> Result<String, String> {
        match gethostname().into_string() {
            Ok(h) => Ok(h),
            Err(e) => Err(format!("Cannot read OS host name: {e:?}")),
        }
    }
}
