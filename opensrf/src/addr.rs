use super::util;
use gethostname::gethostname;
use std::fmt;
use std::process;

const BUS_ADDR_NAMESPACE: &str = "opensrf";

/// Models a bus-level address providing access to indivual components
/// of each address.
///
/// Examples:
///
/// opensrf:router:$username:$domain
/// opensrf:service:$username:$domain:$service
/// opensrf:client:$username:$domain:$hostname:$pid:$random
#[derive(Debug, Clone)]
pub struct BusAddress {
    /// Full raw address string
    full: String,

    domain: String,
    username: String,

    /// Only service addresses have a service name
    service: Option<String>,

    is_client: bool,
    is_service: bool,
    is_router: bool,
}

impl fmt::Display for BusAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Address={}", &self.full)
    }
}

impl BusAddress {
    /// Creates a new BusAddress from a bus address string.
    ///
    /// ```
    /// let addr =
    ///   opensrf::addr::BusAddress::from_str("opensrf:client:foobar:localhost:12345")
    ///   .expect("Error creating address from string");
    ///
    /// assert!(addr.is_client());
    /// assert_eq!(addr.domain(), "localhost");
    /// ```
    pub fn from_str(full: &str) -> Result<Self, String> {
        let parts: Vec<&str> = full.split(':').collect();

        // Every address has 4 well-known parts, so we need that many at minimum.
        if parts.len() < 4 {
            return Err(format!("BusAddress bad format: {}", full));
        }

        let purpose = parts[1];
        let username = parts[2].to_owned();
        let domain = parts[3].to_owned();

        let mut addr = BusAddress {
            full: full.to_string(),
            domain: domain,
            username: username,
            service: None,
            is_client: false,
            is_service: false,
            is_router: false,
        };

        if purpose.eq("service") {
            if let Some(service) = parts.get(4) {
                addr.service = Some(service.to_string());
                addr.is_service = true;
            } else {
                return Err(format!("Invalid service address: {full}"));
            }

        } else if purpose.eq("client") {
            addr.is_client = true;
        } else if purpose.eq("router") {
            addr.is_router = true;
        } else {
            return Err(format!("Invalid bus address: {full}"));
        }

        Ok(addr)
    }

    /// Full address string
    pub fn as_str(&self) -> &str {
        &self.full
    }
    pub fn domain(&self) -> &str {
        &self.domain
    }
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn set_domain(&mut self, s: &str) {
        self.domain = s.to_string();
    }
    pub fn set_username(&mut self, s: &str) {
        self.username = s.to_string();
    }
    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }
    pub fn is_client(&self) -> bool {
        self.is_client
    }
    pub fn is_service(&self) -> bool {
        self.is_service
    }
    pub fn is_router(&self) -> bool {
        self.is_router
    }
}

#[derive(Debug, Clone)]
pub struct ClientAddress {
    addr: BusAddress,
}

impl ClientAddress {
    pub fn from_addr(addr: BusAddress) -> Result<Self, String> {
        if addr.is_client() {
            Ok(ClientAddress { addr })
        } else {
            Err(format!(
                "Cannot create a ClientAddress from a non-client BusAddress"
            ))
        }
    }

    pub fn from_string(full: &str) -> Result<Self, String> {
        let addr = BusAddress::from_str(full)?;
        if !addr.is_client() {
            return Err(format!("Invalid ClientAddress string: {full}"));
        }
        Ok(ClientAddress { addr })
    }

    pub fn as_str(&self) -> &str {
        self.addr.as_str()
    }

    /// Create a new ClientAddress for a domain.
    ///
    /// ```
    /// let username = "opensrf";
    /// let domain = "private.localhost";
    /// let addr = opensrf::addr::ClientAddress::new(username, domain);
    /// assert_eq!(addr.domain(), domain);
    /// assert!(addr.addr().is_client());
    /// ```
    pub fn new(username: &str, domain: &str) -> Self {
        let full = format!(
            "{}:client:{}:{}:{}:{}:{}",
            BUS_ADDR_NAMESPACE,
            username,
            domain,
            &gethostname().into_string().unwrap(),
            process::id(),
            &util::random_number(6)
        );

        ClientAddress {
            // Assumes the address string built above is valid.
            addr: BusAddress::from_str(&full).unwrap(),
        }
    }

    /// Allow the caller to provide the address content after the domain.
    ///
    /// ```
    /// let username = "opensrf";
    /// let domain = "private.localhost";
    /// let mut addr = opensrf::addr::ClientAddress::new(username, domain);
    /// assert_eq!(addr.domain(), domain);
    ///
    /// let remainder = "HELLO123";
    /// addr.set_remainder(remainder);
    /// assert!(addr.addr().is_client());
    /// assert!(addr.as_str().ends_with(remainder));
    /// ```
    pub fn set_remainder(&mut self, remainder: &str) {
        self.addr.full = format!(
            "{}:client:{}:{}:{}",
            BUS_ADDR_NAMESPACE,
            self.addr().username(),
            self.addr().domain(),
            remainder,
        );
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
    }
}

impl fmt::Display for ClientAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientAddress={}", self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct ServiceAddress {
    addr: BusAddress,
}

impl ServiceAddress {
    pub fn from_addr(addr: BusAddress) -> Result<Self, String> {
        if addr.is_service() {
            Ok(ServiceAddress { addr })
        } else {
            Err(format!(
                "Cannot create a ServiceAddress from a non-service BusAddress"
            ))
        }
    }

    pub fn from_string(full: &str) -> Result<Self, String> {
        let addr = BusAddress::from_str(full)?;
        if !addr.is_service() {
            return Err(format!("Invalid ServiceAddress string: {full}"));
        }
        Ok(ServiceAddress { addr })
    }

    pub fn as_str(&self) -> &str {
        self.addr.as_str()
    }

    /// Create a user/domain-agnostic service address.
    ///
    /// Service address are non domain-specific and refer generically
    /// to a service.
    ///
    /// ```
    /// let service = "opensrf.settings";
    /// let mut addr = opensrf::addr::ServiceAddress::new(service);
    /// assert_eq!(addr.service(), service);
    /// assert!(addr.addr().is_service());
    /// ```
    pub fn new(service: &str) -> Self {
        let full = format!("{}:service:_:_:{}", BUS_ADDR_NAMESPACE, &service);

        ServiceAddress {
            addr: BusAddress::from_str(&full).unwrap(),
        }
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
    }

    pub fn addr_mut(&mut self) -> &mut BusAddress {
        &mut self.addr
    }

    pub fn service(&self) -> &str {
        self.addr().service().unwrap()
    }
}

impl fmt::Display for ServiceAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ServiceAddress={}", self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct RouterAddress {
    addr: BusAddress,
}

impl RouterAddress {
    pub fn from_addr(addr: BusAddress) -> Result<Self, String> {
        if addr.is_router() {
            Ok(RouterAddress { addr })
        } else {
            Err(format!(
                "Cannot create a RouterAddress from a non-service BusAddress"
            ))
        }
    }

    /// Create a new router address from a string
    ///
    /// ```
    /// let addr_res = opensrf::addr::RouterAddress::from_string("foo:bar");
    /// assert!(addr_res.is_err());
    ///
    /// let addr_res = opensrf::addr::RouterAddress::from_string("opensrf:router:localhost");
    /// assert!(addr_res.is_ok());
    /// assert!(addr_res.unwrap().domain().eq("localhost"));
    /// ```
    pub fn from_string(full: &str) -> Result<Self, String> {
        let addr = BusAddress::from_str(full)?;
        if !addr.is_router() {
            return Err(format!("Invalid RouterAddress string: {full}"));
        }
        Ok(RouterAddress { addr })
    }

    pub fn as_str(&self) -> &str {
        self.addr.as_str()
    }

    /// Create a new router address from a domain.
    ///
    /// ```
    /// let addr = opensrf::addr::RouterAddress::new("router", "localhost");
    /// assert_eq!(addr.as_str(), "opensrf:router:router:localhost");
    /// ```
    pub fn new(username: &str, domain: &str) -> Self {
        let full = format!("{}:router:{}:{}", BUS_ADDR_NAMESPACE, username, domain);
        RouterAddress {
            addr: BusAddress::from_str(&full).unwrap(),
        }
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
    }
}

impl fmt::Display for RouterAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RouterAddress={}", self.as_str())
    }
}
