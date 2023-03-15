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
/// opensrf:service:$service
/// opensrf:client:$domain:$hostname:$pid:$random
/// opensrf:router:$domain
#[derive(Debug, Clone)]
pub struct BusAddress {
    /// Full raw address string
    full: String,

    /// A top-level service address has no domain.
    domain: Option<String>,

    /// Only top-level service addresses have a service name
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
    /// let addr = opensrf::addr::BusAddress::new_from_string("opensrf:client:localhost:12345")
    ///   .expect("Error creating address from string");
    ///
    /// assert!(addr.is_client());
    /// assert!(addr.domain().is_some());
    /// assert_eq!(addr.domain().unwrap(), "localhost");
    /// ```
    pub fn new_from_string(full: &str) -> Result<Self, String> {
        let parts: Vec<&str> = full.split(':').collect();

        // We only really care about the first 3 parts of the address.
        if parts.len() < 3 {
            return Err(format!("BusAddress bad format: {}", full));
        }

        let purpose = parts[1];
        let sod = parts[2].to_owned(); // service name or domain

        let mut addr = BusAddress {
            full: full.to_string(),
            domain: None,
            service: None,
            is_client: false,
            is_service: false,
            is_router: false,
        };

        if purpose.eq("service") {
            addr.service = Some(sod);
            addr.is_service = true;
        } else if purpose.eq("client") {
            addr.domain = Some(sod);
            addr.is_client = true;
        } else if purpose.eq("router") {
            addr.domain = Some(sod);
            addr.is_router = true;
        } else {
            return Err(format!("Unknown BusAddress purpose: {}", purpose));
        }

        Ok(addr)
    }
}

impl BusAddress {
    /// Full address string
    pub fn as_str(&self) -> &str {
        &self.full
    }
    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
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
        let addr = BusAddress::new_from_string(full)?;
        if !addr.is_client() {
            return Err(format!("Invalid ClientAddress string: {full}"));
        }
        Ok(ClientAddress { addr })
    }

    pub fn as_str(&self) -> &str {
        self.addr.as_str()
    }

    pub fn new(domain: &str) -> Self {
        let full = format!(
            "{}:client:{}:{}:{}:{}",
            BUS_ADDR_NAMESPACE,
            domain,
            &gethostname().into_string().unwrap(),
            process::id(),
            &util::random_number(6)
        );

        ClientAddress {
            addr: BusAddress {
                full,
                domain: Some(domain.to_string()),
                service: None,
                is_client: true,
                is_service: false,
                is_router: false,
            },
        }
    }

    /// Allow the caller to provide the address content after the domain.
    pub fn set_remainder(&mut self, remainder: &str) {
        self.addr.full = format!(
            "{}:client:{}:{}",
            BUS_ADDR_NAMESPACE,
            self.domain(),
            remainder,
        );
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
    }

    pub fn domain(&self) -> &str {
        self.addr().domain().unwrap()
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
        let addr = BusAddress::new_from_string(full)?;
        if !addr.is_service() {
            return Err(format!("Invalid ServiceAddress string: {full}"));
        }
        Ok(ServiceAddress { addr })
    }

    pub fn as_str(&self) -> &str {
        self.addr.as_str()
    }

    pub fn new(service: &str) -> Self {
        let full = format!("{}:service:{}", BUS_ADDR_NAMESPACE, &service);

        ServiceAddress {
            addr: BusAddress {
                full,
                domain: None,
                service: Some(service.to_string()),
                is_client: false,
                is_service: true,
                is_router: false,
            },
        }
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
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
        let addr = BusAddress::new_from_string(full)?;
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
    /// let addr = opensrf::addr::RouterAddress::new("localhost");
    /// assert_eq!(addr.as_str(), "opensrf:router:localhost");
    /// ```
    pub fn new(domain: &str) -> Self {
        let full = format!("{}:router:{}", BUS_ADDR_NAMESPACE, &domain);

        RouterAddress {
            addr: BusAddress {
                full,
                service: None,
                domain: Some(domain.to_string()),
                is_client: false,
                is_service: false,
                is_router: true,
            },
        }
    }

    pub fn addr(&self) -> &BusAddress {
        &self.addr
    }

    pub fn domain(&self) -> &str {
        self.addr().domain().unwrap()
    }
}

impl fmt::Display for RouterAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RouterAddress={}", self.as_str())
    }
}
