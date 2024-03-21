use crate::util;
use gethostname::gethostname;
use std::fmt;
use std::process;

const BUS_ADDR_NAMESPACE: &str = "opensrf";

#[derive(Debug, Clone, PartialEq)]
enum AddressPurpose {
    Router,
    Service,
    Client,
}

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
    /// Full address string, recompiled as needed.
    full: String,

    purpose: AddressPurpose,
    domain: String,
    username: String,

    /// Only some addresses have content after the $domain.
    remainder: Option<String>,
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
    ///   evergreen::addr::BusAddress::from_str("opensrf:client:foobar:localhost:12345")
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

        let purpose = match parts[1] {
            "router" => AddressPurpose::Router,
            "service" => AddressPurpose::Service,
            "client" => AddressPurpose::Client,
            _ => return Err(format!("Invalid address purpose: {}", parts[1])),
        };

        let username = parts[2].to_string();
        let domain = parts[3].to_string();
        let remainder = match parts.len() > 4 {
            true => Some(parts[4..].join(":")),
            _ => None,
        };

        Ok(BusAddress {
            full: full.to_string(),
            purpose,
            username,
            domain,
            remainder,
        })
    }

    /// Router address
    ///
    /// Send messages here to talk to a Router.
    ///
    /// ```
    /// let addr = evergreen::addr::BusAddress::for_router("router", "private.localhost");
    ///
    /// assert!(addr.is_router());
    /// assert_eq!(addr.as_str(), "opensrf:router:router:private.localhost");
    /// ```
    pub fn for_router(username: &str, domain: &str) -> Self {
        let full = format!("{}:router:{}:{}", BUS_ADDR_NAMESPACE, username, domain);

        BusAddress {
            full,
            purpose: AddressPurpose::Router,
            domain: domain.to_string(),
            username: username.to_string(),
            remainder: None,
        }
    }

    /// Service address unqualified by username or domain.
    ///
    /// The router will fill in the gaps for username/domain.
    ///
    /// ```
    /// let addr = evergreen::addr::BusAddress::for_bare_service("opensrf.settings");
    ///
    /// assert!(addr.is_service());
    /// assert_eq!(addr.service(), Some("opensrf.settings"));
    /// assert_eq!(addr.as_str(), "opensrf:service:_:_:opensrf.settings");
    /// ```
    pub fn for_bare_service(service: &str) -> Self {
        BusAddress::for_service("_", "_", service)
    }

    pub fn for_service(username: &str, domain: &str, service: &str) -> Self {
        let full = format!(
            "{}:service:{}:{}:{}",
            BUS_ADDR_NAMESPACE, username, domain, service
        );

        BusAddress {
            full,
            purpose: AddressPurpose::Service,
            domain: domain.to_string(),
            username: username.to_string(),
            remainder: Some(service.to_string()),
        }
    }

    /// Create a new client address.
    ///
    /// ```
    /// let username = "opensrf";
    /// let domain = "private.localhost";
    /// let addr = evergreen::addr::BusAddress::for_client(username, domain);
    /// assert_eq!(addr.domain(), domain);
    /// assert!(addr.is_client());
    /// ```
    pub fn for_client(username: &str, domain: &str) -> Self {
        let remainder = format!(
            "{}:{}:{}",
            &gethostname().into_string().unwrap(),
            process::id(),
            &util::random_number(6)
        );

        let full = format!(
            "{}:client:{}:{}:{}",
            BUS_ADDR_NAMESPACE, username, domain, remainder
        );

        BusAddress {
            full,
            purpose: AddressPurpose::Client,
            domain: domain.to_string(),
            username: username.to_string(),
            remainder: Some(remainder),
        }
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
    /// All the stuff after opensrf:$purpose:$username:$domain
    pub fn remainder(&self) -> Option<&str> {
        self.remainder.as_deref()
    }

    fn compile(&mut self) {
        let purpose = if self.is_service() {
            "service"
        } else if self.is_router() {
            "router"
        } else {
            "client"
        };

        self.full = format!(
            "{}:{}:{}:{}",
            BUS_ADDR_NAMESPACE,
            purpose,
            self.username(),
            self.domain()
        );

        if let Some(r) = self.remainder.as_ref() {
            self.full += ":";
            self.full += r;
        }
    }

    pub fn set_domain(&mut self, s: &str) {
        self.domain = s.to_string();
        self.compile();
    }
    pub fn set_username(&mut self, s: &str) {
        self.username = s.to_string();
        self.compile();
    }

    /// Allow the caller to provide the address content after the domain.
    ///
    /// ```
    /// let username = "opensrf";
    /// let domain = "private.localhost";
    /// let mut addr = evergreen::addr::BusAddress::for_client(username, domain);
    /// assert_eq!(addr.domain(), domain);
    ///
    /// let remainder = "HELLO123";
    /// addr.set_remainder(remainder);
    /// assert!(addr.is_client());
    /// assert!(addr.as_str().ends_with(remainder));
    /// ```
    pub fn set_remainder(&mut self, s: &str) {
        self.remainder = Some(s.to_string());
        self.compile();
    }

    pub fn service(&self) -> Option<&str> {
        if self.is_service() {
            self.remainder.as_deref()
        } else {
            None
        }
    }
    pub fn is_client(&self) -> bool {
        self.purpose == AddressPurpose::Client
    }
    pub fn is_service(&self) -> bool {
        self.purpose == AddressPurpose::Service
    }
    pub fn is_router(&self) -> bool {
        self.purpose == AddressPurpose::Router
    }
}
