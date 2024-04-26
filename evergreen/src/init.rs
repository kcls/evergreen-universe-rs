//! Connect to OpenSRF/Redis, load host settings, and load the IDL.
use crate::idl;
use crate::osrf::conf;
use crate::osrf::logging;
use crate::osrf::sclient::HostSettings;
use crate::Client;
use crate::EgResult;
use std::env;

const DEFAULT_OSRF_CONFIG: &str = "/openils/conf/opensrf_core.xml";
const DEFAULT_IDL_PATH: &str = "/openils/conf/fm_IDL.xml";

// TODO we no longer need this since it's just a Client wrapper.
#[derive(Clone)]
pub struct Context {
    client: Client,
}

impl Context {
    pub fn client(&self) -> &Client {
        &self.client
    }
}

pub struct InitOptions {
    /// Skip logging initialization.
    /// Useful if changes to the logging config first.
    pub skip_logging: bool,

    /// Skip fetching the host settings from opensrf.settings
    pub skip_host_settings: bool,

    /// Application name to use with syslog.
    pub appname: Option<String>,
}

impl InitOptions {
    pub fn new() -> InitOptions {
        InitOptions {
            skip_logging: false,
            skip_host_settings: false,
            appname: None,
        }
    }
}

/// Read environment variables, parse the core config, setup logging.
///
/// This does not connect to the bus.
pub fn init() -> EgResult<Context> {
    with_options(&InitOptions::new())
}

/// Parse the OpenSRF config file, connect to the message bus, and
/// optionally fetch the host settings and initialize logging.
pub fn osrf_init(options: &InitOptions) -> EgResult<Client> {
    let builder = if let Ok(fname) = env::var("OSRF_CONFIG") {
        conf::ConfigBuilder::from_file(&fname)?
    } else {
        conf::ConfigBuilder::from_file(DEFAULT_OSRF_CONFIG)?
    };

    let mut config = builder.build()?;

    if let Ok(_) = env::var("OSRF_LOCALHOST") {
        config.set_hostname("localhost");
    } else if let Ok(v) = env::var("OSRF_HOSTNAME") {
        config.set_hostname(&v);
    }

    // When custom client connection/logging values are provided via
    // the ENV, propagate them to all variations of a client connection
    // supported by the current opensrf_core.xml format.

    if let Ok(level) = env::var("OSRF_LOG_LEVEL") {
        config.client_mut().logging_mut().set_log_level(&level);
        if let Some(gateway) = config.gateway_mut() {
            gateway.logging_mut().set_log_level(&level);
        }
        for router in config.routers_mut() {
            router.client_mut().logging_mut().set_log_level(&level);
        }
    }

    if let Ok(facility) = env::var("OSRF_LOG_FACILITY") {
        config
            .client_mut()
            .logging_mut()
            .set_syslog_facility(&facility)?;
        if let Some(gateway) = config.gateway_mut() {
            gateway.logging_mut().set_syslog_facility(&facility)?;
        }
        for router in config.routers_mut() {
            router
                .client_mut()
                .logging_mut()
                .set_syslog_facility(&facility)?;
        }
    }

    if let Ok(username) = env::var("OSRF_BUS_USERNAME") {
        config.client_mut().set_username(&username);
        if let Some(gateway) = config.gateway_mut() {
            gateway.set_username(&username);
        }
        for router in config.routers_mut() {
            router.client_mut().set_username(&username);
        }
    }

    if let Ok(password) = env::var("OSRF_BUS_PASSWORD") {
        config.client_mut().set_password(&password);
        if let Some(gateway) = config.gateway_mut() {
            gateway.set_password(&password);
        }
        for router in config.routers_mut() {
            router.client_mut().set_password(&password);
        }
    }

    if !options.skip_logging {
        let mut logger = logging::Logger::new(config.client().logging())?;
        if let Some(name) = options.appname.as_ref() {
            logger.set_application(name);
        }
        logger
            .init()
            .or_else(|e| Err(format!("Error initializing logger: {e}")))?;
    }

    // Save the config as the one-true-global-osrf-config
    config.store()?;

    let client = Client::connect().or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    // We try to get the IDL path from opensrf.settings, but that will
    // fail if we are not connected to a domain running opensrf.settings
    // (e.g. a public domain).

    if !options.skip_host_settings {
        HostSettings::load(&client)?;
    }

    Ok(client)
}

pub fn with_options(options: &InitOptions) -> EgResult<Context> {
    let client = osrf_init(&options)?;

    load_idl()?;

    Ok(Context { client })
}

/// Locate and parse the IDL file.
pub fn load_idl() -> EgResult<()> {
    if let Ok(v) = env::var("EG_IDL_FILE") {
        return idl::Parser::load_file(&v);
    }

    if HostSettings::is_loaded() {
        if let Some(fname) = HostSettings::value("/IDL")?.as_str() {
            return idl::Parser::load_file(fname);
        }
    }

    idl::Parser::load_file(DEFAULT_IDL_PATH)
}

/// Create a new connection using pre-compiled context components.  Useful
/// for spawned threads so they can avoid repetitive processing at
/// connect time.
///
/// The only part that must happen in its own thread is the opensrf connect.
pub fn init_from_parts() -> EgResult<Context> {
    let client = Client::connect().or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    Ok(Context { client })
}
