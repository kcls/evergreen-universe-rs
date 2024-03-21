use crate::idl;
use crate::osrf::conf;
use crate::osrf::logging;
use crate::osrf::sclient;
use crate::Client;
use crate::EgResult;
use std::env;
use std::sync::Arc;

const DEFAULT_OSRF_CONFIG: &str = "/openils/conf/opensrf_core.xml";
const DEFAULT_IDL_PATH: &str = "/openils/conf/fm_IDL.xml";

#[derive(Clone)]
pub struct Context {
    client: Client,
    config: Arc<conf::Config>,
    idl: Arc<idl::Parser>,
    host_settings: Option<Arc<sclient::HostSettings>>,
}

impl Context {
    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn config(&self) -> &Arc<conf::Config> {
        &self.config
    }
    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }
    pub fn host_settings(&self) -> Option<&Arc<sclient::HostSettings>> {
        self.host_settings.as_ref()
    }
}

pub struct InitOptions {
    pub skip_logging: bool,
    pub skip_host_settings: bool,
    // Application name to use with syslog.
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
    init_with_options(&InitOptions::new())
}

pub fn osrf_init(options: &InitOptions) -> EgResult<conf::Config> {
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

    Ok(config)
}

pub fn init_with_options(options: &InitOptions) -> EgResult<Context> {
    let config = osrf_init(&options)?;
    let config = config.into_shared();

    let client = Client::connect(config.clone())
        .or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    // We try to get the IDL path from opensrf.settings, but that will
    // fail if we are not connected to a domain running opensrf.settings
    // (e.g. a public domain).

    let mut idl_file = DEFAULT_IDL_PATH.to_string();
    let mut host_settings: Option<Arc<sclient::HostSettings>> = None;

    if !options.skip_host_settings {
        if let Ok(s) = sclient::SettingsClient::get_host_settings(&client, false) {
            if let Some(fname) = s.value("/IDL").as_str() {
                idl_file = fname.to_string();
            }
            host_settings = Some(s.into_shared());
        }
    }

    // Always honor the environment variable if present.
    if let Ok(v) = env::var("EG_IDL_FILE") {
        idl_file = v;
    }

    let idl_parser = idl::Parser::parse_file(&idl_file)
        .or_else(|e| Err(format!("Cannot parse IDL file: {e}")))?;

    idl::set_thread_idl(&idl_parser);

    Ok(Context {
        client,
        config,
        idl: idl_parser,
        host_settings,
    })
}

/// Create a new connection using pre-compiled context components.  Useful
/// for spawned threads so they can avoid repetitive processing at
/// connect time.
///
/// The only part that must happen in its own thread is the opensrf connect.
pub fn init_from_parts(
    config: Arc<conf::Config>,
    idl: Arc<idl::Parser>,
    host_settings: Option<Arc<sclient::HostSettings>>,
) -> EgResult<Context> {
    let client = Client::connect(config.clone())
        .or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    Ok(Context {
        client,
        config,
        idl,
        host_settings,
    })
}
