use super::conf;
use super::logging;
use std::env;

const DEFAULT_OSRF_CONFIG: &str = "/openils/conf/opensrf_core.xml";

pub struct InitOptions {
    pub skip_logging: bool,
}

impl InitOptions {
    pub fn new() -> InitOptions {
        InitOptions {
            skip_logging: false,
        }
    }
}

/// Read environment variables, parse the core config, setup logging.
///
/// This does not connect to the bus.
pub fn init() -> Result<conf::Config, String> {
    init_with_options(&InitOptions::new())
}

pub fn init_with_options(options: &InitOptions) -> Result<conf::Config, String> {
    let filename = match env::var("OSRF_CONFIG") {
        Ok(v) => v,
        Err(_) => DEFAULT_OSRF_CONFIG.to_string(),
    };

    let mut config = conf::ConfigBuilder::from_file(&filename)?.build()?;

    if let Ok(_) = env::var("OSRF_LOCALHOST") {
        config.set_hostname("localhost");
    } else if let Ok(v) = env::var("OSRF_HOSTNAME") {
        config.set_hostname(&v);
    }

    if let Ok(level) = env::var("OSRF_LOG_LEVEL") {
        config.client_mut().logging_mut().set_log_level(&level);
    }

    if let Ok(username) = env::var("OSRF_BUS_USERNAME") {
        config.client_mut().set_username(&username);
    }

    if let Ok(password) = env::var("OSRF_BUS_PASSWORD") {
        config.client_mut().set_password(&password);
    }

    if !options.skip_logging {
        logging::Logger::new(config.client().logging())?
            .init()
            .or_else(|e| Err(format!("Error initializing logger: {e}")))?;
    }

    Ok(config)
}
