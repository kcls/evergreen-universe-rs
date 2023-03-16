use super::conf;
use super::logging;
use getopts;
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

/// Read common command line parameters, parse the core config,
/// setup logging.
///
/// This does not connect to the bus.
pub fn init() -> Result<conf::Config, String> {
    let (config, _) = init_with_options(&mut getopts::Options::new())?;
    Ok(config)
}

pub fn init_with_options(
    opts: &mut getopts::Options,
) -> Result<(conf::Config, getopts::Matches), String> {
    init_with_more_options(opts, &InitOptions::new())
}

/// Same as init(), but allows the caller to pass in a prepopulated set
/// of getopts::Options, which are then augmented with the standard
/// OpenSRF command line options.
///
/// OpenSRF command line options are all prefixed with 'osrf-' to avoid
/// occupying option names that could be useful for clients.
pub fn init_with_more_options(
    opts: &mut getopts::Options,
    options: &InitOptions,
) -> Result<(conf::Config, getopts::Matches), String> {
    let args: Vec<String> = env::args().collect();

    // Override the calculated hostname with "localhost"
    opts.optflag("l", "osrf-localhost", "Use Localhost");

    // Override the calculated hostname with a specified value
    opts.optopt("", "osrf-hostname", "hostname", "hostname");

    // Path to opensrf_core.xml
    opts.optopt("", "osrf-config", "OpenSRF Config", "OSRF_CONFIG");

    // Add more logging options
    opts.optopt("", "osrf-log-level", "Log Level Number (0-5)", "LOG_LEVEL");

    // Override configured bus credentials.
    opts.optopt("", "osrf-bus-username", "Bus Login Username", "BUS_USERNAME");
    opts.optopt("", "osrf-bus-password", "Bus Login Password", "BUS_PASSWORD");

    let params = opts
        .parse(&args[1..])
        .or_else(|e| Err(format!("Error parsing options: {e}")))?;

    let filename = params
        .opt_get_default("osrf-config", DEFAULT_OSRF_CONFIG.to_string())
        .or_else(|e| Err(format!("Error reading osrf-config option: {e}")))?;

    let mut config = conf::ConfigBuilder::from_file(&filename)?.build()?;

    if params.opt_present("osrf-localhost") {
        config.set_hostname("localhost");
    } else if let Some(hostname) = params.opt_str("osrf-hostname") {
        config.set_hostname(&hostname);
    }

    if let Some(level) = params.opt_str("osrf-log-level") {
        config.client_mut().logging_mut().set_log_level(&level);
    }

    if let Some(username) = params.opt_str("osrf-bus-username") {
        config.client_mut().set_username(&username);
    }

    if let Some(password) = params.opt_str("osrf-bus-password") {
        config.client_mut().set_password(&password);
    }

    if !options.skip_logging {
        logging::Logger::new(config.client().logging())?
            .init()
            .or_else(|e| Err(format!("Error initializing logger: {e}")))?;
    }

    Ok((config, params))
}
