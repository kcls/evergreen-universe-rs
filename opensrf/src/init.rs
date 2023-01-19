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
pub fn init_with_more_options(
    opts: &mut getopts::Options,
    options: &InitOptions,
) -> Result<(conf::Config, getopts::Matches), String> {
    let args: Vec<String> = env::args().collect();

    // Override the calculated hostname with "localhost"
    opts.optflag("l", "localhost", "Use Localhost");

    // Override the calculated hostname with a specified value
    opts.optopt("h", "hostname", "hostname", "hostname");

    // Path to opensrf_core.xml
    opts.optopt("c", "osrf-config", "OpenSRF Config", "OSRF_CONFIG");

    // Add more logging options
    opts.optopt("", "log-level", "Log Level Number (0-5)", "LOG_LEVEL");

    // Override configured bus credentials.
    opts.optopt("", "bus-username", "Bus Login Username", "BUS_USERNAME");
    opts.optopt("", "bus-password", "Bus Login Password", "BUS_PASSWORD");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => Err(format!("Error parsing options: {e}"))?,
    };

    let filename = match params.opt_get_default("osrf-config", DEFAULT_OSRF_CONFIG.to_string()) {
        Ok(f) => f,
        Err(e) => Err(format!("Error reading osrf-config option: {e}"))?,
    };

    let mut config = conf::ConfigBuilder::from_file(&filename)?.build()?;

    if params.opt_present("localhost") {
        config.set_hostname("localhost");
    } else if let Some(hostname) = params.opt_str("hostname") {
        config.set_hostname(&hostname);
    }

    if let Some(level) = params.opt_str("log-level") {
        config.client_mut().logging_mut().set_log_level(&level);
    }

    if let Some(username) = params.opt_str("bus-username") {
        config.client_mut().set_username(&username);
    }

    if let Some(password) = params.opt_str("bus-password") {
        config.client_mut().set_password(&password);
    }

    if !options.skip_logging {
        if let Err(e) = logging::Logger::new(config.client().logging())?.init() {
            return Err(format!("Error initializing logger: {e}"));
        }
    }

    Ok((config, params))
}
