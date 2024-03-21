use getopts;
use log::LevelFilter;
use std::env;
use syslog::{BasicLogger, Facility, Formatter3164};
use std::path::Path;

mod conf;
mod server;
mod session;

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-sip2-mediator.yml";
const DEFAULT_CONFIG_2: &str = "./sip2-mediator/conf/eg-sip2-mediator.yml";
const DEFAULT_CONFIG_3: &str = "/usr/local/etc/eg-sip2-mediator.example.yml";
const DEFAULT_CONFIG_4: &str = "./sip2-mediator/conf/eg-sip2-mediator.example.yml";

const HELP_TEXT: &str = r#"

Options:

    --sip-address <sip-address>
        Listen address for SIP server.

    --sip-port <sip-port>
        List port for SIP server.

    --http-host <http-host>
        Hostname of HTTP API server.

    --http-port <http-port>
        Port for HTTP API server.

    --http-proto <http-proto>
        Protocoal for HTTP API server. http or https.

    --http-path <http-path>
        URL path for HTTP API server

    --max-clients <max-clients>
        Maximum number of SIP client connections allowed.

    --syslog-facility <syslog-facility>

    --syslog-level <syslog-level>

    --ascii
        Normalize and encode data returned to SIP clients as ASCII.
        Otherwise, uses UTF8.
"#;

fn main() {
    let conf = parse_args();
    setup_logging(&conf);

    server::Server::new(conf).serve();
}

fn setup_logging(config: &conf::Config) {
    // This does not cover every possibility
    let facility = match &config.syslog_facility.to_lowercase()[..] {
        "local0" => Facility::LOG_LOCAL0,
        "local1" => Facility::LOG_LOCAL1,
        "local2" => Facility::LOG_LOCAL2,
        "local3" => Facility::LOG_LOCAL3,
        "local4" => Facility::LOG_LOCAL4,
        "local5" => Facility::LOG_LOCAL5,
        "local6" => Facility::LOG_LOCAL0,
        "local7" => Facility::LOG_LOCAL0,
        _ => Facility::LOG_USER,
    };

    let level = match &config.syslog_level.to_lowercase()[..] {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    let formatter = Formatter3164 {
        facility: facility,
        hostname: None,
        process: "sip2-mediator".into(),
        pid: std::process::id(),
    };

    let logger = match syslog::unix(formatter) {
        Ok(logger) => logger,
        Err(e) => {
            eprintln!("Cannot connect to syslog: {:?}", e);
            return;
        }
    };

    log::set_boxed_logger(Box::new(BasicLogger::new(logger)))
        .map(|()| log::set_max_level(level))
        .expect("Boxed logger setup with loglevel");
}

fn parse_args() -> conf::Config {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("", "config-file", "", "");
    opts.optopt("", "sip-address", "", "");
    opts.optopt("", "sip-port", "", "");
    opts.optopt("", "http-url", "", "");
    opts.optopt("", "max-clients", "", "");
    opts.optopt("", "syslog-facility", "", "");
    opts.optopt("", "syslog-level", "", "");
    opts.optopt("", "ascii", "", "");
    opts.optopt("", "ignore-ssl-errors", "", "");
    opts.optflag("", "help", "");

    let options = opts
        .parse(&args[1..])
        .expect("Error parsing command line options");

    if options.opt_present("help") {
        println!("{}", HELP_TEXT);
        std::process::exit(0);
    }

    let mut config = conf::Config::new();

    // Start with a config file, if we can find one, then override
    // with command line options.
    if let Some(v) = options.opt_str("config-file") {
        config.read_yaml(&v);
    } else if let Ok(ref file) = env::var("EG_SIP2_MEDIATOR_CONFIG") {
        config.read_yaml(file);
    } else if Path::new(DEFAULT_CONFIG_1).exists() {
        config.read_yaml(DEFAULT_CONFIG_1);
    } else if Path::new(DEFAULT_CONFIG_2).exists() {
        config.read_yaml(DEFAULT_CONFIG_2);
    } else if Path::new(DEFAULT_CONFIG_3).exists() {
        config.read_yaml(DEFAULT_CONFIG_3);
    } else if Path::new(DEFAULT_CONFIG_4).exists() {
        config.read_yaml(DEFAULT_CONFIG_4);
    }

    if let Some(v) = options.opt_str("sip-address") {
        config.sip_address = String::from(v);
    }

    if let Some(v) = options.opt_str("sip-port") {
        config.sip_port = v.parse::<u16>().expect("Invalid SIP port");
    }

    if let Some(v) = options.opt_str("http-url") {
        config.http_url = String::from(v);
    }

    if let Some(v) = options.opt_str("max-clients") {
        config.max_clients = v.parse::<usize>().expect("Invalid Max Clients");
    }

    if let Some(v) = options.opt_str("syslog-facility") {
        config.syslog_facility = String::from(v);
    }

    if let Some(v) = options.opt_str("syslog-level") {
        config.syslog_level = String::from(v);
    }

    if options.opt_present("ascii") {
        config.ascii = true;
    }

    if options.opt_present("ignore-ssl-errors") {
        config.ignore_ssl_errors = true;
    }

    config
}
