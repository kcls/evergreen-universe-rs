use evergreen as eg;
use std::path::Path;

mod conf;
mod query;
mod server;
mod session;

/// How often we wake and check for shutdown, etc. signals.
const DEFAULT_SIG_INTERVAL: u64 = 5;

const IMPLEMENTATION_ID: &str = "EG";
const IMPLEMENTATION_NAME: &str = "Evergreen";
const IMPLEMENTATION_VERSION: &str = "0.1.0";

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-z39-server.yml";
const DEFAULT_CONFIG_2: &str = "./z39-server/conf/eg-z39-server.yml";

fn load_config() -> eg::EgResult<conf::Config> {
    if let Ok(ref file) = std::env::var("EG_Z39_SERVER_CONFIG") {
        conf::Config::from_yaml(file)
    } else if Path::new(DEFAULT_CONFIG_1).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_1)
    } else if Path::new(DEFAULT_CONFIG_2).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_2)
    } else {
        Err("sip2-mediator requires a configuration file".into())
    }
}

fn main() {
    let Ok(conf) = load_config().inspect_err(|e| eprintln!("Config error: {e}")) else {
        return;
    };

    let options = eg::init::InitOptions {
        skip_logging: false,
        skip_host_settings: true,
        appname: Some("z39-server".to_string()),
    };

    let Ok(client) = eg::init::with_options(&options)
        .inspect_err(|e| eprintln!("Cannot connect to Evergreen: {e}")) else {
        return;
    };

    // The main server thread doesn't need a bus connection, but we
    // do want the other init() pieces.  Drop the client to force a
    // disconnect.
    drop(client);

    // Some responses have canned values that we can set up front.
    z39::Settings {
        implementation_id: Some(IMPLEMENTATION_ID.to_string()),
        implementation_name: Some(IMPLEMENTATION_NAME.to_string()),
        implementation_version: Some(IMPLEMENTATION_VERSION.to_string()),
        ..Default::default()
    }
    .apply();

    let tcp_listener = match eg::util::tcp_listener(&conf.bind, DEFAULT_SIG_INTERVAL) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Cannot listen for connections at {}: {e}", conf.bind);
            return;
        }
    };

    server::Z39Server::start(tcp_listener);
}
