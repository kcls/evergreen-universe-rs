use evergreen as eg;
use std::path::Path;

mod conf;
mod error;
mod query;
mod server;
mod session;

use error::LocalResult;

/// How often we wake and check for shutdown, etc. signals.
///
/// Keep is shorter for dev/debugging.
const DEFAULT_SIG_INTERVAL: u64 = 3;

const IMPLEMENTATION_ID: &str = "EG";
const IMPLEMENTATION_NAME: &str = "Evergreen";
const IMPLEMENTATION_VERSION: &str = "0.1.0";

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-z39-server.yml";
const DEFAULT_CONFIG_2: &str = "./z39-server/conf/eg-z39-server.yml";

fn load_config() -> LocalResult<conf::Config> {
    if let Ok(ref file) = std::env::var("EG_Z39_SERVER_CONFIG") {
        conf::Config::from_yaml(file)
    } else if Path::new(DEFAULT_CONFIG_1).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_1)
    } else if Path::new(DEFAULT_CONFIG_2).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_2)
    } else {
        Err("z39-server requires a configuration file".into())
    }
}

fn main() {
    let conf = match load_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading config file: {e}");
            return;
        }
    };

    // Give the user something if -h/--help are used.
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && (args[1] == "-h" || args[1] == "--help") {
        println!("See {}", conf.filename);
        return;
    }

    // We want logging and IDL parsing.
    let options = eg::init::InitOptions {
        skip_logging: false,
        skip_host_settings: true,
        appname: Some("z39-server".to_string()),
    };

    // We don't need the client retrurned by this call (or its
    // long-lived connection).  Let it drop.
    if let Err(e) = eg::init::with_options(&options) {
        eprintln!("Cannot connect to Evergreen: {e}");
        return;
    };

    // Some responses have canned values that we can set up front.
    z39::Settings {
        implementation_id: Some(IMPLEMENTATION_ID.to_string()),
        implementation_name: Some(IMPLEMENTATION_NAME.to_string()),
        implementation_version: Some(IMPLEMENTATION_VERSION.to_string()),
        // Supported operations
        init_options: z39::settings::InitOptions {
            search: true,
            presen: true,
            ..Default::default()
        },
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

    log::info!(
        "Z39 server starting at {} with databases [{}]",
        conf.bind,
        conf.database_names().join(", ")
    );

    conf.apply();

    server::Z39Server::start(tcp_listener);
}
