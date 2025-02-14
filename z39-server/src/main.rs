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

    // Give the user something if -h/--help are used.
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && (args[1] == "-h" || args[1] == "--help") {
        println!("See {}", conf.filename);
        return;
    }

    let options = eg::init::InitOptions {
        skip_logging: false,
        skip_host_settings: true,
        appname: Some("z39-server".to_string()),
    };

    let client = match eg::init::with_options(&options) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Cannot connect to Evergreen: {e}");
            return;
        }
    };

    // The main server thread doesn't need a bus connection.
    // Drop the client to force a disconnect.
    drop(client);

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
        "Z39 server starting at {} with databases {}", 
        conf.bind,
        conf.databases.iter().map(|d| d.name.clone()).collect::<Vec<String>>().join(",")
    );

    conf.apply();

    server::Z39Server::start(tcp_listener);
}
