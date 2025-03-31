#![forbid(unsafe_code)]

use eg::EgResult;
use evergreen as eg;
use std::env;
use std::path::Path;

mod conf;
mod server;
mod session;

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-sip2-mediator.yml";
const DEFAULT_CONFIG_2: &str = "./sip2-mediator/conf/eg-sip2-mediator.yml";

fn load_config() -> EgResult<conf::Config> {
    if let Ok(ref file) = env::var("EG_SIP2_MEDIATOR_CONFIG") {
        conf::Config::from_yaml(file)
    } else if Path::new(DEFAULT_CONFIG_1).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_1)
    } else if Path::new(DEFAULT_CONFIG_2).exists() {
        conf::Config::from_yaml(DEFAULT_CONFIG_2)
    } else {
        Err("sip2-mediator requires a configuration file".into())
    }
}

fn main() -> EgResult<()> {
    let conf = load_config()?;
    let max_workers = conf.max_workers;
    let min_workers = conf.min_workers;
    let min_idle_workers = conf.min_idle_workers;
    let max_worker_requests = conf.max_worker_requests;

    let options = eg::init::InitOptions {
        skip_logging: false,
        skip_host_settings: true,
        appname: Some("sip2-mediator".to_string()),
    };

    let client = eg::init::with_options(&options)?;

    // The main server thread doesn't need a bus connection, but we
    // do want the other init() pieces.
    drop(client); // force a cleanup and disconnect

    let stream = server::Server::setup(conf)?;

    let mut s = mptc::Server::new(Box::new(stream));

    s.set_max_workers(max_workers);
    s.set_min_workers(min_workers);
    s.set_min_idle_workers(min_idle_workers);
    s.set_max_worker_requests(max_worker_requests);

    s.run();

    Ok(())
}
