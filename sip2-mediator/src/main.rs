use evergreen as eg;
use eg::EgResult;
use mptc;
use std::path::Path;

mod conf;
mod server;
mod session;

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-sip2-mediator.yml";
const DEFAULT_CONFIG_2: &str = "./sip2-mediator/conf/eg-sip2-mediator.yml";
const DEFAULT_CONFIG_3: &str = "/usr/local/etc/eg-sip2-mediator.example.yml";
const DEFAULT_CONFIG_4: &str = "./sip2-mediator/conf/eg-sip2-mediator.example.yml";

fn load_config() -> EgResult<conf::Config> {

    let file = if let Some(v) = options.opt_str("config") {
        v
    } else if let Ok(ref file) = env::var("EG_SIP2_MEDIATOR_CONFIG") {
        file
    } else if Path::new(DEFAULT_CONFIG_1).exists() {
        DEFAULT_CONFIG_1
    } else if Path::new(DEFAULT_CONFIG_2).exists() {
        DEFAULT_CONFIG_2
    } else if Path::new(DEFAULT_CONFIG_3).exists() {
        DEFAULT_CONFIG_3
    } else if Path::new(DEFAULT_CONFIG_4).exists() {
        DEFAULT_CONFIG_4
    } else {
        return Err(format!("sip2-mediator requires a configuration file").into());
    };

    let mut config = conf::Config::new();

    config.read_yaml(&file)
}

fn main() -> EgResult<()> {
    let conf = load_config()?;

    // Support env vars as well?
    let max_workers = conf.max_clients;
    let min_workers = conf.min_workers;

    let ctx = match eg::init::init()?

    let stream = match server::Server::setup(conf, ctx)?;

    let mut s = mptc::Server::new(Box::new(stream));

    s.set_max_workers(max_workers);
    s.set_min_workers(min_workers);

    // Each SIP sessions counts as one request to MPTC.
    // Use the default value for max worker requests.
    // s.set_max_worker_requests(...);

    s.run();

    Ok(())
}


