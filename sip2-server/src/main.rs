use evergreen as eg;
use std::env;
use std::path::Path;

mod checkin;
mod checkout;
mod conf;
mod item;
mod monitor;
mod patron;
mod payment;
mod server;
mod session;
mod util;

const DEFAULT_CONFIG_1: &str = "/usr/local/etc/eg-sip2-server.yml";
const DEFAULT_CONFIG_2: &str = "/usr/local/etc/eg-sip2-server.example.yml";
const DEFAULT_CONFIG_3: &str = "./sip2-server/conf/eg-sip2-server.yml";
const DEFAULT_CONFIG_4: &str = "./sip2-server/conf/eg-sip2-server.example.yml";

fn main() {
    let file_op = env::var("EG_SIP2_SERVER_CONFIG");

    let config_file = if let Ok(ref file) = file_op {
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
        panic!("No viable SIP2 Server Configuration Found");
    };

    let ctx = eg::init::init().expect("Evergreen Init");

    log::info!("SIP2 Server starting with config {config_file}");

    if let Err(e) = server::Server::new(config_file, ctx).serve() {
        log::error!("SIP Server exited with error: {e}");
    }
}
