use evergreen as eg;

mod query;
mod server;
mod session;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 2210;
const DEFAULT_SIG_INTERVAL: u64 = 5;

fn load_options() -> (String, u16, u64) {
    let mut ops = getopts::Options::new();

    ops.optflag("h", "help", "");
    ops.optopt("", "host", "", "");
    ops.optopt("", "port", "", "");

    let args: Vec<String> = std::env::args().collect();

    let params = match ops.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => panic!("Cannot parse options: {}", e),
    };

    let host = params.opt_str("host").unwrap_or(DEFAULT_HOST.to_string());
    let port = if let Some(p) = params.opt_str("port") {
        p.parse::<u16>().expect("Invalid port value: {p}")
    } else {
        DEFAULT_PORT
    };

    let sig_interval = DEFAULT_SIG_INTERVAL; // todo

    (host, port, sig_interval)
}

fn main() {
    let (host, port, sig_interval) = load_options();

    // Some responses have canned values that we can set up front.
    z39::Settings {
        implementation_id: Some("EG".to_string()),
        implementation_name: Some("Evergreen".to_string()),
        implementation_version: Some("0.1.0".to_string()),
        ..Default::default()
    }
    .apply();

    let tcp_listener = match eg::util::tcp_listener(&host, port, sig_interval) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Cannot listen for connections at {host}:{port}: {e}");
            return;
        }
    };

    server::Z39Server::start(tcp_listener);
}
