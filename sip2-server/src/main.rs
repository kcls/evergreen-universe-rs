use evergreen as eg;
use getopts;

mod conf;
mod item;
mod server;
mod session;

const HELP_TEXT: &str = r#"

Options:

    --config-file <conf/sip2-server.yml>
        SIP server configuration file.

"#;

fn main() {
    let mut opts = getopts::Options::new();

    opts.optopt("", "config-file", "", "");

    let ctx = eg::init::init_with_options(&mut opts).expect("Evergreen Init");
    let options = ctx.params();

    if options.opt_present("help") {
        println!("{}", HELP_TEXT);
        std::process::exit(0);
    }

    let mut sip_conf = conf::Config::new();

    let config_file = match options.opt_str("config-file") {
        Some(f) => f,
        None => "conf/sip2-server.yml".to_string(),
    };

    sip_conf.read_yaml(&config_file);

    server::Server::new(sip_conf, ctx).serve();
}
