use evergreen as eg;

use crate::server::Z39Server;
use z39::message::Message;

mod query;
mod server;
mod session;

fn main() {

    // Some responses have canned values that we need to set up front.
    z39::Settings {
        implementation_id: Some("EG".to_string()),
        implementation_name: Some("Evergreen".to_string()),
        implementation_version: Some("0.1.0".to_string()),
        ..Default::default()
    }.apply();

    // TODO command line, etc.
    let tcp_listener = eg::util::tcp_listener(
        "127.0.0.1",
        2210,
        3,
    )
    .unwrap(); // todo error reporting

    Z39Server::start(tcp_listener);
}


