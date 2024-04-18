use eg::osrf::server::Server;
use evergreen as eg;
pub mod app;
pub mod methods;

fn main() {
    if let Err(e) = Server::start(Box::new(app::RsSearchApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
