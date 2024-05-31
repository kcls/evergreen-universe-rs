use eg::osrf::server::Server;
use evergreen as eg;
pub mod app;
pub mod methods;
pub mod session;
pub mod item;
pub mod util;

fn main() {
    if let Err(e) = Server::start(Box::new(app::Sip2Application::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}