use eg::osrf::server::Server;
use evergreen as eg;
pub mod app;
pub mod checkin;
pub mod checkout;
pub mod holds;
pub mod item;
pub mod methods;
pub mod patron;
pub mod payment;
pub mod session;
pub mod util;

fn main() {
    if let Err(e) = Server::start(Box::new(app::Sip2Application::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
