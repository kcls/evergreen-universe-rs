use eversrf as eg;
use eg::server::Server;
pub mod app;
pub mod methods;

fn main() {
    if let Err(e) = Server::start(Box::new(app::HoldTargeterApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
