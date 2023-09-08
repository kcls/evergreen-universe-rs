use opensrf::server::Server;
pub mod app;
pub mod methods;

fn main() {
    if let Err(e) = Server::start(Box::new(app::RsHoldTargeterApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
