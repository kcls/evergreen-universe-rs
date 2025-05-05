use eg::osrf::microsvc::Microservice;
use eg::osrf::server::Server;
use evergreen as eg;
use std::env;
pub mod app;
pub mod methods;

fn main() {
    let service = Box::new(app::AddrsApplication::new());

    let outcome = if env::vars().any(|(k, _)| k == "EG_SERVICE_AS_MICRO") {
        Microservice::start(service)
    } else {
        Server::start(service)
    };

    if let Err(e) = outcome {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
