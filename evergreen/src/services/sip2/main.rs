use eg::osrf::microsvc::Microservice;
use eg::osrf::server::Server;
use evergreen as eg;
use std::env;
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
    let service = Box::new(app::Sip2Application::new());

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
