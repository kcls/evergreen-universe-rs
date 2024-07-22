use eg::osrf::microsvc::Microservice;
use evergreen as eg;
pub mod app;
pub mod methods;

fn main() {
    if let Err(e) = Microservice::start(Box::new(app::RsActorApplication::new())) {
        log::error!("Exiting on server failure: {e}");
    } else {
        log::info!("Server exited normally");
    }
}
