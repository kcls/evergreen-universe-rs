use std::sync::mpsc;
use std::sync::Arc;
use super::conf;
use evergreen as eg;
use opensrf as osrf;
use osrf::bus::Bus;
use osrf::addr::ClientAddress;
use std::sync::atomic::{AtomicBool, Ordering};

// Wake up occaisonally to see if we need to shutdown.
const POLL_TIMEOUT: i32 = 5;

#[derive(Debug, Clone)]
pub enum MonitorAction {
    Shutdown,
    AddAccount(conf::SipAccount),
    DisableAccount(String),
}

#[derive(Debug, Clone)]
pub struct MonitorEvent {
    action: MonitorAction,
}

impl MonitorEvent {
    pub fn action(&self) -> &MonitorAction {
        &self.action
    }
}

impl TryFrom<json::JsonValue> for MonitorEvent {
    type Error = String;
    fn try_from(v: json::JsonValue) -> Result<MonitorEvent, Self::Error> {

        let action = v["action"].as_str().ok_or(format!("MonitorEvent has no action"))?;

        match action {
            "shutdown" => Ok(MonitorEvent { action: MonitorAction::Shutdown }),
            "add-account" => todo!(),
            "disable-account" => todo!(),
            _ => Err(format!("Monitor command not supported: {action}"))
        }
    }
}

pub struct Monitor {
    sip_config: conf::Config,
    osrf_bus: Bus,
    to_parent_tx: mpsc::Sender<MonitorEvent>,
    shutdown: Arc<AtomicBool>,
}

impl Monitor {

    pub fn new(
        sip_config: conf::Config,
        osrf_config: Arc<osrf::Config>,
        to_parent_tx: mpsc::Sender<MonitorEvent>,
        shutdown: Arc<AtomicBool>,
    ) -> Monitor {

        let osrf_bus = Bus::new(osrf_config.client())
            .expect("Cannot connect to OpenSRF: {e}");

        Monitor {
            sip_config,
            osrf_bus,
            to_parent_tx,
            shutdown,
        }
    }

    pub fn run(&mut self) {

        let mut bus_addr = self.osrf_bus.address().clone();
        if let Some(a) = self.sip_config.monitor_address() {
            bus_addr.set_remainder(a);
            self.osrf_bus.set_address(&bus_addr);
        }

        println!("SIP Monitor listening at {}", self.osrf_bus.address().full());

        loop {

            if self.shutdown.load(Ordering::Relaxed) {
                log::info!("Monitor thread exiting on shutdown command");
                break;
            };

            let json_value_op = match self.osrf_bus.recv_json_value(POLL_TIMEOUT, None) {
                Ok(op) => op,
                // Panic here will kill the monitor thread, which the main
                // server thread will detect.
                Err(e) => panic!("Monitor thread could not read from opensrf bus: {}", e),
            };

            if json_value_op.is_none() { continue; }

            let event: MonitorEvent = match json_value_op.unwrap().try_into() {
                Ok(e) => e,
                Err(e) => {
                    log::warn!("Monitor command error: {e}");
                    continue;
                }
            };

            log::info!("Monitor received command: {event:?}");

            match event.action() {
                MonitorAction::Shutdown => {
                    self.shutdown.store(true, Ordering::Relaxed);
                }
                _ => {
                    if let Err(e) = self.to_parent_tx.send(event) {
                        log::error!("Error sending event to server process: {e}");
                        // likely all is lost here, but do our best to
                        // perform a graceful shutdown.
                        self.shutdown.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}
