use super::conf;
use opensrf as osrf;
use osrf::bus::Bus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

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
        let osrf_bus = Bus::new(osrf_config.client()).expect("Cannot connect to OpenSRF: {e}");

        Monitor {
            sip_config,
            osrf_bus,
            to_parent_tx,
            shutdown,
        }
    }

    pub fn parse_event(&self, v: &json::JsonValue) -> Result<MonitorEvent, String> {
        let action = v["action"]
            .as_str()
            .ok_or(format!("MonitorEvent has no action"))?;

        if action.eq("shutdown") {
            return Ok(MonitorEvent { action: MonitorAction::Shutdown });
        }

        if action.eq("add-account") {

            let settings = match v["settings"].as_str() {
                Some(s) => match self.sip_config.get_settings(s) {
                    Some(s2) => s2,
                    None => Err(format!("No such SIP settings group: {s}"))?,
                }
                None => Err(format!("SIP setting group name required"))?,
            };

            let sgroup = v["settings"].as_str()
                .ok_or(format!("settings name required"))?;

            let settings = self.sip_config.get_settings(sgroup)
                .ok_or(format!("No such sip setting group: {settings:?}"))?;

            let sip_username = v["sip_username"].as_str()
                .ok_or(format!("sip_username required"))?;

            let sip_password = v["sip_password"].as_str()
                .ok_or(format!("sip_password required"))?;

            let ils_username = v["ils_username"].as_str()
                .ok_or(format!("ils_username required"))?;

            let mut account = conf::SipAccount::new(
                settings,
                sip_username,
                sip_password,
                ils_username,
            );

            account.set_workstation(v["workstation"].as_str());

            return Ok(MonitorEvent { action: MonitorAction::AddAccount(account) });
        }

        if action.eq("disable-account") {
            todo!()
        }

        Err(format!("Monitor command not supported: {action}"))
    }

    pub fn run(&mut self) {
        let mut bus_addr = self.osrf_bus.address().clone();
        if let Some(a) = self.sip_config.monitor_address() {
            bus_addr.set_remainder(a);
            self.osrf_bus.set_address(&bus_addr);
        }

        println!(
            "SIP Monitor listening at {}",
            self.osrf_bus.address().full()
        );

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

            if json_value_op.is_none() {
                continue;
            }

            let event: MonitorEvent = match self.parse_event(&json_value_op.unwrap()) {
                Ok(e) => e,
                Err(e) => {
                    // TODO reply to caller with error message
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
