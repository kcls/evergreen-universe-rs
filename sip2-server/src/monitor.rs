use std::sync::mpsc;
use std::sync::Arc;
use super::conf;
use evergreen as eg;
use opensrf as osrf;
use osrf::bus::Bus;

#[derive(Debug, Clone)]
pub enum ShutdownStyle {
    Graceful,
    Fast,
    Now
}

#[derive(Debug, Clone)]
pub enum MonitorAction {
    Shutdown(ShutdownStyle),
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
    to_parent_tx: mpsc::SyncSender<MonitorEvent>,
}

impl Monitor {

    pub fn new(
        sip_config: conf::Config,
        osrf_config: Arc<osrf::Config>,
        to_parent_tx: mpsc::SyncSender<MonitorEvent>
    ) -> Monitor {

        let osrf_bus = Bus::new(osrf_config.client())
            .expect("Cannot connect to OpenSRF: {e}");

        Monitor {
            sip_config,
            osrf_bus,
            to_parent_tx,
        }
    }

    pub fn run(&mut self) {
    }
}
