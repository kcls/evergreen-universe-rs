use super::conf;
use std::str;
use std::time::Duration;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::net::{TcpListener, TcpStream, Shutdown};

// Wake up occaisonally to see if we need to shutdown, which can
// be initiated via external actions.
const POLL_TIMEOUT: u64 = 5;

// Read data from the socket in chunks this size.
const READ_BUFSIZE: usize = 512;

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
    to_parent_tx: mpsc::Sender<MonitorEvent>,
    shutdown: Arc<AtomicBool>,
}

impl Monitor {
    pub fn new(
        sip_config: conf::Config,
        to_parent_tx: mpsc::Sender<MonitorEvent>,
        shutdown: Arc<AtomicBool>,
    ) -> Monitor {
        Monitor {
            sip_config,
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
            let sgroup = v["settings"].as_str()
                .ok_or(format!("settings name required"))?;

            let settings = self.sip_config.get_settings(sgroup)
                .ok_or(format!("No such sip setting group: {sgroup}"))?;

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

            if let Some(username) = v["sip_username"].as_str() {
                return Ok(MonitorEvent {
                    action: MonitorAction::DisableAccount(username.to_string())
                });
            } else {
                return Err(format!("sip_username value required"));
            }
        }

        Err(format!("Monitor command not supported: {action}"))
    }

    pub fn run(&mut self) {

        let bind = format!(
            "{}:{}",
            self.sip_config.monitor_address().unwrap_or("127.0.0.1"),
            self.sip_config.monitor_port().unwrap_or(6001),
        );

        let listener = TcpListener::bind(bind).expect("Error starting SIP monitor");

        for stream in listener.incoming() {

            match stream {
                Ok(s) => self.handle_client(s),
                Err(e) => log::error!("Error accepting TCP connection {}", e),
            }

            if self.shutdown.load(Ordering::Relaxed) {
                log::info!("Monitor thread exiting on shutdown command");
                return;
            };
        }
    }

    fn handle_client(&mut self, mut stream: TcpStream) {
        loop {

            let command = match self.read_stream(&mut stream) {
                Some(c) => c,
                None => break,
            };

            if let Err(e) = self.handle_command(&mut stream, &command) {
                // TODO report error to the caller
                break;
            }
        }

        log::info!("Monitor disconnecting from client: {stream:?}");

        stream.shutdown(Shutdown::Both);
    }

    fn handle_command(&self, stream: &mut TcpStream, command: &str) -> Result<(), String> {
        todo!()
    }

    fn read_stream(&self, stream: &mut TcpStream) -> Option<String> {
        let timeout = Duration::from_secs(POLL_TIMEOUT);

        // Wake up periodically to see if another thread
        // has set the shutdown flag.
        if let Err(e) = stream.set_read_timeout(Some(timeout)) {
            log::error!("Invalid timeout: {timeout:?} {e}");
            return None;
        }

        loop {

            if self.shutdown.load(Ordering::Relaxed) {
                log::info!("Monitor thread exiting on shutdown command");
                return None;
            };

            let mut buf: [u8; READ_BUFSIZE] = [0; READ_BUFSIZE];

            let num_bytes = match stream.read(&mut buf) {
                Ok(n) => n,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            log::trace!("SIP tcp read timed out.  trying again");
                            continue;
                        }
                        _ => {
                            log::error!("recv() failed: {e}");
                            return None;
                        }
                    }
                }
            };

            // Reading zero bytes can mean the client disconnected.
            // There will at least be a newline character during
            // normal interactions.
            if num_bytes == 0 { return None; }

            let chunk = match str::from_utf8(&buf) {
                Ok(s) => s,
                Err(s) => {
                    log::error!("recv() got non-utf data: {}", s);
                    return None;
                }
            };

            return Some(chunk.trim().to_string());
        }
    }

        /*
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
        */

}
