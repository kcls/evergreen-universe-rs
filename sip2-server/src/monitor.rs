use super::conf;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

const HELP_TEXT: &str = r#"
Commands entered here affect the running instance only, not the config files.

Commands:
  help
  shutdown
  list-accounts
  add-account <setting-group> <sip-user> <sip-pass> <ils-user> [<workstation>]
  disable-account <sip-user>
"#;

// Wake up occaisonally to see if we need to shutdown, which can
// be initiated via external actions.
const POLL_TIMEOUT: u64 = 5;

// Read data from the socket in chunks this size.
const READ_BUFSIZE: usize = 512;

/// Set of actions that may be delivered to the parent/server process
/// for handling.
#[derive(Debug)]
pub enum MonitorAction {
    AddAccount(conf::SipAccount),
    DisableAccount(String),
}

#[derive(Debug)]
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

            log::info!("Monitor read command: {command}");

            if command.eq("exit") || command.eq("quit") {
                break;
            }

            if let Err(e) = self.handle_command(&mut stream, &command) {
                if let Err(e2) = stream.write(format!("Command failed: {command} {e}\n").as_bytes())
                {
                    log::error!("Error replying to caller.  Exiting: {e2}");
                    break;
                }
            }
        }

        log::info!("Monitor disconnecting from client: {stream:?}");

        stream.shutdown(Shutdown::Both).ok();
    }

    fn handle_command(&mut self, stream: &mut TcpStream, commands: &str) -> Result<(), String> {
        let command = match commands.split(" ").next() {
            Some(c) => {
                if c.len() == 0 {
                    // empty line
                    return Ok(());
                } else {
                    c
                }
            }
            None => return Ok(()),
        };

        let mut response = "-------------------------------------\n".to_string();

        match command {
            "help" => response += HELP_TEXT,
            "shutdown" => {
                response += "OK\n";
                self.shutdown.store(true, Ordering::Relaxed);
                self.wake_server();
            }

            "list-accounts" => {
                for acct in self.sip_config.accounts() {
                    response += &format!(
                        "settings={} username={}\n",
                        acct.settings().name(),
                        acct.sip_username()
                    );
                }

                // As a separate thread, we operator on a cloned version
                // of the SIP config.  To include manually added
                // (in-memory only) accounts, we'd have to request
                // an updated list of accounts from the main server process.
                response += "\n* Does not include live changes *\n";
            }
            "add-account" => {
                self.add_account(commands)?;
                self.wake_server();
                response += "OK\n";
            }
            "disable-account" => {
                self.disable_account(commands)?;
                self.wake_server();
                response += "OK\n";
            }
            _ => Err(format!("Unrecognized command"))?,
        }

        response += "-------------------------------------\n";

        stream
            .write(response.as_bytes())
            .or_else(|e| Err(format!("Error sending monitor reply: {e}")))?;

        Ok(())
    }

    /// Connect to the TCP port of our server to wake it up and process
    /// pending commands.
    ///
    /// The server only wakes to check for shutdown signals, etc. when
    /// a new client connects to its SIP port.
    fn wake_server(&self) {
        if let Ok(stream) =
            TcpStream::connect((self.sip_config.sip_address(), self.sip_config.sip_port()))
        {
            // And immediately disconnect
            stream.shutdown(Shutdown::Both).ok();
        }
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
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        log::trace!("SIP tcp read timed out.  trying again");
                        continue;
                    }
                    _ => {
                        log::error!("recv() failed: {e}");
                        return None;
                    }
                },
            };

            // Reading zero bytes can mean the client disconnected.
            // There will at least be a newline character during
            // normal interactions.
            if num_bytes == 0 {
                return None;
            }

            let chunk = match str::from_utf8(&buf) {
                Ok(s) => s,
                Err(s) => {
                    log::error!("recv() got non-utf data: {}", s);
                    return None;
                }
            };

            // remove '\0' chars and trailing newlines
            return Some(chunk.trim_matches(char::from(0)).trim_end().to_string());
        }
    }

    fn disable_account(&mut self, command: &str) -> Result<(), String> {
        let commands: Vec<&str> = command.split(" ").collect();

        let username = if commands.len() > 1 {
            &commands[1]
        } else {
            Err(format!("disable-account missing parameters"))?
        };

        let event = MonitorEvent {
            action: MonitorAction::DisableAccount(username.to_string()),
        };

        if let Err(e) = self.to_parent_tx.send(event) {
            log::error!("Error sending event to server process: {e}");
            self.shutdown.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    fn add_account(&mut self, command: &str) -> Result<(), String> {
        let commands: Vec<&str> = command.split(" ").collect();

        if commands.len() < 5 {
            Err(format!("add-account missing parameters"))?;
        }

        let sgroup = &commands[1];

        let settings = self
            .sip_config
            .get_settings(sgroup)
            .ok_or(format!("No such sip setting group: {sgroup}"))?;

        let mut account = conf::SipAccount::new(
            settings,
            &commands[2], // sip user
            &commands[3], // sip pass
            &commands[4], // ils user
        );

        if let Some(w) = commands.get(5) {
            account.set_workstation(Some(w));
        }

        let event = MonitorEvent {
            action: MonitorAction::AddAccount(account),
        };

        if let Err(e) = self.to_parent_tx.send(event) {
            log::error!("Error sending event to server process: {e}");
            self.shutdown.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}
