use z39::message::Message;
use crate::Z39Worker;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const NETWORK_BUFSIZE: usize = 1024;

pub(crate) struct Z39Session {
    tcp_stream: TcpStream,
    peer_addr: String,
    shutdown: Arc<AtomicBool>,
    worker: Box<dyn Z39Worker>,
}

impl fmt::Display for Z39Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Z39Session [{}]", self.peer_addr)
    }
}

impl Z39Session {
    pub fn new(
        tcp_stream: TcpStream,
        peer_addr: String,
        shutdown: Arc<AtomicBool>,
        worker: Box<dyn Z39Worker>,
    ) -> Self {
        Self {
            tcp_stream,
            peer_addr,
            shutdown,
            worker,
        }
    }

    pub fn listen(&mut self) -> Result<(), String> {
        log::info!("{self} starting session");

        let mut bytes = Vec::new();

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete message is formed.  Handle the
        // message, rinse and repeat.
        loop {
            let mut buffer = [0u8; NETWORK_BUFSIZE];

            let count = match self.tcp_stream.read(&mut buffer) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        if self.shutdown.load(Ordering::Relaxed) {
                            log::debug!("{self} Shutdown signal received, exiting listen loop");
                            break;
                        }
                        // Go back and wait for requests to arrive.
                        continue;
                    }
                    _ => {
                        // Connection severed.  we're done.
                        log::info!("{self} Socket closed: {e}");
                        break;
                    }
                }
            };

            if count == 0 {
                // Returning Ok(0) from read for a TcpStream indicates the
                // remote end of the stream was shut down.
                log::debug!("{self} socket shut down by remote endpoint");
                break;
            }

            bytes.extend_from_slice(&buffer[0..count]);

            // Parse the message bytes
            let Some(msg) = Message::from_bytes(&bytes)? else {
                log::debug!("{self} partial message read; more bytes needed");
                continue;
            };

            // Reset the byte array for the next message cycle.
            bytes.clear();

            // Let the worker do its thing
            let resp = self.worker.handle_message(msg)?;

            // Turn the response into bytes
            let bytes = resp.to_bytes()?;

            log::trace!("{self} replying with {bytes:?}");

            self.tcp_stream.write_all(bytes.as_slice()).map_err(|e| e.to_string())?;
        }

        log::info!("{self} session exiting");

        Ok(())
    }

    /// Shut down the sesion's TcpStrean.
    pub fn shutdown(&mut self) {
        self.tcp_stream.shutdown(std::net::Shutdown::Both).ok();
    }
}
