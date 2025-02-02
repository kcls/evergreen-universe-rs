use z39::message::*;
use evergreen as eg;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const NETWORK_BUFSIZE: usize = 1024;

pub struct Z39Session {
    pub tcp_stream: TcpStream,
    pub peer_addr: String,
    pub shutdown: Arc<AtomicBool>,
    pub client: eg::Client,
}

impl fmt::Display for Z39Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Z39Session [{}]", self.peer_addr)
    }
}

impl Z39Session {
    fn handle_message(&mut self, message: Message) -> Result<(), String> {
        log::debug!("{self} processing message {message:?}");

        match &message.payload {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r),
            MessagePayload::SearchRequest(r) => self.handle_search_request(r),
            _ => todo!("handle_message() unsupported message type"),
        }
    }

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> Result<(), String> {
        let bytes = Message::from_payload(MessagePayload::InitializeResponse(
            InitializeResponse::default(),
        ))
        .to_bytes()?;

        self.reply(bytes.as_slice())
    }

    fn handle_search_request(&mut self, _req: &SearchRequest) -> Result<(), String> {
        let mut resp = SearchResponse::default();

        // TODO
        resp.result_count = 1;
        resp.search_status = true;

        let bytes = Message::from_payload(MessagePayload::SearchResponse(resp)).to_bytes()?;

        self.reply(bytes.as_slice())
    }


    /// Drop a set of bytes onto the wire.
    fn reply(&mut self, bytes: &[u8]) -> Result<(), String> {
        log::debug!("{self} replying with {bytes:?}");

        self.tcp_stream.write_all(bytes).map_err(|e| e.to_string())
    }

    pub fn listen(&mut self) -> Result<(), String> {
        log::info!("{self} starting session");

        let mut bytes = Vec::new();
        let mut buffer = [0u8; NETWORK_BUFSIZE];

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete object/message is formed.  Handle
        // the message, rinse and repeat.
        loop {

            let _count = match self.tcp_stream.read(&mut buffer) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        if self.shutdown.load(Ordering::Relaxed) {
                            log::debug!("Shutdown signal received, exiting listen loop");
                            break;
                        }
                        continue;
                    }
                    _ => {
                        log::info!("Socket closed: {e}");
                        break;
                    }
                }
            };

            bytes.extend_from_slice(&buffer);

            let msg = match Message::from_bytes(&bytes) {
                Ok(maybe) => match maybe {
                    Some(m) => {
                        bytes.clear();
                        m
                    }
                    None => continue, // more bytes needed
                },
                Err(e) => {
                    log::error!("cannot parse message: {e} {bytes:?}");
                    break;
                }
            };

            if let Err(e) = self.handle_message(msg) {
                log::error!("cannot handle message: {e} {bytes:?}");
                break;
            }
        }

        log::info!("session exiting");

        self.tcp_stream.shutdown(std::net::Shutdown::Both).ok();

        Ok(())
    }
}
