use crate::Z39ConnectRequest;
use z39::message::*;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

const NETWORK_BUFSIZE: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct Z39Session {
    tcp_stream: Option<TcpStream>,
    peer_addr: Option<String>,
}

impl fmt::Display for Z39Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(a) = self.peer_addr.as_ref() {
            write!(f, "Z39Session [{a}]")
        } else {
            write!(f, "Z39Session")
        }
    }
}

impl Z39Session {
    /// Panics if the stream is None.
    fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        self.tcp_stream.as_mut().unwrap()
    }

    fn handle_message(&mut self, message: Message) -> Result<(), String> {
        log::debug!("{self} processing message {message:?}");

        match message.payload() {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r),
            _ => todo!(),
        }
    }

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> Result<(), String> {
        let bytes = Message::from_payload(MessagePayload::InitializeResponse(
            InitializeResponse::default(),
        ))
        .to_bytes()?;

        self.reply(bytes.as_slice())
    }

    fn reply(&mut self, bytes: &[u8]) -> Result<(), String> {
        log::debug!("{self} replying with {bytes:?}");
        self.tcp_stream_mut()
            .write_all(bytes)
            .map_err(|e| e.to_string())
    }
}

impl mptc::RequestHandler for Z39Session {
    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        // Turn the general mptc::Request into a type we can perform actions on.
        let request = Z39ConnectRequest::downcast(&mut request);

        // Z39ConnectRequest's only real job was to pass us the stream.
        self.tcp_stream = request.tcp_stream.take();

        self.peer_addr = Some(
            self.tcp_stream_mut()
                .peer_addr()
                .map_err(|e| e.to_string())?
                .to_string(),
        );

        log::info!("{self} starting session");

        let mut bytes = Vec::new();
        let mut buffer = [0u8; NETWORK_BUFSIZE];

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete object/message is formed.  Handle
        // the message, rinse and repeat.
        while let Ok(count) = self.tcp_stream_mut().read(&mut buffer) {
            if count == 0 {
                log::debug!("client socket shutdown.  exiting");
                break;
            }

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

        self.tcp_stream_mut()
            .shutdown(std::net::Shutdown::Both)
            .ok();

        Ok(())
    }
}
