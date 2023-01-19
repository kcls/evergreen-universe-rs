use std::time::Duration;
use super::error::Error;
use super::spec;
use super::Message;
use std::io::prelude::*;
use std::net::{Shutdown, TcpStream};
use std::str;
use deunicode::deunicode;

// Read data from the socket in chunks this size.
const READ_BUFSIZE: usize = 256;

/// Manages a TCP connection to a SIP server and handles message sending
/// and receiving.
pub struct Connection {
    tcp_stream: TcpStream,

    // If set, non-ASCII chars are removed from outbound messages.
    ascii: bool,
}

impl Connection {
    /// Creates a new SIP client and opens the TCP connection to the server
    ///
    /// * `sip_host` - SIP server host/ip and port
    /// * E.g. "127.0.0.1:6001"
    ///
    /// ```
    /// use sip2::Connection;
    /// assert_eq!(Connection::new("JUNK0+..-*z$@").is_err(), true);
    /// ```
    pub fn new(sip_host: &str) -> Result<Self, Error> {
        log::debug!("Connection::new() connecting to: {}", sip_host);

        match TcpStream::connect(sip_host) {
            Ok(stream) => Ok(Connection {
                tcp_stream: stream,
                ascii: false,
            }),
            Err(s) => {
                log::error!("Connection::new() failed: {}", s);
                return Err(Error::NetworkError);
            }
        }
    }

    pub fn new_from_stream(tcp_stream: TcpStream) -> Self {
        Connection {
            ascii: false,
            tcp_stream: tcp_stream,
        }
    }

    pub fn set_ascii(&mut self, ascii: bool) {
        self.ascii = ascii;
    }

    /// Shutdown the TCP connection with the SIP server.
    pub fn disconnect(&self) -> Result<(), Error> {
        log::debug!("Connection::disconnect()");

        match self.tcp_stream.shutdown(Shutdown::Both) {
            Ok(_) => Ok(()),
            Err(s) => {
                log::error!("disconnect() failed: {}", s);
                return Err(Error::NetworkError);
            }
        }
    }

    /// Send a SIP message
    pub fn send(&mut self, msg: &Message) -> Result<(), Error> {
        let mut msg_sip = msg.to_sip() + spec::LINE_TERMINATOR;

        if self.ascii {
            // https://crates.io/crates/deunicode
            // "Some transliterations do produce \n characters."
            msg_sip = deunicode(&msg_sip).replace("\n", "");
        }

        log::info!("OUTBOUND: {}", msg_sip);

        match self.tcp_stream.write(&msg_sip.as_bytes()) {
            Ok(_) => Ok(()),
            Err(s) => {
                log::error!("send() failed: {}", s);
                return Err(Error::NetworkError);
            }
        }
    }

    /// Receive a SIP response.
    ///
    /// Blocks until a response is received.
    pub fn recv(&mut self) -> Result<Message, Error> {
        match self.recv_internal(None) {
            Ok(op) => match op {
                Some(m) => Ok(m),
                None => Err(Error::NetworkError),
            }
            Err(e) => Err(e),
        }
    }

    pub fn recv_with_timeout(&mut self, timeout: u64) -> Result<Option<Message>, Error> {
        self.recv_internal(Some(Duration::from_secs(timeout)))
    }

    fn recv_internal(&mut self, timeout: Option<Duration>) -> Result<Option<Message>, Error> {

        log::trace!("recv_internal() with timeout {:?}", timeout);

        if let Err(e) = self.tcp_stream.set_read_timeout(timeout) {
            log::error!("Invalid timeout: {timeout:?} {e}");
            return Err(Error::NetworkError);
        }

        let mut text = String::from("");

        loop {
            let mut buf: [u8; READ_BUFSIZE] = [0; READ_BUFSIZE];

            let num_bytes = match self.tcp_stream.read(&mut buf) {
                Ok(num) => num,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            log::trace!("SIP tcp read timed out.  trying again");
                            continue;
                        },
                        _ => {
                            log::error!("recv() failed: {e}");
                            return Err(Error::NetworkError);
                        }
                    }
                }
            };

            if num_bytes == 0 {
                break;
            }

            let chunk = match str::from_utf8(&buf) {
                Ok(s) => s,
                Err(s) => {
                    log::error!("recv() got non-utf data: {}", s);
                    return Err(Error::MessageFormatError);
                }
            };

            text.push_str(chunk);

            if num_bytes < READ_BUFSIZE {
                break;
            }
        }

        if text.len() == 0 {
            // Receiving none with no timeout is an error.
            log::warn!("Reading TCP stream returned 0 bytes");
            return Err(Error::NoResponseError);
        }

        // Discard the line terminator and any junk after it.
        let mut parts = text.split(spec::LINE_TERMINATOR);

        match parts.next() {
            Some(s) => {
                log::info!("INBOUND: {}", s);
                let msg = Message::from_sip(s)?;
                return Ok(Some(msg));
            }
            None => Err(Error::MessageFormatError),
        }
    }

    /// Shortcut for:  self.send(msg); resp = self.recv();
    pub fn sendrecv(&mut self, msg: &Message) -> Result<Message, Error> {
        self.send(msg)?;
        self.recv()
    }
}
