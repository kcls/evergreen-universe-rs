use super::error::Error;
use super::spec;
use super::Message;
use deunicode::deunicode;
use std::fmt;
use std::io::prelude::*;
use std::net::{Shutdown, TcpStream};
use std::str;
use std::time::Duration;

// Read data from the socket in chunks this size.
const READ_BUFSIZE: usize = 256;

/// Manages a TCP connection to a SIP server and handles message sending
/// and receiving.
pub struct Connection {
    tcp_stream: TcpStream,

    // If set, non-ASCII chars are removed from outbound messages.
    ascii: bool,

    log_prefix: Option<String>,
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(log_prefix) = self.log_prefix.as_ref() {
            write!(f, "{log_prefix} ")
        } else {
            write!(f, "")
        }
    }
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
                log_prefix: None,
            }),
            Err(s) => {
                log::error!("Connection::new() failed: {s}");
                Err(Error::NetworkError(s.to_string()))
            }
        }
    }

    /// Create a new SIP connection from an existing TCP stream.
    pub fn from_stream(tcp_stream: TcpStream) -> Self {
        Connection {
            ascii: false,
            tcp_stream,
            log_prefix: None,
        }
    }

    /// Set the write timeout on our TCP socket to the provided duration.
    ///
    /// If this method is never called, no timeout is applied.
    ///
    /// * `timeout` - The max duration to block.
    ///   A value of None removes the timeout.
    pub fn set_send_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        if let Err(e) = self.tcp_stream.set_write_timeout(timeout) {
            log::error!("{self}Invalid timeout: {timeout:?} {e}");
            return Err(Error::NetworkError(e.to_string()));
        }

        Ok(())
    }

    /// Set the read timeout on our TCP socket to the provided duration.
    ///
    /// If this method is never called, no timeout is applied.
    ///
    /// * `timeout` - The max duration to block.
    ///   A value of None removes the timeout.
    pub fn set_recv_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        if let Err(e) = self.tcp_stream.set_read_timeout(timeout) {
            log::error!("{self}Invalid timeout: {timeout:?} {e}");
            return Err(Error::NetworkError(e.to_string()));
        }

        Ok(())
    }

    /// Add a string that will be prepended to all log:: calls where
    /// a self exists.
    pub fn set_log_prefix(&mut self, prefix: impl Into<String>) {
        self.log_prefix = Some(prefix.into());
    }

    /// Set the ascii flag
    pub fn set_ascii(&mut self, ascii: bool) {
        self.ascii = ascii;
    }

    /// Shutdown the TCP connection with the SIP server.
    pub fn disconnect(&self) -> Result<(), Error> {
        log::debug!("{self}Connection::disconnect()");

        match self.tcp_stream.shutdown(Shutdown::Both) {
            Ok(_) => Ok(()),
            Err(s) => {
                log::error!("{self}disconnect() failed: {s}");
                Err(Error::NetworkError(s.to_string()))
            }
        }
    }

    /// Send a SIP message
    ///
    /// If a send timeout is applied and the send operation times out,
    /// returns an Err.
    pub fn send(&mut self, msg: &Message) -> Result<(), Error> {
        let mut msg_sip = msg.to_sip();

        if self.ascii {
            // https://crates.io/crates/deunicode
            // "Some transliterations do produce \n characters."
            msg_sip = deunicode(&msg_sip).replace('\n', "");
        }

        // No need to redact here since SIP replies do not include passwords.
        log::info!("{self}OUTBOUND: {}", msg_sip);

        msg_sip.push(spec::LINE_TERMINATOR);

        match self.tcp_stream.write_all(msg_sip.as_bytes()) {
            Ok(_) => Ok(()),
            Err(s) => {
                log::error!("{self}send() failed: {}", s);
                Err(Error::NetworkError(s.to_string()))
            }
        }
    }

    /// Receive a SIP response.
    ///
    /// If a recv timeout is applied and the timeout is reached,
    /// returns None.
    pub fn recv(&mut self) -> Result<Option<Message>, Error> {
        let mut text = String::from("");

        loop {
            let mut buf: [u8; READ_BUFSIZE] = [0; READ_BUFSIZE];

            let num_bytes = match self.tcp_stream.read(&mut buf) {
                Ok(num) => num,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        log::trace!("{self}SIP tcp read timed out.  Returning None");
                        return Ok(None);
                    }
                    _ => {
                        log::error!("{self}recv() failed: {e}");
                        return Err(Error::NetworkError(e.to_string()));
                    }
                },
            };

            if num_bytes == 0 {
                break;
            }

            let chunk = match str::from_utf8(&buf) {
                Ok(s) => s,
                Err(s) => {
                    log::error!("{self}recv() got non-utf data: {}", s);
                    return Err(Error::MessageFormatError);
                }
            };

            text += chunk;

            if text.contains(spec::LINE_TERMINATOR) {
                // We've read a whole message.
                break;
            }
        }

        if text.is_empty() {
            // Receiving none with no timeout indicates either an error
            // or the client simply disconnected.
            log::debug!("{self}Reading TCP stream returned 0 bytes");
            return Err(Error::NoResponseError);
        }

        // SIP requests should always arrive one at a time.  Discard the
        // line/message terminator and any data that exists beyond it.
        let mut parts = text.split(spec::LINE_TERMINATOR);

        match parts.next() {
            Some(s) => {
                let msg = Message::from_sip(s)?;
                log::info!("{self}INBOUND: {}", msg.to_sip_redacted());
                Ok(Some(msg))
            }
            None => Err(Error::MessageFormatError),
        }
    }

    /// Shortcut for:  self.send(msg); resp = self.recv();
    pub fn sendrecv(&mut self, msg: &Message) -> Result<Option<Message>, Error> {
        self.send(msg)?;
        self.recv()
    }
}
