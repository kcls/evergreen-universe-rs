use crate::message::*;
use crate::Z39ConnectRequest;

use rasn::types::BitString;

use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

const BUFSIZE: usize = 1024;

// Copy Yaz values here.
const PREF_VALUE_SIZE: u32 = 67108864;

const IMPLEMENTATION_ID: &str = "EG";
const IMPLEMENTATION_NAME: &str = "Evergreen";
const IMPLEMENTATION_VERSION: &str = "0.1.0";


#[derive(Debug)]
pub(crate) struct Z39Session {
    id: u64,
    tcp_stream: Option<TcpStream>,
}

impl Z39Session {
    pub fn new(id: u64) -> Self {
        Z39Session {
            id,
            tcp_stream: None,
        }
    }

    /// Panics if the stream is None.
    fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        self.tcp_stream.as_mut().unwrap()
    }

    fn handle_message(&mut self, message: Message) -> Result<(), String> {
        println!("REQ: {message:?}");

        match message.payload() {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r),
            _ => todo!(),
        }
    }

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> Result<(), String> {

       let resp = InitializeResponse {
            reference_id: None,
            protocol_version: BitString::repeat(true, 3),
            options: BitString::repeat(true, 16), // TODO
            preferred_message_size: PREF_VALUE_SIZE,
            exceptional_record_size: PREF_VALUE_SIZE,
            result: Some(true),
            implementation_id: Some(IMPLEMENTATION_ID.to_string()),
            implementation_name: Some(IMPLEMENTATION_NAME.to_string()),
            implementation_version: Some(IMPLEMENTATION_VERSION.to_string()),
        };

        let bytes = Message::from_payload(MessagePayload::InitializeResponse(resp)).to_bytes()?;

        self.tcp_stream_mut().write_all(bytes.as_slice()).map_err(|e| e.to_string())
    }
}

impl mptc::RequestHandler for Z39Session {
    fn worker_start(&mut self) -> Result<(), String> {
        println!("Z39Session::worker_start({})", self.id);
        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        println!("Z39Session::worker_end({})", self.id);
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        println!("Z39Session::process({})", self.id);

        // Turn the general mptc::Request into a type we can perform actions on.
        let request = Z39ConnectRequest::downcast(&mut request);

        // Z39ConnectRequest's only real job was to pass us the stream.
        self.tcp_stream = request.tcp_stream.take();

        let mut bytes = Vec::new();
        let mut buffer = [0u8; BUFSIZE];

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete object/message is formed.  Handle
        // the message, rinse and repeat.
        while let Ok(count) = self.tcp_stream_mut().read(&mut buffer) {
            if count == 0 {
                log::debug!("client socket shutdown.  exiting");
                break;
            }

            bytes.extend_from_slice(&buffer);

            match Message::from_bytes(&bytes) {
                Ok(op) => if let Some(msg) = op {
                    self.handle_message(msg)?; // TODO
                } else {
                    // More bytes needed.
                    continue;
                }
                Err(e) => {
                    log::error!("Cannot parse message: {e} {bytes:?}");
                    break;
                }
            }
        }

        /*
        request
            .tcp_stream
            .write_all(format!("Replying from {:?}: ", std::thread::current().id()).as_bytes())
            .expect("Stream.write()");

        request
            .tcp_stream
            .write_all(&buffer[..count])
            .expect("Stream.write()");
        */

        self.tcp_stream_mut().shutdown(std::net::Shutdown::Both).ok();

        Ok(())
    }
}
