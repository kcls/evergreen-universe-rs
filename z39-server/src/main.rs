use evergreen as eg;

use der_parser::ber::*;

use std::any::Any;
use std::io::Read; // needed by TcpStream
use std::net::{TcpListener, TcpStream};

mod messages;
use messages::Message;

const BUFSIZE: usize = 256;

struct Z39ConnectRequest {
    tcp_stream: Option<TcpStream>,
}

impl Z39ConnectRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut Z39ConnectRequest {
        h.as_any_mut()
            .downcast_mut::<Z39ConnectRequest>()
            .expect("Z39ConnectRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for Z39ConnectRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct Z39Session {
    id: u64,
    tcp_stream: Option<TcpStream>,
}

impl Z39Session {

    /// Panics if the stream is None.
    fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        self.tcp_stream.as_mut().unwrap()
    }

    fn handle_message(&mut self, message: Message) -> Result<(), String> {
        println!("REQ: {message:?}");

        Ok(())
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

            match parse_ber(&bytes) {
                Ok((rem, obj)) => {
                    log::debug!("read {} bytes\n{bytes:?}\n", bytes.len());

                    let message = match Message::from_ber(&obj) {
                        Ok(m) => m,
                        Err(e) => {
                            log::error!("could not parse bytes as BER: {e:?} bytes={bytes:?}");
                            break;
                        }
                    };

                    if let Err(e) = self.handle_message(message) {
                        log::warn!("exiting session after error: {e}");
                        break;
                    }

                    // If we have trailing bytes add them to the pile
                    // to get re-parsed on the next cycle.
                    bytes = rem.to_vec();
                }
                Err(e) => {
                    if let der_parser::asn1_rs::Err::Incomplete(_) = e {
                        continue; // More data needed.
                    } else {
                        log::error!("parsing failed: {e:?} bytes={bytes:?}");
                        break;
                    }
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

struct Z39Server {
    tcp_listener: TcpListener,
    id_gen: u64,
}

impl mptc::RequestStream for Z39Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let tcp_stream = match self.tcp_listener.accept() {
            Ok((stream, addr)) => {
                println!("z39 connect from {addr}");
                stream
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // See if we need to to into/out of ready mode.

                        // TODO
                        //self.check_heartbeat_signals();

                        // No connection received within the timeout.
                        // Return None to the mptc::Server so it can
                        // perform housekeeping.
                        return Ok(None);
                    }
                    _ => {
                        log::error!("SIPServer accept() failed {e}");
                        return Ok(None);
                    }
                }
            }
        };

        let request = Z39ConnectRequest { tcp_stream: Some(tcp_stream) };
        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        self.id_gen += 1;
        let h = Z39Session {
            id: self.id_gen,
            tcp_stream: None
        };
        Box::new(h)
    }

    fn reload(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn shutdown(&mut self) {}
}

fn main() {
    let tcp_listener = eg::util::tcp_listener(
        "127.0.0.1",
        2210,
        3, // TODO
    ).unwrap(); //  todo

    let server = Z39Server {
        id_gen: 0,
        tcp_listener,
    };

    let mut s = mptc::Server::new(Box::new(server));

    s.run();
}


