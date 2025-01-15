use evergreen as eg;

use der_parser::ber::*;
use der_parser::der::Tag;
use der_parser::error::*;
//use der_parser::nom::HexDisplay;

use std::any::Any;
use std::io::Read; // needed by TcpStream
use std::net::{TcpListener, TcpStream};

mod messages;
use messages::Message;
use messages::InitializeRequest;

struct Z39ConnectRequest {
    tcp_stream: TcpStream,
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

#[derive(Debug, Clone)]
struct Z39Session {
    /// Example of a thread-local value
    id: u64,
}

impl Z39Session {
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


        // Feed the BER parser bytes until it returns a completed object.
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1];

        while let Ok(_) = request.tcp_stream.read(&mut buffer) {
            bytes.push(buffer[0]);

            // TODO handle remainder?
            match parse_ber(&bytes) {
                Ok((rem, obj)) => {
                    println!("Read {} bytes\n{bytes:?}\n", bytes.len());
                    let message = Message::from_ber(&obj)?;
                    println!("REQ: {message:?}");
                }
                Err(e) => {
                    if let der_parser::asn1_rs::Err::Incomplete(needed) = e {
                        continue; // More data needed.
                    } else {
                        println!("parsing failed: {e:?}");
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

        request
            .tcp_stream
            .shutdown(std::net::Shutdown::Both)
            .expect("shutdown()");

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

        let request = Z39ConnectRequest { tcp_stream };
        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        self.id_gen += 1;
        let h = Z39Session { id: self.id_gen };
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


