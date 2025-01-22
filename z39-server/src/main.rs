use evergreen as eg;

use std::any::Any;
use std::net::{TcpListener, TcpStream};

mod message;
mod session;
use session::Z39Session;

pub(crate) struct Z39ConnectRequest {
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

struct Z39Server {
    tcp_listener: TcpListener,
}

impl mptc::RequestStream for Z39Server {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let tcp_stream = match self.tcp_listener.accept() {
            Ok((stream, _addr)) => stream,
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

        let request = Z39ConnectRequest {
            tcp_stream: Some(tcp_stream),
        };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        Box::new(Z39Session::default())
    }
}

fn main() {
    let tcp_listener = eg::util::tcp_listener(
        "127.0.0.1",
        2210,
        3, // TODO
    )
    .unwrap(); //  todo

    let server = Z39Server { tcp_listener };

    let mut s = mptc::Server::new(Box::new(server));

    s.run();
}
