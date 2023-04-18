use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write}; // needed by TcpStream
use std::any::Any;
use mptc;

struct TcpEchoRequest {
    stream: TcpStream,
}

impl TcpEchoRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut TcpEchoRequest {
        h.as_any_mut().downcast_mut::<TcpEchoRequest>()
            .expect("TcpEchoRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for TcpEchoRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct TcpEchoHandler {
    /// Example of a thread-local value
    id: u64,
}

impl mptc::RequestHandler for TcpEchoHandler {
    fn thread_start(&mut self) -> Result<(), String> {
        println!("TcpEchoHandler::thread_start({})", self.id);
        Ok(())
    }

    fn thread_end(&mut self) -> Result<(), String> {
        println!("TcpEchoHandler::thread_end({})", self.id);
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        println!("TcpEchoHandler::process({})", self.id);

        // Turn the generalic mptc::Request into a type we can perform actions on.
        let request = TcpEchoRequest::downcast(&mut request);

        let mut buffer = [0u8; 1024];
        request.stream.read(&mut buffer).expect("Stream.read()");

        // Trim the null bytes from our read buffer.
        let buffer: Vec<u8> = buffer.iter().map(|c| *c).filter(|c| c != &0u8).collect();

        request.stream.write_all(buffer.as_slice()).expect("Stream.write()");
        request.stream.shutdown(std::net::Shutdown::Both).expect("shutdown()");

        Ok(())
    }
}

struct TcpEchoStream {
    listener: TcpListener,
    id_gen: u64,
}

impl mptc::RequestStream for TcpEchoStream {
    fn next(&mut self, _timeout: u64) -> Result<Option<Box<dyn mptc::Request>>, String> {
        // NOTE use the socket2 crate to apply read timeouts to TcpStreams

        let (stream, _addr) = self.listener.accept()
            .or_else(|e| Err(format!("Accept failed: {e}")))?;

        let request = TcpEchoRequest { stream: stream };
        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        self.id_gen += 1;
        let h = TcpEchoHandler {id: self.id_gen};
        Box::new(h)
    }
}

fn main() {
    let stream = TcpEchoStream {
        id_gen: 0,
        listener: TcpListener::bind("127.0.0.1:7878").unwrap(),
    };
    let mut s = mptc::Server::new(Box::new(stream));
    s.run();
}
