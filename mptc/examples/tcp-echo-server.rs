use std::any::Any;
use std::io::{Read, Write}; // needed by TcpStream
use std::net::{TcpListener, TcpStream};

struct TcpEchoRequest {
    stream: TcpStream,
}

impl TcpEchoRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut TcpEchoRequest {
        h.as_any_mut()
            .downcast_mut::<TcpEchoRequest>()
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
    fn worker_start(&mut self) -> Result<(), String> {
        println!("TcpEchoHandler::worker_start({})", self.id);
        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        println!("TcpEchoHandler::worker_end({})", self.id);
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        println!("TcpEchoHandler::process({})", self.id);

        // Turn the general mptc::Request into a type we can perform actions on.
        let request = TcpEchoRequest::downcast(&mut request);

        let mut buffer = [0u8; 1024];
        request.stream.read(&mut buffer).expect("Stream.read()");

        // Trim the null bytes from our read buffer.
        let buffer: Vec<u8> = buffer.iter().copied().filter(|c| c != &0u8).collect();

        request
            .stream
            .write_all(buffer.as_slice())
            .expect("Stream.write()");
        request
            .stream
            .shutdown(std::net::Shutdown::Both)
            .expect("shutdown()");

        Ok(())
    }
}

struct TcpEchoStream {
    listener: TcpListener,
    id_gen: u64,
}

impl mptc::RequestStream for TcpEchoStream {
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let (stream, _addr) = self
            .listener
            .accept().map_err(|e| format!("Accept failed: {e}"))?;

        let request = TcpEchoRequest { stream };
        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        self.id_gen += 1;
        let h = TcpEchoHandler { id: self.id_gen };
        Box::new(h)
    }

    fn reload(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn shutdown(&mut self) {}
}

fn main() {
    let stream = TcpEchoStream {
        id_gen: 0,
        listener: TcpListener::bind("127.0.0.1:7878").unwrap(),
    };
    let mut s = mptc::Server::new(Box::new(stream));
    s.run();
}
