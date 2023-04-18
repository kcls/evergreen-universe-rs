//! Evergreen HTTP+JSON API Server
use evergreen as eg;
use opensrf as osrf;
use eg::idl;
use socket2::{Domain, Socket, Type};
use std::time::Duration;
use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use url::Url;
use std::any::Any;
use mptc;

const DEFAULT_PORT: u16 = 9682;
const BUFSIZE: usize = 1024;
const DUMMY_BASE_URL: &str = "http://localhost";

struct GatewayRequest {
    stream: TcpStream,
    address: SocketAddr,
}

impl GatewayRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut GatewayRequest {
        h.as_any_mut().downcast_mut::<GatewayRequest>()
            .expect("GatewayRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for GatewayRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct GatewayHandler {
    bus: Option<osrf::bus::Bus>,
    osrf_config: Arc<osrf::conf::Config>,
    idl: Arc<idl::Parser>,
}

impl GatewayHandler {
    /// Mutable OpenSRF Bus ref
    ///
    /// Panics if the bus is not yet setup, which should happen in thread_start()
    fn bus(&mut self) -> &mut osrf::bus::Bus {
        self.bus.as_mut().unwrap()
    }
}

impl mptc::RequestHandler for GatewayHandler {

    fn thread_start(&mut self) -> Result<(), String> {
        // We confirmed we have a gateway() config in main().
        let bus_conf = self.osrf_config.gateway().unwrap();

        let bus = osrf::bus::Bus::new(bus_conf)?;
        self.bus = Some(bus);
        Ok(())
    }

    fn thread_end(&mut self) -> Result<(), String> {
        self.bus().disconnect()?;
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        // Turn the generalic mptc::Request into a type we can perform actions on.
        let request = GatewayRequest::downcast(&mut request);

        let mut buffer = [0u8; 1024];
        request.stream.read(&mut buffer).expect("Stream.read()");

        // Trim the null bytes from our read buffer.
        let buffer: Vec<u8> = buffer.iter().map(|c| *c).filter(|c| c != &0u8).collect();

        request.stream.write_all(buffer.as_slice()).expect("Stream.write()");
        request.stream.shutdown(std::net::Shutdown::Both).expect("shutdown()");

        Ok(())
    }
}

struct GatewayStream {
    listener: TcpListener,
    eg_ctx: eg::init::Context,
}

impl GatewayStream {

    fn new(eg_ctx: eg::init::Context, address: &str, port: u16) -> Result<Self, String> {
        let listener = GatewayStream::setup_listener(address, port)?;

        let stream = GatewayStream {
            listener,
            eg_ctx,
        };

        Ok(stream)
    }

    fn setup_listener(address: &str, port: u16) -> Result<TcpListener, String> {
        let destination = format!("{}:{}", address, port);

        log::info!("EG Gateway listeneing at {destination}");

        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .or_else(|e| Err(format!("Socket::new() failed with {e}")))?;

        // When we stop/start the service, the address may briefly linger
        // from open (idle) client connections.
        socket
            .set_reuse_address(true)
            .or_else(|e| Err(format!("Error setting reuse address: {e}")))?;

        let address: SocketAddr = destination
            .parse()
            .or_else(|e| Err(format!("Error parsing listen address: {destination}: {e}")))?;

        socket
            .bind(&address.into())
            .or_else(|e| Err(format!("Error binding to address: {destination}: {e}")))?;

        // 128 == backlog
        socket
            .listen(128)
            .or_else(|e| Err(format!("Error listending on socket {destination}: {e}")))?;

        // We need a read timeout so we can wake periodically to check
        // for shutdown signals.
        let polltime = Duration::from_secs(mptc::SIGNAL_POLL_INTERVAL);

        socket
            .set_read_timeout(Some(polltime))
            .or_else(|e| Err(format!("Error setting socket read_timeout: {e}")))?;

        Ok(socket.into())
    }
}

impl mptc::RequestStream for GatewayStream {

    /// Returns the next client request stream.
    ///
    /// We don't use 'timeout' here since the timeout is applied directly
    /// to our TcpStream.
    fn next(&mut self, _timeout: u64) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let (stream, address) = match self.listener.accept() {
            Ok((s, a)) => (s, a),
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // Accept call timed out.  Let the server know
                        // we received no data within the timeout provided.
                        return Ok(None);
                    }
                    _ => {
                        // Unexpected error.
                        return Err(format!("accept() failed: {e}"));
                    }
                }
            }
        };

        let request = GatewayRequest {
            stream,
            address,
        };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let handler = GatewayHandler {
            bus: None,
            osrf_config: self.eg_ctx.config().clone(),
            idl: self.eg_ctx.idl().clone(),
        };

        Box::new(handler)
    }
}

fn main() {
    let address = match env::var("EG_HTTP_GATEWAY_ADDRESS") {
        Ok(v) => v,
        _ => "127.0.0.1".to_string(),
    };

    let port = match env::var("EG_HTTP_GATEWAY_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    let init_ops = eg::init::InitOptions {
        skip_host_settings: true,
        osrf_ops: osrf::init::InitOptions { skip_logging: true },
    };

    let eg_ctx = eg::init::init_with_options(&init_ops)
        .expect("Cannot initialize Evergreen");

    // Use the logging config from the gateway config chunk
    let gateway_conf = eg_ctx.config().gateway().expect("No gateway configuration found");
    let logger = osrf::logging::Logger::new(gateway_conf.logging()).expect("Creating logger");

    logger.init().expect("Logger Init");

    let stream = GatewayStream::new(eg_ctx, &address, port).expect("Cannot buidl stream");
    let mut server = mptc::Server::new(Box::new(stream));

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MAX_WORKERS") {
        server.set_max_workers(
            n.parse::<usize>().expect("Invalid max-workers value"));
    }

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MIN_WORKERS") {
        server.set_min_workers(
            n.parse::<usize>().expect("Invalid min-workers value"));
    }

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MAX_REQUESTS") {
        server.set_max_worker_requests(
            n.parse::<usize>().expect("Invalid max-requests value"));
    }

    server.run();
}


/*


    fn handle_request(&mut self, mut stream: TcpStream) {
        let client_ip = match stream.peer_addr() {
            Ok(ip) => ip,
            Err(e) => {
                log::error!("Could not determine client IP address: {e}");
                return;
            }
        };

        log::debug!("Handling request from client {client_ip}");

        let text = match self.read_request(&mut stream) {
            Ok(t) => t,
            Err(e) => {
                // TODO 500 internal server error
                log::error!("Error reading TCP stream: {e}");
                return;
            }
        };

        let (service, method) = match self.translate_request(&text) {
            Ok((s, m)) => (s, m),
            Err(e) => {
                // TODO send 400 bad request error
                log::error!("Error translating HTTP request: {e}");
                return;
            }
        };

        let leader = "HTTP/1.1 200 OK";
        let content_type = "Content-Type: text/json";
        let data = format!(
            "[{}]",
            method
                .params()
                .iter()
                .map(|p| p.dump())
                .collect::<Vec<String>>()
                .join(",")
        );
        let content_length = format!("Content-length: {}", data.as_bytes().len());

        let reply = format!("{leader}\r\n{content_type}\r\n{content_length}\r\n\r\n{data}");

        if let Err(e) = stream.write_all(reply.as_bytes()) {
            log::error!("Error writing data to client: {e}");
        }

        if let Err(e) = stream.shutdown(std::net::Shutdown::Both) {
            log::error!("Error shutting down TCP connection: {}", e);
        }
    }

    fn read_request(&mut self, stream: &mut TcpStream) -> Result<String, String> {
        let mut text = String::new();

        loop {
            let mut buffer = [0u8; BUFSIZE];

            let num_bytes = match stream.read(&mut buffer) {
                Ok(n) => n,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => 0,
                    _ => Err(format!("Error reading HTTP stream: {e}"))?,
                },
            };

            if num_bytes > 0 {
                // Append the buffer to the string in progress, removing
                // any trailing null bytes from our pre-initialized buffer.
                text.push_str(String::from_utf8_lossy(&buffer).trim_matches(char::from(0)));
            }

            if num_bytes < BUFSIZE {
                // Reading fewer than the requested number of bytes is
                // our indication that we've read all available data.
                return Ok(text);
            }

            // If the read exceeds the buffer size, set our stream to
            // non-blocking and keep reading until there's nothing left
            // to read.
            stream
                .set_nonblocking(true)
                .or_else(|e| Err(format!("Set nonblocking failed: {e}")))?;
        }
    }

    /// Translate a gateway request into an OpenSRF Method and service name,
    /// which can be relayed to the OpenSRF network.
    ///
    /// * `request` - Full HTTP request text including headers, etc.
    fn translate_request(&self, request: &str) -> Result<(String, osrf::message::Method), String> {
        let mut parts = request.split("\r\n");

        let request = parts.next().ok_or(format!("Request has no request line"))?;
        let mut request_parts = request.split_whitespace();

        let _http_method = request_parts
            .next()
            .ok_or(format!("Request contains no method"))?;

        let pathquery = request_parts
            .next()
            .ok_or(format!("Request contains no path"))?;

        // For now, we don't really care about the headers.
        // Gobble them up and discard them.
        while let Some(header) = parts.next() {
            if header.eq("") {
                // End of headers.
                break;
            }
        }

        let path_url = Url::parse(&format!("{}{}", DUMMY_BASE_URL, pathquery))
            .or_else(|e| Err(format!("Error parsing request URL: {e}")))?;

        // Anything after the headers is the request body.
        // Join the remaining lines into a data string.
        let data = parts.collect::<Vec<&str>>().join("");

        // Parse the request body as a URL so we can unpack any
        // POST params and add them to our parameter list.
        let data_url = Url::parse(&format!("{}?{}", DUMMY_BASE_URL, data))
            .or_else(|e| Err(format!("Error parsing request body as URL: {e}")))?;

        let mut method: Option<String> = None;
        let mut service: Option<String> = None;
        let mut params: Vec<json::JsonValue> = Vec::new();
        let query_iter = path_url.query_pairs().chain(data_url.query_pairs());

        for (k, v) in query_iter {
            if k.eq("method") {
                method = Some(v.to_string());
            } else if k.eq("service") {
                service = Some(v.to_string());
            } else if k.eq("param") {
                let v = json::parse(&v)
                    .or_else(|e| Err(format!("Cannot parse parameter value as JSON: {e}")))?;
                params.push(v);
            }
        }

        if method.is_none() {
            return Err(format!("Request contains no method name"));
        }

        if service.is_none() {
            return Err(format!("Request contains no service name"));
        }

        let m = osrf::message::Method::new(method.as_ref().unwrap(), params);

        Ok((service.unwrap(), m))
    }
}

*/
