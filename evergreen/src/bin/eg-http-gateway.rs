//! Evergreen HTTP+JSON API Server
use eg::idl;
use evergreen as eg;
use mptc;
use opensrf as osrf;
use osrf::client::DataSerializer;
use socket2::{Domain, Socket, Type};
use std::any::Any;
use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const BUFSIZE: usize = 1024;
const DEFAULT_PORT: u16 = 9682;
const DEFAULT_ADDRESS: &str = "127.0.0.1";
const DUMMY_BASE_URL: &str = "http://localhost";
const HTTP_CONTENT_TYPE: &str = "Content-Type: text/json";

/// Max time we'll wait for a reply from an OpenSRF request.
/// TODO configurable?
const OSRF_RELAY_TIMEOUT: i32 = 120;

struct GatewayRequest {
    stream: TcpStream,
    address: SocketAddr,
}

impl GatewayRequest {
    pub fn downcast(h: &mut Box<dyn mptc::Request>) -> &mut GatewayRequest {
        h.as_any_mut()
            .downcast_mut::<GatewayRequest>()
            .expect("GatewayRequest::downcast() given wrong type!")
    }
}

impl mptc::Request for GatewayRequest {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
enum GatewayRequestFormat {
    Fieldmapper,
    RawSlim,
    Raw,
}

impl From<&str> for GatewayRequestFormat {
    fn from(s: &str) -> GatewayRequestFormat {
        match s {
            "raw" => Self::Raw,
            "rawslim" => Self::RawSlim,
            _ => Self::Fieldmapper,
        }
    }
}

impl GatewayRequestFormat {
    fn is_raw(&self) -> bool {
        self == &Self::Raw || self == &Self::RawSlim
    }
}

#[derive(Debug)]
struct ParsedGatewayRequest {
    service: String,
    method: Option<osrf::message::Method>,
    format: GatewayRequestFormat,
    http_method: String,
}

struct GatewayHandler {
    bus: Option<osrf::bus::Bus>,
    osrf_conf: Arc<osrf::conf::Config>,
    idl: Arc<idl::Parser>,
}

impl GatewayHandler {
    /// Mutable OpenSRF Bus ref
    ///
    /// Panics if the bus is not yet setup, which happens in thread_start()
    fn bus(&mut self) -> &mut osrf::bus::Bus {
        self.bus.as_mut().unwrap()
    }

    fn bus_conf(&self) -> &osrf::conf::BusClient {
        self.osrf_conf.gateway().unwrap()
    }

    fn handle_request(&mut self, request: &mut GatewayRequest) -> Result<(), String> {
        let text = self.read_request(request)?;
        let mut req = self.parse_request(&text)?;

        let mut leader = "HTTP/1.1 200 OK";

        let replies = match self.relay_to_osrf(&mut req) {
            Ok(r) => r,
            Err(e) => {
                leader = "HTTP/1.1 400 Bad Request";
                vec![e] // Return the raw error message as JSON.
            }
        };

        // TODO consider replying with chunks of data as responses
        // arrive from opensrf instead of saving them all into
        // an array and writing the array.

        let array = json::JsonValue::Array(replies);
        let data = array.dump();
        let length = format!("Content-length: {}", data.as_bytes().len());

        let response = match req.http_method.as_str() {
            "HEAD" => format!("{leader}\r\n{HTTP_CONTENT_TYPE}\r\n{length}\r\n\r\n"),
            "GET" | "POST" => format!("{leader}\r\n{HTTP_CONTENT_TYPE}\r\n{length}\r\n\r\n{data}"),
            _ => format!("HTTP/1.1 405 Method Not Allowed"),
        };

        if let Err(e) = request.stream.write_all(response.as_bytes()) {
            return Err(format!("Error writing to client: {e}"));
        }

        Ok(())
    }

    fn relay_to_osrf(
        &mut self,
        request: &mut ParsedGatewayRequest,
    ) -> Result<Vec<json::JsonValue>, json::JsonValue> {
        let recipient = osrf::addr::ServiceAddress::new(&request.service);

        // Send every request to the router on our gateway domain.
        let router = osrf::addr::RouterAddress::new(self.bus_conf().domain().name());

        // Avoid cloning the method which could be a big pile o' JSON.
        // We know method is non-None here.
        let method = request.method.take().unwrap();

        let tm = osrf::message::TransportMessage::with_body(
            recipient.as_str(),
            self.bus().address().as_str(),
            &osrf::util::random_number(16), // thread
            osrf::message::Message::new(
                osrf::message::MessageType::Request,
                1, // thread trace
                osrf::message::Payload::Method(method),
            ),
        );

        self.bus().send_to(&tm, router.as_str())?;

        let mut replies: Vec<json::JsonValue> = Vec::new();

        loop {
            // A request can result in any number of response messages.
            let tm = match self.bus().recv(OSRF_RELAY_TIMEOUT, None)? {
                Some(r) => r,
                None => return Ok(replies), // Timeout
            };

            let mut complete = false;
            let mut batch = self.extract_responses(&request.format, &mut complete, tm)?;

            replies.append(&mut batch);

            if complete {
                // Received a Message-Complete status
                return Ok(replies);
            }
        }
    }

    /// Extract API response values from each response message body.
    ///
    /// Returns Err if we receive an unexpected status/response value.
    fn extract_responses(
        &self,
        format: &GatewayRequestFormat,
        complete: &mut bool,
        tm: osrf::message::TransportMessage,
    ) -> Result<Vec<json::JsonValue>, json::JsonValue> {
        let mut replies: Vec<json::JsonValue> = Vec::new();

        for resp in tm.body().iter() {
            if let osrf::message::Payload::Result(resp) = resp.payload() {
                let mut content = resp.content().to_owned();

                if format.is_raw() {
                    // JSON values arrive as Fieldmapper-encoded objects.
                    // Unpacking them via the IDL turns them back
                    // into raw JSON objects.
                    content = self.idl.unpack(content);

                    if format == &GatewayRequestFormat::RawSlim {
                        content = self.scrub_nulls(content);
                    }
                }

                replies.push(content);
            } else if let osrf::message::Payload::Status(stat) = resp.payload() {
                // TODO partial messages not supported (yet).  Result of
                // osrf::client::Client not being Send-able, requiring
                // us to use raw osrf::bus::Bus.  Reconsider.
                match stat.status() {
                    osrf::message::MessageStatus::Complete => {
                        *complete = true;
                        break;
                    }
                    osrf::message::MessageStatus::Ok | osrf::message::MessageStatus::Continue => {
                        break
                    }
                    _ => return Err(stat.to_json_value()),
                }
            }
        }

        Ok(replies)
    }

    /// Remove all JSON NULL's.
    ///
    /// Used to support the RawSlim format.  Useful since raw JSON
    /// versions of Fieldmapper/IDL objects often have lots of null
    /// values, especially with virtual fields.
    fn scrub_nulls(&self, mut value: json::JsonValue) -> json::JsonValue {
        if value.is_object() {
            let mut hash = json::JsonValue::new_object();
            loop {
                let key = match value.entries().next() {
                    Some((k, _)) => k.to_owned(),
                    None => break,
                };

                let scrubbed = self.scrub_nulls(value.remove(&key));
                if !scrubbed.is_null() {
                    hash.insert(&key, scrubbed).unwrap();
                }
            }

            hash
        } else if value.is_array() {
            let mut arr = json::JsonValue::new_array();
            while value.len() > 0 {
                let scrubbed = self.scrub_nulls(value.array_remove(0));
                if !scrubbed.is_null() {
                    arr.push(scrubbed).unwrap();
                }
            }

            arr
        } else {
            value
        }
    }

    /// Pulls the raw request content from the socket and returns it
    /// as a String.
    fn read_request(&mut self, request: &mut GatewayRequest) -> Result<String, String> {
        let mut text = String::new();

        loop {
            let mut buffer = [0u8; BUFSIZE];

            // Block on the first call to read() since we know the
            // client is expecting to send us data.  After the first read,
            // set the stream to nonblocking and keep pulling values
            // until we read fewer bytes than we requested.
            let num_bytes = match request.stream.read(&mut buffer) {
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
            request
                .stream
                .set_nonblocking(true)
                .or_else(|e| Err(format!("Set nonblocking failed: {e}")))?;
        }
    }

    /// Translate a raw gateway request String into a ParsedGatewayRequest.
    ///
    /// * `request` - Full HTTP request text including headers, etc.
    ///
    /// Returns Err if the request cannot be translated.
    fn parse_request(&self, text: &str) -> Result<ParsedGatewayRequest, String> {
        let mut lines = text.split("\r\n");

        let request = lines.next().ok_or(format!("Request has no request line"))?;
        let mut request_parts = request.split_whitespace();

        let http_method = request_parts // GET, POST, etc.
            .next()
            .ok_or(format!("Request contains no method"))?;

        let get_query = request_parts // Relative URL with query params
            .next()
            .ok_or(format!("Request contains no path"))?;

        // For now, we don't really care about the headers.
        // Gobble them up and discard them.
        while let Some(header) = lines.next() {
            if header.eq("") {
                // End of headers.
                break;
            }
            log::trace!("Gateway header: {header}");
        }

        // Anything after the headers is the request body.
        // Join the remaining lines into a single string.
        let body = lines.collect::<Vec<&str>>().join("");

        // Parse the GET portion of the URL so we can extract any params
        // found there.
        let get_url = Url::parse(&format!("{}{}", DUMMY_BASE_URL, get_query))
            .or_else(|e| Err(format!("Error parsing request URL: {e}")))?;

        // Parse the request body as a URL so we can unpack any
        // POST params and add them to our parameter list.
        let post_url = Url::parse(&format!("{}?{}", DUMMY_BASE_URL, body))
            .or_else(|e| Err(format!("Error parsing request body as URL: {e}")))?;

        let mut method: Option<String> = None;
        let mut service: Option<String> = None;
        let mut params: Vec<json::JsonValue> = Vec::new();
        let mut format = GatewayRequestFormat::Fieldmapper;

        // Pack GET and POST params into a single iterator.
        let query_iter = get_url.query_pairs().chain(post_url.query_pairs());

        for (k, v) in query_iter {
            match k.as_ref() {
                "method" => method = Some(v.to_string()),
                "service" => service = Some(v.to_string()),
                "format" => format = v.as_ref().into(),
                "param" => {
                    let v =
                        json::parse(&v).or_else(|e| Err(format!("Cannot parse parameter: {e}")))?;
                    params.push(v);
                }
                _ => {} // ignore other stuff
            }
        }

        if format.is_raw() {
            // The caller is giving us raw JSON as parameter values.
            // We need to turn them into Fieldmapper-encoded values before
            // passing them to OpenSRF.
            let mut packed_params = Vec::new();
            let mut iter = params.drain(0..);
            while let Some(param) = iter.next() {
                packed_params.push(self.idl.unpack(param));
            }
            drop(iter);
            params = packed_params;
        }

        if method.is_none() {
            return Err(format!("Request contains no method name"));
        }

        if service.is_none() {
            return Err(format!("Request contains no service name"));
        }

        let m = osrf::message::Method::new(method.as_ref().unwrap(), params);

        Ok(ParsedGatewayRequest {
            format,
            service: service.unwrap(),
            method: Some(m),
            http_method: http_method.to_string(),
        })
    }
}

impl mptc::RequestHandler for GatewayHandler {
    fn thread_start(&mut self) -> Result<(), String> {
        let bus = osrf::bus::Bus::new(self.bus_conf())?;
        self.bus = Some(bus);
        Ok(())
    }

    fn thread_end(&mut self) -> Result<(), String> {
        self.bus().disconnect()?;
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let mut request = GatewayRequest::downcast(&mut request);

        log::debug!("Gateway request received from {}", request.address);

        let result = self.handle_request(&mut request);

        // Always try to shut down the request stream regardless of
        // what happened in our request handler.
        request
            .stream
            .shutdown(std::net::Shutdown::Both)
            .or_else(|e| Err(format!("Error shutting down worker stream socket: {e}")))?;

        result
    }
}

struct GatewayStream {
    listener: TcpListener,
    eg_ctx: eg::init::Context,
}

impl GatewayStream {
    fn new(eg_ctx: eg::init::Context, address: &str, port: u16) -> Result<Self, String> {
        let listener = GatewayStream::setup_listener(address, port)?;

        let stream = GatewayStream { listener, eg_ctx };

        Ok(stream)
    }

    // Setup our TcpListener with a timeout so mptc can periodically
    // wake to check for shutdown, etc. signals.
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

        socket
            .listen(128) // 128 == backlog.
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

        let request = GatewayRequest { stream, address };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let handler = GatewayHandler {
            bus: None,
            osrf_conf: self.eg_ctx.config().clone(),
            idl: self.eg_ctx.idl().clone(),
        };

        Box::new(handler)
    }
}

fn main() {
    let address = env::var("EG_HTTP_GATEWAY_ADDRESS").unwrap_or(DEFAULT_ADDRESS.to_string());

    let port = match env::var("EG_HTTP_GATEWAY_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    let init_ops = eg::init::InitOptions {
        // As a gateway, we generally won't have access to the host
        // settings, since that's typically on a private domain.
        skip_host_settings: true,

        // Skip logging so we can use the loging config in
        // the gateway() config instead.
        osrf_ops: osrf::init::InitOptions { skip_logging: true },
    };

    // Connect to OpenSRF, parse the IDL
    let eg_ctx = eg::init::init_with_options(&init_ops).expect("Evergreen init");

    // Setup logging with the gateway config
    let gateway_conf = eg_ctx
        .config()
        .gateway()
        .expect("No gateway configuration found");

    osrf::logging::Logger::new(gateway_conf.logging())
        .expect("Creating logger")
        .init()
        .expect("Logger Init");

    let stream = GatewayStream::new(eg_ctx, &address, port).expect("Build stream");
    let mut server = mptc::Server::new(Box::new(stream));

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MAX_WORKERS") {
        server.set_max_workers(n.parse::<usize>().expect("Invalid max-workers"));
    }

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MIN_WORKERS") {
        server.set_min_workers(n.parse::<usize>().expect("Invalid min-workers"));
    }

    if let Ok(n) = env::var("EG_HTTP_GATEWAY_MAX_REQUESTS") {
        server.set_max_worker_requests(n.parse::<usize>().expect("Invalid max-requests"));
    }

    server.run();
}
