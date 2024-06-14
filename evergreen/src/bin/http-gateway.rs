//! Evergreen HTTP+JSON Gateway
use eg::date;
use eg::idl;
use eg::osrf::conf;
use eg::osrf::logging::Logger;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::any::Any;
use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use url::Url;

const BUFSIZE: usize = 1024;
const DEFAULT_PORT: u16 = 9682;
const DEFAULT_ADDRESS: &str = "127.0.0.1";
const DUMMY_BASE_URL: &str = "http://localhost";
const HTTP_CONTENT_TYPE: &str = "Content-Type: text/json";

/// Max time we'll wait for a reply from an OpenSRF request.
/// Keep this value large and assume the proxy (eg. nginx) we sit
/// behind had sane read/write timeouts
const OSRF_RELAY_TIMEOUT: i32 = 300;
const GATEWAY_POLL_TIMEOUT: u64 = 5;

struct GatewayRequest {
    stream: TcpStream,
    address: SocketAddr,
    start_time: date::EgDate,
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

#[derive(Debug)]
struct ParsedGatewayRequest {
    service: String,
    method: Option<eg::osrf::message::MethodCall>,
    format: idl::DataFormat,
    http_method: String,
}

/// Just the stuff we need.
struct ParsedHttpRequest {
    path: String,
    method: String,
    /// Only POST requests will have an HTTP body
    body: Option<String>,
}

struct GatewayHandler {
    bus: Option<eg::osrf::bus::Bus>,
    partial_buffer: Option<String>,
}

impl GatewayHandler {
    /// Mutable OpenSRF Bus ref
    ///
    /// Panics if the bus is not yet setup, which happens in worker_start()
    fn bus(&mut self) -> &mut eg::osrf::bus::Bus {
        self.bus.as_mut().unwrap()
    }

    fn handle_request(&mut self, request: &mut GatewayRequest) -> EgResult<()> {
        // For now we asssume any error is the result of a bad request.
        // We could make the various read/parsers return something
        // more meaningful to separate, e.g., 4XX and 5XX errors.
        let mut response = eg::hash! {
            status: 400,
            payload: [],
        };

        let mut http_req = None;

        match self.read_request(request) {
            Ok(htreq) => match self.parse_request(htreq) {
                Ok(hreq) => {
                    http_req = Some(hreq);

                    // Log the call before we relay it to OpenSRF in case the
                    // request exits early on a failure.
                    self.log_request(request, http_req.as_ref().unwrap());

                    match self.relay_to_osrf(http_req.as_mut().unwrap()) {
                        Ok(list) => {
                            response["payload"] = EgValue::Array(list);
                            response["status"] = EgValue::from(200);
                        }
                        Err(e) => log::error!("relay_to_osrf() failed: {e}"),
                    }
                }
                Err(e) => log::error!("parse_request() failed: {e}"),
            },
            Err(e) => log::error!("read_request() failed: {e}"),
        }

        let data = response.dump();
        let length = format!("Content-Length: {}", data.as_bytes().len());

        let leader = if response["status"] == EgValue::Number(200.into()) {
            "HTTP/1.1 200 OK"
        } else {
            "HTTP/1.1 400 Bad Request"
        };

        // It's possible http_req failed to parse successfully
        let http_method = match http_req.as_ref() {
            Some(req) => req.http_method.as_str(),
            None => "GET",
        };

        let response = match http_method {
            "HEAD" => format!("{leader}\r\n{HTTP_CONTENT_TYPE}\r\n{length}\r\n\r\n"),
            "GET" | "POST" => format!("{leader}\r\n{HTTP_CONTENT_TYPE}\r\n{length}\r\n\r\n{data}"),
            _ => "HTTP/1.1 405 Method Not Allowed\r\n".to_string(),
        };

        if let Err(e) = request.stream.write_all(response.as_bytes()) {
            return Err(format!("Error writing to client: {e}").into());
        }

        let duration = date::now() - request.start_time;
        let millis = (duration.num_milliseconds() as f64) / 1000.0;

        log::debug!("[{}] Request duration: {:.3}s", request.address, millis);

        Ok(())
    }

    fn relay_to_osrf(&mut self, request: &mut ParsedGatewayRequest) -> EgResult<Vec<EgValue>> {
        let recipient = eg::osrf::addr::BusAddress::for_bare_service(&request.service);

        // Send every request to the router on our gateway domain.
        let router = eg::osrf::addr::BusAddress::for_router(
            conf::config().gateway().unwrap().router_name(),
            conf::config().gateway().unwrap().domain().name(),
        );

        // Avoid cloning the method which could be a big pile o' JSON.
        // We know method is non-None here.
        let method = request.method.take().unwrap();

        let tm = eg::osrf::message::TransportMessage::with_body(
            recipient.as_str(),
            self.bus().address().as_str(),
            &eg::util::random_number(16), // thread
            eg::osrf::message::Message::new(
                eg::osrf::message::MessageType::Request,
                1, // thread trace
                eg::osrf::message::Payload::Method(method),
            ),
        );

        self.bus().send_to(tm, router.as_str())?;

        let mut replies: Vec<EgValue> = Vec::new();

        loop {
            // A request can result in any number of response messages.
            let tm = match self.bus().recv(OSRF_RELAY_TIMEOUT, None)? {
                Some(r) => r,
                None => return Ok(replies), // Timeout
            };

            let mut complete = false;
            let mut batch = self.extract_osrf_responses(&request.format, &mut complete, tm)?;

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
    fn extract_osrf_responses(
        &mut self,
        format: &idl::DataFormat,
        complete: &mut bool,
        mut tm: eg::osrf::message::TransportMessage,
    ) -> EgResult<Vec<EgValue>> {
        let mut replies: Vec<EgValue> = Vec::new();

        for mut resp in tm.body_mut().drain(..) {
            if let eg::osrf::message::Payload::Result(result) = resp.payload_mut() {
                let mut content = result.take_content();

                if result.status() == &eg::osrf::message::MessageStatus::Partial {
                    let buf = match self.partial_buffer.as_mut() {
                        Some(b) => b,
                        None => {
                            self.partial_buffer = Some(String::new());
                            self.partial_buffer.as_mut().unwrap()
                        }
                    };

                    // The content of a partial message is a parital raw
                    // JSON string, representing a sub-chunk of the JSON
                    // value response as a whole.  These chunks are not
                    // parseable as JSON values.  Toss them on the buffer
                    // for later parsing.
                    if let Some(chunk) = content.as_str() {
                        buf.push_str(chunk);
                    }

                    // Not enough data yet to create a reply.  Keep reading,
                    // which may involve future calls to extract_osrf_responses()
                    continue;
                } else if result.status() == &eg::osrf::message::MessageStatus::PartialComplete {
                    // Take + clear the partial buffer.
                    let mut buf = match self.partial_buffer.take() {
                        Some(b) => b,
                        None => String::new(),
                    };

                    // Append any trailing content if available.
                    if let Some(chunk) = content.as_str() {
                        buf.push_str(chunk);
                    }

                    // Parse the collected chunks as a the final JSON value.
                    content = EgValue::parse(&buf).map_err(|e| format!("Error reconstituting partial message: {e}"))?;
                }

                if format.is_hash() {
                    // JSON replies arrive from opensrf as Fieldmapper-encoded
                    // objects.  Decode them into flat hashes for the caller.
                    content.to_classed_hash();

                    if format == &idl::DataFormat::Hash {
                        // If the caller specifically requests the Hash
                        // format remove all the null hash values as well.
                        content.scrub_hash_nulls();
                    }
                }

                replies.push(content);
            } else if let eg::osrf::message::Payload::Status(stat) = resp.payload() {
                match stat.status() {
                    eg::osrf::message::MessageStatus::Complete => {
                        *complete = true;
                    }
                    eg::osrf::message::MessageStatus::Ok
                    | eg::osrf::message::MessageStatus::Continue => {
                        // Keep reading in case there's more data in the message.
                    }
                    _ => return Err(stat.clone().into_json_value().dump().into()),
                }
            }
        }

        Ok(replies)
    }

    /// Pulls the raw request content from the socket and returns it
    /// as a String.
    fn read_request(&mut self, request: &mut GatewayRequest) -> EgResult<ParsedHttpRequest> {
        // It's assumed we don't need a timeout on the tcpstream for
        // any reads because we sit behind a proxy-like thing
        // (e.g. nginx) that applies reasonable read/write timeouts
        // for HTTP clients.

        let mut header_byte_count = 0;
        let mut parsed_req = None;
        let mut content_length = 0;
        let mut chars: Vec<u8> = Vec::new();

        loop {
            // Pull a chunk of bytes from the stream and see what we can
            // do with it.
            let mut buffer = [0u8; BUFSIZE];

            let num_bytes = request
                .stream
                .read(&mut buffer).map_err(|e| format!("Error reading HTTP stream: {e}"))?;

            log::trace!("Read {num_bytes} from the TCP stream");

            for c in buffer.iter() {
                if *c == 0 {
                    // Drop any trailing '\0' chars.
                    break;
                }
                chars.push(*c);
            }

            if parsed_req.is_none() {
                // Parse the headers and extract the values we care about.

                let mut headers = [httparse::EMPTY_HEADER; 64];
                let mut req = httparse::Request::new(&mut headers);

                log::trace!(
                    "Parsing chars: {}",
                    String::from_utf8_lossy(chars.as_slice())
                );

                let res = req
                    .parse(chars.as_slice()).map_err(|e| format!("Error readong HTTP headers: {e}"))?;

                if res.is_partial() {
                    // We haven't read enough header data yet.
                    // Go back to pulling bytes from the socket.
                    continue;
                }

                // httparse::Result contains the byte count of the header
                // once full parsed.
                header_byte_count = res.unwrap();

                for header in req.headers.iter() {
                    if header.name.to_lowercase().as_str() == "content-length" {
                        let len = String::from_utf8_lossy(header.value);
                        if let Ok(size) = len.parse::<usize>() {
                            content_length = size;
                            break;
                        }
                    }
                }

                let method = req
                    .method
                    .map(|v| v.to_string())
                    .ok_or("Invalid HTTP request".to_string())?;

                let path = req
                    .path
                    .map(|v| v.to_string())
                    .ok_or("Invalid HTTP request".to_string())?;

                parsed_req = Some(ParsedHttpRequest {
                    method,
                    path,
                    body: None,
                });
            }

            if chars.len() == header_byte_count {
                // We have read zero bytes of body data.
                // There may be none to read.

                if content_length == 0 {
                    return Ok(parsed_req.take().unwrap());
                }

                // We have a non-zero content-length.
                // Keep reading data.
                continue;
            }

            let body_bytes = &chars[header_byte_count..];
            let body_byte_count = body_bytes.len();

            log::trace!("Read {body_byte_count} body bytes, want {content_length}");

            if body_byte_count == content_length {
                // We've read all the body data.
                let mut parsed_req = parsed_req.take().unwrap();

                parsed_req.body = Some(String::from_utf8_lossy(body_bytes).to_string());

                return Ok(parsed_req);
            }

            if body_byte_count > content_length {
                return Err("Content exceeds Content-Length header value".to_string().into());
            }

            // Keep reading data until body_byte_count >= content_length
        }
    }

    /// Translate a raw gateway request String into a ParsedGatewayRequest.
    ///
    /// * `request` - Full HTTP request text including headers, etc.
    ///
    /// Returns Err if the request cannot be translated.
    fn parse_request(&self, http_req: ParsedHttpRequest) -> EgResult<ParsedGatewayRequest> {
        let url_params = match http_req.body {
            // POST params are in the body
            Some(b) => format!("{}?{}", DUMMY_BASE_URL, &b),
            // GET Params are in the path.
            None => format!("{}{}", DUMMY_BASE_URL, &http_req.path),
        };

        let parsed_url = Url::parse(&url_params).map_err(|e| format!("Error parsing request params: {e}"))?;

        let mut method: Option<String> = None;
        let mut service: Option<String> = None;
        let mut params: Vec<EgValue> = Vec::new();
        let mut format = idl::DataFormat::Fieldmapper;

        // First see if the caller requested a format so we can
        // apply the needed changes while parsing the data below.
        for (k, v) in parsed_url.query_pairs() {
            if k.as_ref() == "format" {
                format = v.as_ref().into();
            }
        }

        for (k, v) in parsed_url.query_pairs() {
            match k.as_ref() {
                "method" => method = Some(v.to_string()),
                "service" => service = Some(v.to_string()),
                "param" => {
                    let jval = json::parse(&v).map_err(|e| format!("Cannot parse parameter: {e} : {v}"))?;

                    let val;
                    if format.is_hash() {
                        // Caller is sending flat-hash parameters.
                        // Translate them into Fieldmapper parameters
                        // before relaying them to opensrf.
                        val = EgValue::from_classed_json_hash(jval)?;
                    } else {
                        // Caller is sending array-based Fieldmapper IDL value.
                        val = EgValue::from_json_value(jval)?;
                    }

                    params.push(val);
                }
                _ => {} // ignore other stuff
            }
        }

        let method = method
            .as_ref()
            .ok_or("Request contains no method name".to_string())?;

        let service = service.ok_or("Request contains no service name".to_string())?;

        let osrf_method = eg::osrf::message::MethodCall::new(method, params);

        Ok(ParsedGatewayRequest {
            format,
            service,
            method: Some(osrf_method),
            http_method: http_req.method.to_string(),
        })
    }

    fn log_request(&self, request: &GatewayRequest, req: &ParsedGatewayRequest) {
        let method = req.method.as_ref().unwrap();

        let log_params = eg::util::stringify_params(
            method.method(),
            method.params(),
            conf::config().log_protect(),
        );

        log::info!(
            "ACT:[{}] {} {} {}",
            request.address,
            req.service,
            method.method(),
            log_params
        );

        // Also log as INFO e.g. gateway.xx.log
        log::info!(
            "[{}] {} {} {}",
            request.address,
            req.service,
            method.method(),
            log_params
        );
    }
}

impl mptc::RequestHandler for GatewayHandler {
    fn worker_start(&mut self) -> Result<(), String> {
        let gconf = conf::config().gateway().expect("Gateway Config Required");
        let bus = eg::osrf::bus::Bus::new(gconf)?;
        self.bus = Some(bus);
        Ok(())
    }

    fn worker_end(&mut self) -> Result<(), String> {
        // Bus will be cleaned up on thread exit -> Drop
        Ok(())
    }

    fn process(&mut self, mut request: Box<dyn mptc::Request>) -> Result<(), String> {
        let request = GatewayRequest::downcast(&mut request);

        log::debug!("[{}] Gateway request received", request.address);

        let result = self.handle_request(request);

        // Always try to shut down the request stream regardless of
        // what happened in our request handler.
        request
            .stream
            .shutdown(std::net::Shutdown::Both).map_err(|e| format!("Error shutting down worker stream socket: {e}"))?;

        result.map_err(|e| format!("{e}"))
    }
}

struct GatewayStream {
    listener: TcpListener,
}

impl GatewayStream {
    fn new(address: &str, port: u16) -> EgResult<Self> {
        log::info!("EG Gateway listening at {address}:{port}");

        let listener =
            eg::util::tcp_listener(address, port, GATEWAY_POLL_TIMEOUT).map_err(|e| format!(
                    "Cannot listen for connections on {address}:{port} {e}"
                ))?;

        let stream = GatewayStream { listener };

        Ok(stream)
    }
}

impl mptc::RequestStream for GatewayStream {
    /// Returns the next client request stream.
    fn next(&mut self) -> Result<Option<Box<dyn mptc::Request>>, String> {
        let (stream, address) = match self.listener.accept() {
            Ok((s, a)) => (s, a),
            Err(e) => match e.kind() {
                // socket read timeout.
                std::io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(format!("accept() failed: {e}")),
            },
        };

        // Every new request gets its own log trace.
        Logger::mk_log_trace();

        let request = GatewayRequest {
            stream,
            address,
            start_time: date::now(),
        };

        Ok(Some(Box::new(request)))
    }

    fn new_handler(&mut self) -> Box<dyn mptc::RequestHandler> {
        let handler = GatewayHandler {
            bus: None,
            partial_buffer: None,
        };

        Box::new(handler)
    }

    fn reload(&mut self) -> Result<(), String> {
        // We have no config file to reload.
        Ok(())
    }

    fn shutdown(&mut self) {
        // Our wokers only handle one request, then exit.  No
        // need to notify them of emminent shutdown.
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
        skip_logging: true,
        appname: Some(String::from("http-gateway")),
    };

    // Connect to OpenSRF, parse the IDL
    // NOTE: Since we are not fetching host settings, we use
    // the default IDL path unless it's overridden with the
    // EG_IDL_FILE environment variable.
    eg::init::with_options(&init_ops).expect("Evergreen init");

    // Setup logging with the gateway config
    let gateway_conf = conf::config().gateway().expect("Gateway config Required");

    eg::osrf::logging::Logger::new(gateway_conf.logging())
        .expect("Creating logger")
        .init()
        .expect("Logger Init");

    let stream = GatewayStream::new(&address, port).expect("Build stream");
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
