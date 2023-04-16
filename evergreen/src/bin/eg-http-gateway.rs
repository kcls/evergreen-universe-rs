//! Evergreen HTTP+JSON API Server
use evergreen as eg;
use opensrf as osrf;
use osrf::worker::WorkerState;
use osrf::worker::WorkerStateEvent;
use socket2::{Domain, Socket, Type};
use std::env;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use std::thread;
use url::Url;

const DEFAULT_PORT: u16 = 9682;
const MAX_WORKERS: usize = 128;
const MIN_WORKERS: usize = 4;
const POLL_INTERVAL: u64 = 3;
const BUFSIZE: usize = 1024;
const DUMMY_BASE_URL: &str = "http://localhost";
const MAX_REQUESTS: usize = 10_000;

fn main() {
    let mut server = setup_server();

    // Use the logging config from the gateway config chunk
    let gateway = server
        .ctx
        .config()
        .gateway()
        .expect("No gateway configuration found");

    let logger = osrf::logging::Logger::new(gateway.logging()).expect("Creating logger");

    logger.init().expect("Logger Init");

    if let Err(e) = server.run() {
        log::error!("Gateway exited with error: {e}");
    }
}

fn setup_server() -> Server {
    let address = match env::var("EG_HTTP_GATEWAY_ADDRESS") {
        Ok(v) => v,
        _ => "127.0.0.1".to_string(),
    };

    let port = match env::var("EG_HTTP_GATEWAY_PORT") {
        Ok(v) => v.parse::<u16>().expect("Invalid port number"),
        _ => DEFAULT_PORT,
    };

    let max_workers = match env::var("EG_HTTP_GATEWAY_MAX_WORKERS") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-workers value"),
        _ => MAX_WORKERS,
    };

    let min_workers = match env::var("EG_HTTP_GATEWAY_MIN_WORKERS") {
        Ok(v) => v.parse::<usize>().expect("Invalid min-workers value"),
        _ => MIN_WORKERS,
    };

    let max_requests = match env::var("EG_HTTP_GATEWAY_MAX_REQUESTS") {
        Ok(v) => v.parse::<usize>().expect("Invalid max-requests value"),
        _ => MAX_REQUESTS,
    };

    let init_ops = eg::init::InitOptions {
        skip_host_settings: true,
        osrf_ops: osrf::init::InitOptions { skip_logging: true },
    };

    let context = match eg::init::init_with_options(&init_ops) {
        Ok(c) => c,
        Err(e) => panic!("Cannot init: {}", e),
    };

    let (tx, rx): (
        mpsc::Sender<WorkerStateEvent>,
        mpsc::Receiver<WorkerStateEvent>,
    ) = mpsc::channel();

    Server {
        address,
        port,
        max_workers,
        min_workers,
        max_requests,
        ctx: context,
        worker_id_gen: 0,
        workers: HashMap::new(),
        shutdown: Arc::new(AtomicBool::new(false)),
        to_server_tx: tx,
        to_server_rx: rx,
    }
}

struct Server {
    port: u16,
    address: String,
    max_workers: usize,
    min_workers: usize,
    max_requests: usize,
    ctx: eg::init::Context,
    worker_id_gen: u64,
    shutdown: Arc<AtomicBool>,
    workers: HashMap<u64, WorkerThread>,

    /// Channels for sending worker state events to the main server thread.
    to_server_tx: mpsc::Sender<WorkerStateEvent>,
    to_server_rx: mpsc::Receiver<WorkerStateEvent>,
}

impl Server {
    fn next_worker_id(&mut self) -> u64 {
        self.worker_id_gen += 1;
        self.worker_id_gen
    }

    fn setup_listener(&mut self) -> Result<TcpListener, String> {
        let destination = format!("{}:{}", &self.address, self.port);
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
        let polltime = Duration::from_secs(POLL_INTERVAL);

        socket
            .set_read_timeout(Some(polltime))
            .or_else(|e| Err(format!("Error setting socket read_timeout: {e}")))?;

        Ok(socket.into())
    }

    fn run(&mut self) -> Result<(), String> {
        self.spawn_threads()?;
        let listener = self.setup_listener()?;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let client_socket = match listener.accept() {
                Ok((s, _)) => s,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            continue; // Poll timeout, keep going.
                        }
                        _ => {
                            log::error!("accept() failed: {e}");
                            continue;
                        }
                    }
                }
            };

            self.dispatch(client_socket.into());

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            if let Ok(evt) = self.to_server_rx.try_recv() {
                self.handle_worker_event(&evt);
            }

            self.check_failed_threads();

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }

        self.ctx.client().clear().ok();

        log::debug!("Server shutting down; waiting for threads to complete");

        // TODO join threads

        log::debug!("All threads complete.  Shutting down");

        Ok(())
    }

    /// Set the state of our thread worker based on the state reported
    /// to us by the thread.
    fn handle_worker_event(&mut self, evt: &WorkerStateEvent) {
        log::trace!("server received WorkerStateEvent: {:?}", evt);

        let worker_id = evt.worker_id();

        let worker: &mut WorkerThread = match self.workers.get_mut(&worker_id) {
            Some(w) => w,
            None => {
                log::error!("No worker found with id {worker_id}");
                return;
            }
        };

        if evt.state() == WorkerState::Done {
            // Worker is done -- remove it and fire up new ones as needed.
            self.remove_thread(&worker_id);
        } else {
            log::trace!("server: updating thread state: {:?}", worker_id);
            worker.state = evt.state();
        }

        let idle = self.idle_worker_count();
        let active = self.active_worker_count();

        log::trace!("server: workers idle={idle} active={active}");

        if idle == 0 {
            // Try to keep at least one idle worker on retainer.
            if active < self.max_workers {
                self.spawn_one_thread();
            } else {
                log::warn!("server: reached max workers!");
            }
        }
    }

    // Check for threads that panic!ed and were unable to send any
    // worker state info to us.
    fn check_failed_threads(&mut self) {
        let failed: Vec<u64> = self
            .workers
            .iter()
            .filter(|(_, v)| v.join_handle.is_finished())
            .map(|(k, _)| *k) // k is a &u64
            .collect();

        for worker_id in failed {
            log::info!("Found a thread that exited ungracefully: {worker_id}");
            self.remove_thread(&worker_id);
        }
    }


    fn remove_thread(&mut self, worker_id: &u64) {
        log::trace!("server: removing thread {}", worker_id);
        self.workers.remove(worker_id);
        self.spawn_threads();
    }

    fn spawn_threads(&mut self) -> Result<(), String> {
        while self.workers.len() < self.min_workers {
            self.spawn_one_thread()?;
        }

        Ok(())
    }

    fn spawn_one_thread(&mut self) -> Result<u64, String> {
        let idl = self.ctx.idl().clone();
        let osrf_config = self.ctx.config().clone();
        let worker_id = self.next_worker_id();
        let max_reqs = self.max_requests;
        let shutdown = self.shutdown.clone();
        let to_server_tx = self.to_server_tx.clone();

        // Channel for sending a stream to the worker for processing.
        let (tx, rx): (
            mpsc::Sender<TcpStream>,
            mpsc::Receiver<TcpStream>,
        ) = mpsc::channel();

        let handle: thread::JoinHandle<()> = thread::spawn(move || {
            Worker::start(
                worker_id,
                max_reqs,
                osrf_config,
                idl,
                shutdown,
                to_server_tx,
                rx
            )
        });

        let wt = WorkerThread {
            join_handle: handle,
            state: WorkerState::Idle,
            to_worker_tx: tx,
        };

        self.workers.insert(worker_id, wt);

        Ok(worker_id)
    }

    fn active_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Active)
            .count()
    }

    fn idle_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|v| v.state == WorkerState::Idle)
            .count()
    }

    fn get_idle_worker(&mut self) -> Result<&mut WorkerThread, String> {

        loop {

            // First look for an existing idle thread
            let id_op = self.workers
                .iter()
                .filter(|(_, v)| v.state == WorkerState::Idle)
                .map(|(k, _)| *k) // &u64
                .next();

            if id_op.is_some() {
                return Ok(self.workers.get_mut(id_op.as_ref().unwrap()).unwrap());
            }

            // Otherwise, see if we can create a new thread.
            if self.workers.len() < self.max_workers {
                let worker_id = self.spawn_one_thread()?;
                return Ok(self.workers.get_mut(&worker_id).unwrap());
            }

            log::warn!("We've reach max workers.  Waiting for a worker to finish...");

            // We've hit max threads.  Wait for a busy worker to
            // become available folllowed by a panic!ed thread check.
            if let Ok(evt) = self.to_server_rx.recv_timeout(Duration::from_secs(1)) {
                // This will spawn a new worker for us if it can.
                self.handle_worker_event(&evt);
            }

            self.check_failed_threads();
        }
    }

    fn dispatch(&mut self, stream: TcpStream) -> Result<(), String> {
        let active_count = self.active_worker_count();
        let worker_thread = self.get_idle_worker()?;

        //if let Err(e) = self.

        log::debug!("Accepting new gateway connection; active={active_count}");

        Ok(())
    }
}

struct WorkerThread {
    join_handle: thread::JoinHandle<()>,
    state: WorkerState,
    to_worker_tx: mpsc::Sender<TcpStream>,
}


struct Worker {
    worker_id: u64,
    osrf_client: osrf::client::Client,
    shutdown: Arc<AtomicBool>,
    to_server_tx: mpsc::Sender<WorkerStateEvent>,
    to_worker_rx: mpsc::Receiver<TcpStream>,
}

impl Worker {

    fn start(
        worker_id: u64,
        max_requests: usize,
        config: Arc<osrf::conf::Config>,
        idl: Arc<eg::idl::Parser>,
        shutdown: Arc<AtomicBool>,
        to_server_tx: mpsc::Sender<WorkerStateEvent>,
        to_worker_rx: mpsc::Receiver<TcpStream>,
    ) {

        let mut osrf_client = match osrf::Client::connect(config.clone()) {
            Ok(c) => c,
            Err(e) => {
                log::error!("Worker cannot connect to OpenSRF: {e}");
                return;
            }
        };

        osrf_client.set_serializer(eg::idl::Parser::as_serializer(&idl));

        let mut worker = Worker {
            worker_id,
            osrf_client,
            shutdown,
            to_server_tx,
            to_worker_rx,
        };

        let mut request_count = 0;

        while request_count < max_requests {
            // TODO listen for thread channel message w/ a stream in it.
            //self.handle_request(stream);
            request_count += 1;
        }

        log::debug!("Worker {} exiting on max requests", worker.worker_id);

        worker.osrf_client.clear().ok();
    }

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
