use eg::common::auth;
use eg::osrf::message;
use eg::util;
use eg::EgValue;
use evergreen as eg;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use tungstenite as ws;
use ws::protocol::Message;
use ws::protocol::WebSocket;
use ws::stream::MaybeTlsStream;

/// Each websocket client will send this many requests in a loop.
const REQS_PER_THREAD: usize = 100;

/// Number of parallel websocket clients to launch.
/// Be cautious when setting this value, especially on a production
/// system, since it's trivial to overwhelm a service with too many
/// websocket clients making API calls to the same service.
const THREAD_COUNT: usize = 10;

/// Websocket server URI.
//const DEFAULT_URI: &str = "wss://redis.demo.kclseg.org:443/osrf-websocket-translator";
const DEFAULT_URI: &str = "ws://127.0.0.1:7682";

/// How many times we repeat the entire batch.
const NUM_ITERS: usize = 20;

/// If non-zero, have each thread pause this many ms between requests.
/// Helpful for focusing on endurance / real-world traffic patterns more
/// than per-request speed.
//const REQ_PAUSE: u64 = 10;
const REQ_PAUSE: u64 = 0;

// Since we're testing Websockets, which is a public-facing gateway,
// the destination service must be a public service.
//const SERVICE: &str = "open-ils.actor";
//const SERVICE: &str = "open-ils.rs-circ";
const SERVICE: &str = "open-ils.auth";

fn main() {
    let mut batches = 0;
    let reqs_per_batch = THREAD_COUNT * REQS_PER_THREAD;

    while batches < NUM_ITERS {
        batches += 1;
        let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

        let start = Instant::now();

        while handles.len() < THREAD_COUNT {
            handles.push(thread::spawn(run_thread));
        }

        // Wait for all threads to finish.
        for h in handles {
            h.join().ok();
        }

        let duration = (start.elapsed().as_millis() as f64) / 1000.0;
        println!(
            "\n\nBatch Requests: {reqs_per_batch}; Duration: {:.3}\n",
            duration
        );
    }

    println!("Batch requests processed: {}", reqs_per_batch * NUM_ITERS);

    // uncomment to test creating a record bucket using hash-based values.
    // test_formats();
}

fn run_thread() {
    // TODO make SSL connections possible.
    // https://docs.rs/tungstenite/latest/tungstenite/client/fn.client.html
    let (mut client, _) = ws::client::connect(DEFAULT_URI).unwrap();

    let mut counter = 0;

    while counter < REQS_PER_THREAD {
        send_one_request(&mut client, counter);
        counter += 1;
        if REQ_PAUSE > 0 {
            thread::sleep(Duration::from_millis(REQ_PAUSE));
        }
    }

    client.close(None).ok();
}

fn send_one_request(client: &mut WebSocket<MaybeTlsStream<std::net::TcpStream>>, count: usize) {
    let echo = format!("Hello, World {count}");
    let echostr = echo.as_str();

    let message = json::object! {
        thread: util::random_number(12),
        service: SERVICE,
        osrf_msg: [{
            __c: "osrfMessage",
            __p: {
                threadTrace:1,
                type: "REQUEST",
                locale: "en-US",
                timezone: "America/New_York",
                api_level: 1,
                ingress: "opensrf",
                payload:{
                    __c: "osrfMethod",
                    __p:{
                        method: "opensrf.system.echo",
                        params: [echostr],
                    }
                }
            }
        }]
    };

    if let Err(e) = client.write_message(Message::text(message.dump())) {
        eprintln!("Error in send: {e}");
        return;
    }

    let response = match client.read_message() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error in recv: {e}");
            return;
        }
    };

    // NOTE this one-for-one request/response approach only works if we
    // send exactly 1 thing to echo AND the server packages the Request
    // Complete message in the same transport message as the reply.
    if let Message::Text(text) = response {
        if let Some(resp) = unpack_response(&text) {
            assert_eq!(resp.as_str().unwrap(), echostr);
            print!("+");
            std::io::stdout().flush().ok();
        }
    }
}

fn unpack_response(text: &str) -> Option<EgValue> {
    let mut ws_msg = json::parse(text).unwrap();
    let mut osrf_list = ws_msg["osrf_msg"].take();
    let osrf_msg = osrf_list[0].take();

    if osrf_msg.is_null() {
        panic!("No response from request");
    }

    let mut msg = message::Message::from_json_value(osrf_msg, true).unwrap();

    if let message::Payload::Result(ref mut res) = msg.payload_mut() {
        Some(res.take_content())
    } else if let message::Payload::Status(stat) = msg.payload() {
        if *(stat.status()) as isize >= 300 {
            panic!("Unexpected response status: {:?}", stat);
        }
        return None;
    } else {
        panic!("No response data");
    }
}

/// Testing the HASH format for parameters and responses.
fn _test_formats() {
    println!("EG init");

    let client = eg::init().expect("EG init");

    println!("Logging in");

    let args = auth::LoginArgs::new("admin", "demo123", auth::LoginType::Temp, None);
    let auth_ses = match auth::Session::login(&client, &args).expect("login()") {
        Some(s) => s,
        None => panic!("Login failed"),
    };

    let token = auth_ses.token();

    println!("Logged in OK");

    let (mut client, _) = ws::client::connect(DEFAULT_URI).unwrap();

    let name = format!("test-bucket-{}", util::random_number(8));
    let bucket = json::object! {
        "_classname": "cbreb",
        "owner": 1,
        "owning_lib": 1,
        "name": name.as_str(),
        // remaining fields are not required / have default values.
    };

    let message = json::object! {
        thread: util::random_number(12),
        service: SERVICE,
        format: "hash",
        osrf_msg: [{
            __c: "osrfMessage",
            __p: {
                threadTrace:1,
                type: "REQUEST",
                locale: "en-US",
                timezone: "America/New_York",
                api_level: 1,
                ingress: "opensrf",
                payload:{
                    __c: "osrfMethod",
                    __p:{
                        method: "open-ils.actor.container.create",
                        params: [token, "biblio", bucket.clone()]
                    }
                }
            }
        }]
    };

    if let Err(e) = client.write_message(Message::text(message.dump())) {
        eprintln!("Error in send: {e}");
        return;
    }

    let response = match client.read_message() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error in recv: {e}");
            return;
        }
    };

    if let Message::Text(text) = response {
        if let Some(resp) = unpack_response(&text) {
            println!("Bucket created returned WS response: {}", resp.dump());
        }
    }

    // Now fetch the bucket and make sure we can retrieve it as a hash

    let message = json::object! {
        thread: util::random_number(12),
        service: "open-ils.pcrud",
        format: "hash",
        osrf_msg: [{
            __c: "osrfMessage",
            __p: {
                threadTrace:1,
                type: "REQUEST",
                locale: "en-US",
                timezone: "America/New_York",
                api_level: 1,
                ingress: "opensrf",
                payload:{
                    __c: "osrfMethod",
                    __p:{
                        method: "open-ils.pcrud.search.cbreb",
                        params: [token, {"name": name.to_owned()}]
                    }
                }
            }
        }]
    };

    if let Err(e) = client.write_message(Message::text(message.dump())) {
        eprintln!("Error in send: {e}");
        return;
    }

    let response = match client.read_message() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error in recv: {e}");
            return;
        }
    };

    if let Message::Text(text) = response {
        if let Some(resp) = unpack_response(&text) {
            println!("Bucket retrieve returned WS response: {}", resp.dump());
        }
    }

    client.close(None).ok();
}
