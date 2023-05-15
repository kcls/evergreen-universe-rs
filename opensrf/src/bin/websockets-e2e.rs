use websocket::{OwnedMessage, ClientBuilder, Message};
use websocket::sync::Client;
use threadpool::ThreadPool;
use opensrf::util;
use opensrf::message;

/// Each websocket client will send this many requests in a loop.
const REQS_PER_THREAD: usize = 100;
/// Number of parallel websocket clients to launch.
/// Be cautious when setting this value, especially on a production
/// system, since it's trivial to overwhelm a service with too many
/// websocket clients making API calls to the same service.
const THREAD_COUNT: usize = 15;
const DEFAULT_URI: &str = "ws://127.0.0.1:7682";

/// How many times we repeat the entire batch.
const NUM_ITERS: usize = 5;

// Since we're testing Websockets, which is a public-facing gateway,
// the destination service must be a public service.
const SERVICE: &str = "open-ils.actor";

fn main() {
    let mut batches = 0;

    while batches < NUM_ITERS {
        batches += 1;

        let mut threads = 0;
        let pool = ThreadPool::new(THREAD_COUNT);

        while threads < THREAD_COUNT {
            pool.execute(|| run_thread());
            threads += 1;
        }

        // Wait for every thread / client in this pool to complete.
        pool.join();

        println!("");
    }
}

fn run_thread() {

    let mut client = ClientBuilder::new(DEFAULT_URI)
        .unwrap()
        .connect_insecure()
        .unwrap();

    let mut counter = 0;

    while counter < REQS_PER_THREAD {
        send_one_request(&mut client, counter);
        counter += 1;
    }
}

fn send_one_request(client: &mut Client<std::net::TcpStream>, count: usize) {
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

    if let Err(e) = client.send_message(&Message::text(message.dump())) {
        eprintln!("Error in send: {e}");
        return;
    }

    let response = match client.recv_message() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error in recv: {e}");
            return;
        }
    };

    if let OwnedMessage::Text(text) = response {

        let mut ws_msg = json::parse(&text).unwrap();
        let mut osrf_list = ws_msg["osrf_msg"].take();
        let osrf_msg = osrf_list[0].take();

        if osrf_msg.is_null() {
            panic!("No response from request");
        }

        let msg = message::Message::from_json_value(osrf_msg).unwrap();

        if let message::Payload::Result(res) = msg.payload() {
            let content = res.content();
            assert_eq!(content, &echostr);
            print!("+");
        }
    }
}

