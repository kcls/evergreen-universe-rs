use opensrf::Client;

//const SERVICE: &str = "opensrf.settings";
const SERVICE: &str = "opensrf.rs-public";
const METHOD: &str = "opensrf.system.echo";

fn main() -> Result<(), String> {
    let conf = opensrf::init::init()?;

    let mut client = Client::connect(conf.into_shared())?;

    // ---------------------------------------------------------
    // SESSION + MANUAL REQUEST --------------------------------

    let mut ses = client.session(SERVICE);

    ses.connect()?; // Optional

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request(METHOD, &params)?;

    // Loop will continue until the request is complete or a recv()
    // call times out.
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // Only required if connected

    // ---------------------------------------------------------
    // SESSION REQUEST WITH ITERATOR ---------------------------

    let mut ses = client.session(SERVICE);

    for resp in ses.sendrecv(METHOD, 12345)? {
        println!("Response: {}", resp.dump());
    }

    // --------------------------------------------------------
    // ONE-OFF REQUEST WITH ITERATOR --------------------------

    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in client.sendrecv(SERVICE, METHOD, &params)? {
        println!("Response: {}", resp.dump());
    }

    for _ in 0..10 {
        let params: Vec<u8> = vec![];
        for resp in client.sendrecv(SERVICE, "opensrf.rs-public.counter", &params)? {
            println!("Counter is {}", resp.dump());
        }
    }

    Ok(())
}
