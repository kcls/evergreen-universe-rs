use opensrf::Client;
use opensrf::SettingsClient;
use std::collections::HashMap;

//const SERVICE: &str = "opensrf.settings";
const SERVICE: &str = "open-ils.pcrud";
//const SERVICE: &str = "opensrf.rs-public";
const METHOD: &str = "opensrf.system.echo";

fn main() -> Result<(), String> {
    let mut conf = opensrf::init::init()?;
    conf.client_mut().set_domain("public.localhost");

    let conf = conf.into_shared();

    let mut client = Client::connect(conf.clone())?;

    let domain = conf.client().domain().name();

    // See what's up with the router.
    if let Some(jv) = client.send_router_command(domain, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    let mut ses = client.session(SERVICE);
    let mut ses2 = client.session(SERVICE);

    let params = vec!["hello2", "world2", "again"];

    ses.connect()?; // optional
    ses2.connect()?;

    // Request -> Receive example
    let mut req = ses.request(METHOD, &params)?;

    let mut req2 = ses2.request(METHOD, &params)?;

    while let Some(resp) = req2.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    while let Some(resp) = req.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    // Iterator example
    let params = vec!["hello2", "world2", "again"];
    for resp in ses.sendrecv(METHOD, &params)? {
        println!("Response: {}", resp.dump());
    }

    ses2.disconnect()?; // only required if ses.connect() was called
    ses.disconnect()?; // only required if ses.connect() was called

    // Example of a variety of JsonValue creation options.
    let params = vec![
        json::parse("{\"stuff\":[3, 123, null]}").unwrap(),
        json::from(HashMap::from([("more stuff", "yep")])),
        json::JsonValue::Null,
        json::from(vec![1.1, 2.0, 3.0]),
        json::object! {"just fantastic": json::array!["a", "b"]},
    ];

    for resp in client.sendrecv(SERVICE, "opensrf.system.echo", &params)? {
        println!("SYSTEM ECHO: {}", resp.dump());
    }

    /*
    for _ in 0..10 {
        if let Some(resp) = client
            .sendrecv(
                "opensrf.rs-public",
                "opensrf.rs-public.counter",
                &Vec::<u8>::new(),
            )?
            .next()
        {
            println!("opensrf.rs-public returned: {}", resp.dump());
        }
    }
    */

    Ok(())
}
