use opensrf::Client;
use std::collections::HashMap;

fn main() -> Result<(), String> {
    let conf = opensrf::init::init()?;
    let client = Client::connect(conf.into_shared())?;

    // SESSION + MANUAL REQUEST --------------------------------
    let mut ses = client.session("opensrf.settings");

    ses.connect()?; // Optional

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request("opensrf.system.echo", params)?;

    // Loop will continue until the request is complete or a recv()
    // call times out.
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // Only required if connected


    // Variety of param creation options.
    let params = vec![
        json::parse("{\"stuff\":[3, 123, null]}").unwrap(),
        json::from(HashMap::from([("more stuff", "yep")])),
        json::JsonValue::Null,
        json::from(vec![1.1, 2.0, 3.0]),
        json::object! {"just fantastic": json::array!["a", "b"]},
    ];

    // ONE-OFF WITH ITERATOR --------------------------
    for resp in client.sendrecv("opensrf.settings", "opensrf.system.echo", params.clone())? {
        println!("Response: {}", resp.dump());
    }

    Ok(())
}
