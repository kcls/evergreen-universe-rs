# OpenSRF Rust Bindings

## Synopsis

```rs
use opensrf::Client;

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
        json::from(std::collections::HashMap::from([("more stuff", "yep")])),
        json::JsonValue::Null,
        json::from(vec![1.1, 2.0, 3.0]),
        json::object! {"just fantastic": json::array!["a", "b"]},
    ];

    // ONE-OFF WITH ITERATOR --------------------------
    for resp in client.send_recv("opensrf.settings", "opensrf.system.echo", params.clone())? {
        println!("Response: {}", resp.dump());
    }

    // Give me a single response ----------------------
    let json_str = client
        .send_recv_one("opensrf.settings", "opensrf.system.echo", "Hello, World")?
        .expect("echo response");

    println!("GOT A: {}", json_str);

    Ok(())
}
```

## Example

```sh
cargo run --example client-demo

# Or from the root of the repository
cargo run --package opensrf --example client-demo
```
