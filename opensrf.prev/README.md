# OpenSRF Rust Bindings

## Synopsis

```rs
use opensrf::Client;

fn main() -> Result<(), String> {
    // Standard setup + connect routines.
    let conf = opensrf::init::init()?;
    let client = Client::connect(conf.into_shared())?;

    let mut ses = client.session("opensrf.settings");

    ses.connect()?; // Optional

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request("opensrf.system.echo", params)?;

    // We anticipate multiple responses.  Collect them all!
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // Only required if connected

    // ------------------------------------------------------------------
    // One-off request and we only care about the 1st response.

    let value = "Hello, World, Pamplemousse";
    let response = client
        .send_recv_one("opensrf.settings", "opensrf.system.echo", value)?
        .unwrap();

    // Client responses are json::JsonValue's
    let resp_str = response.as_str().unwrap();

    assert_eq!(resp_str, value);

    println!("Response: {resp_str}");

    Ok(())
}
```

## Example

```sh
cargo run --example client-demo

# Or from the root of the repository
cargo run --package opensrf --example client-demo
```
