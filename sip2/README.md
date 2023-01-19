# sip2-client-rs

Rust [SIP2](https://en.wikipedia.org/wiki/Standard_Interchange_Protocol)
Client Library

## Two Modes of Operation

### Connection API

* Supports the full SIP2 specification
* Allows complete control over every fixed field and field value.
* Gracefully handles unknown / custom message fields.

### Client API

* Sits atop the Connection API and provides canned requests for common tasks.  
* Client methods allow the caller to send messages using a minimal
  number of parameters without having to create the message by hand.

## Running the example

```sh
cargo run --example sip2-client -- --sip-host sip.example.org:6001 \
    --sip-user sip-login --sip-pass sip-password \
    --patron-barcode 1234567890 --item-barcode 0987654321
```

## Connection API Example

```rs
use sip2::*;

let host = "localhost:6001";
let user = "sip-user";
let pass = "sip-pass";

let con = Connection::new(host).unwrap();

let req = Message::new(
    &spec::M_LOGIN,
    vec![
        FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
        FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
    ],
    vec![
        Field::new(spec::F_LOGIN_UID.code, user),
        Field::new(spec::F_LOGIN_PWD.code, pass),
    ]
);

let resp = con.sendrecv(&req).unwrap();

println!("Received: {}", resp);

// Verify the response reports a successful login
if resp.spec().code == spec::M_LOGIN_RESP.code
    && resp.fixed_fields().len() == 1
    && resp.fixed_fields()[0].value() == "1" {

    println!("Login OK");

} else {

    println!("Login Failed");
}

```

## Client API example


```rs
use sip2::*;

let host = "localhost:6001";
let user = "sip-user";
let pass = "sip-pass";

let mut client = Client::new(host).unwrap();

let params = ParamSet::new();
params.set_sip_user(user);
params.set_sip_pass(pass);

let resp = client.login(&params).unwrap();

prinln!("Received: {}", resp.msg());

match resp.ok() {
    true => println!("Login OK"),
    false => eprintln!("Login Failed"),
}

```


