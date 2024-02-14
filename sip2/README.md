# SIP2 Rust Library

Rust [SIP2](https://en.wikipedia.org/wiki/Standard_Interchange_Protocol)
Client Library

## Two Modes of Operation

### Connection API

* Supports the full SIP2 specification
* Allows complete control over every fixed field and field value.
* Gracefully handles unknown / custom message fields.

### Client API

* Sits atop the Connection API and provides canned actions for common tasks.  
* Client methods allow the caller to send messages using a minimal
  number of parameters without having to create the message by hand.

## Running the CLI

```sh
cargo run --bin sip2-client-cli -- --sip-user sip-user  \
    --sip-pass sip-pass                                 \
    --item-barcode 30000017113634                       \
    --patron-barcode 394902                             \
    --message-type item-information                     \
    --message-type patron-status                        \
    --message-type patron-information

```

## Connection API Examples

### Connection API Spec Building

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
).expect("Message Has Valid Content");

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

### Connection API Free-Text Message Building

```rs
let req = Message::from_values(
    &spec::M_LOGIN,
    &["0", "0"],
    &[("CN", "sip-username"), ("CO", "sip-password")]
).expect("Message Has Valid Content");
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


