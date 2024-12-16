use sip2::{Connection, Message};

fn main() {
    // Connect to our SIP server
    let mut con = Connection::new("127.0.0.1:6001").expect("should connect");

    // Manually create a login message
    let req = Message::from_values(
        "93",
        &["0", "0"],
        &[("CN", "sip-user"), ("CO", "sip-pass")]
    ).expect("should be valid message content");

    // Send the message and wait for a response.
    let resp = con.sendrecv(&req).expect("should receive a response");

    println!("resp: {resp:?}");

    // A successful login returns a first fixed field value of "1".
    if let Some(ff) = resp.fixed_fields().first() {
        if ff.value() == "1" {
            println!("Login succeeded");
        }
    }
}

