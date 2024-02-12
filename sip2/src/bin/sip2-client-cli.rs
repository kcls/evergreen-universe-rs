use getopts;
use sip2::*;
use std::env;

const DEFAULT_HOST: &str = "localhost:6001";

const HELP_TEXT: &str = r#"

Required:

    --sip-host <host:port>
    --sip-user <username>
    --sip-pass <password>

Params for Sending SIP Requests

    --message-type <mtype>

        Repeatable.  Options include:
            "item-information"

    --patron-barcode <barcode>
    --patron-pass <password>
    --item-barcode <barcode>

"#;

fn print_err(err: &str) -> String {
    format!("\n\nError: {}\n\n------{}", err, HELP_TEXT)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("h", "sip-host", "SIP Host", "HOST");
    opts.optopt("u", "sip-user", "SIP User", "USER");
    opts.optopt("w", "sip-pass", "SIP pass", "PASSWORD");

    // Optional
    opts.optopt("b", "patron-barcode", "Patron Barcode", "PATRON-BARCODE");
    opts.optopt("p", "patron-password", "Patron Password", "PATRON-PASSWORD");
    opts.optopt("i", "item-barcode", "Item Barcode", "ITEM-BARCODE");
    opts.optopt("l", "location-code", "Location Code", "LOCATION-CODE");
    opts.optmulti("", "message-type", "Message Type", "");

    let options = opts
        .parse(&args[1..])
        .expect("Error parsing command line options");

    let host = options.opt_str("sip-host").unwrap_or(DEFAULT_HOST.to_string());

    let user = options
        .opt_str("sip-user")
        .expect(&print_err("--sip-user required"));
    let pass = options
        .opt_str("sip-pass")
        .expect(&print_err("--sip-pass required"));

    let messages = options.opt_strs("message-type");

    // Connect to the SIP server
    let mut client = Client::new(&host).expect("Cannot Connect");

    // ParamSet can hold params for a variety of (but not all) SIP
    // requests.  We can keep appending values and reuse the same
    // paramset for all request below.
    let mut params = ParamSet::new();
    params.set_sip_user(&user).set_sip_pass(&pass);

    if let Some(location) = options.opt_str("location-code") {
        params.set_location(&location);
    }

    // Login to the SIP server
    match client.login(&params).expect("Login Error").ok() {
        true => println!("Login OK"),
        false => eprintln!("Login Failed"),
    }

    // Check the SIP server status
    match client.sc_status().expect("SC Status Error").ok() {
        true => println!("SC Status OK"),
        false => eprintln!("SC Status Says Offline"),
    }

    // Collect some params up front for ease of use.
    if let Some(item_id) = options.opt_str("item-barcode") {
        params.set_item_id(&item_id);
    }

    if let Some(patron_id) = options.opt_str("patron-barcode") {
        params.set_patron_id(&patron_id);
    }

    if let Some(patron_pwd) = options.opt_str("patron-password") {
        params.set_patron_pwd(&patron_pwd);
    }

    // Send the requested message types.
    for message in messages {
        let resp = match message.as_str() {

            "item-information" => {
                client.item_info(&params).expect("Item Info Failed")
            }

            "patron-status" => {
                client.patron_status(&params).expect("Patron Status Failed")
            }

            "patron-information" => {
                client.patron_info(&params).expect("Patron Information Failed")
            }

            _ => panic!("Unsupported message type: {}", message),
        };

        println!("{}", resp.msg());
    }
}

