use getopts;
use sip2::*;
use std::env;
use std::time::SystemTime;

const DEFAULT_HOST: &str = "localhost:6001";

const HELP_TEXT: &str = r#"
Synopsis:

sip2-client-cli --sip-user sip-user --sip-pass sip-pass \
    --item-barcode 30000017113634                       \
    --patron-barcode 394902                             \
    --message-type item-information                     \
    --message-type patron-status                        \
    --message-type patron-information

Send messages to a SIP server and print the responses to STDOUT.

Required:

    --sip-host <host:port> [default="localhost:6001"]
    --sip-user <username>
    --sip-pass <password>

Params for Sending SIP Requests

    --message-type <mtype>

        Specify which messages to send to the SIP server.  Repeatable.  

        Options include:
            * item-information
            * patron-status
            * patron-information
            * checkout
            * checkin

    --institution <institution>
    --patron-barcode <barcode>
    --patron-pass <password>
    --item-barcode <barcode>

"#;

#[rustfmt::skip]
fn main() {
    let options = read_options();

    if options.opt_present("help") {
        println!("{HELP_TEXT}");
        return;
    }

    let sip_params = setup_params(&options);

    // Connect to the SIP server

    let host = options
        .opt_str("sip-host")
        .unwrap_or(DEFAULT_HOST.to_string());

    let mut client = Client::new(&host).expect("Cannot Connect");

    // Login to the SIP server

    match client.login(&sip_params).expect("Login Error").ok() {
        true => println!("Login OK"),
        false => eprintln!("Login Failed"),
    }

    // Check the SIP server status

    match client.sc_status().expect("SC Status Error").ok() {
        true => println!("SC Status OK"),
        false => eprintln!("SC Status Says Offline"),
    }

    // Send the requested messages

    for message in options.opt_strs("message-type") {
        let start = SystemTime::now();

        let resp = match message.as_str() {
            "item-information" => 
                client.item_info(&sip_params).expect("Item Info Requested"),

            "patron-status" => 
                client.patron_status(&sip_params).expect("Patron Status Requested"),

            "patron-information" => 
                client.patron_info(&sip_params).expect("Patron Info Requested"),

            "checkout" => 
                client.checkout(&sip_params).expect("Checkout Requested"),

            "checkin" => 
                client.checkin(&sip_params).expect("Checkin Requested"),

            _ => panic!("Unsupported message type: {}", message),
        };

        let duration = start.elapsed().unwrap().as_micros();

        // translate micros to millis retaining 3 decimal places.
        let millis = (duration as f64) / 1000.0;

        println!("{}[Duration: {:.3} ms]\n", resp.msg(), millis);
    }
}

/// Read the command line arguments
fn read_options() -> getopts::Matches {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("", "sip-host", "SIP Host", "");
    opts.optopt("", "sip-user", "SIP User", "");
    opts.optopt("", "sip-pass", "SIP pass", "");
    opts.optopt("", "institution", "Institution", "");
    opts.optopt("", "terminal-password", "Terminal Password", "");
    opts.optopt("", "patron-barcode", "Patron Barcode", "");
    opts.optopt("", "patron-password", "Patron Password", "");
    opts.optopt("", "item-barcode", "Item Barcode", "");
    opts.optopt("", "location-code", "Location Code", "");

    opts.optflag("h", "help", "");

    opts.optmulti("", "message-type", "Message Type", "");

    let matches = opts
        .parse(&args[1..]) // skip the command name
        .expect("Error parsing command line options");

    matches
}

/// Create the SIP paramater set from the command line arguments.
///
/// ParamSet can hold params for a variety of (but not all) SIP
/// requests.  We can pre-load the ParamSet for our messages.
fn setup_params(options: &getopts::Matches) -> ParamSet {
    let mut params = ParamSet::new();

    let user = options.opt_str("sip-user").expect("--sip-user required");

    let pass = options.opt_str("sip-pass").expect("--sip-pass required");

    params.set_sip_user(&user).set_sip_pass(&pass);

    if let Some(ref terminal_pwd) = options.opt_str("terminal-password") {
        params.set_terminal_pwd(terminal_pwd);
    }

    if let Some(ref institution) = options.opt_str("institution") {
        params.set_institution(institution);
    }

    if let Some(ref location) = options.opt_str("location-code") {
        params.set_location(location);
    }

    // Collect some params up front for ease of use.
    if let Some(ref item_id) = options.opt_str("item-barcode") {
        params.set_item_id(item_id);
    }

    if let Some(ref patron_id) = options.opt_str("patron-barcode") {
        params.set_patron_id(patron_id);
    }

    if let Some(ref patron_pwd) = options.opt_str("patron-password") {
        params.set_patron_pwd(patron_pwd);
    }

    params
}
