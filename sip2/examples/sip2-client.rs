use getopts;
use sip2::*;
use std::env;

const HELP_TEXT: &str = r#"

Required:

    --sip-host <host:port>
    --sip-user
    --sip-pass

Optional:

    --patron-barcode
    --patron-pass
    --item-barcode

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
    opts.optopt("p", "patron-pass", "Patron Password", "PATRON-PASSWORD");
    opts.optopt("i", "item-barcode", "Item Barcode", "ITEM-BARCODE");
    opts.optopt("l", "location-code", "Location Code", "LOCATION-CODE");

    let options = opts
        .parse(&args[1..])
        .expect("Error parsing command line options");

    let host = options
        .opt_str("sip-host")
        .expect(&print_err("--sip-host required"));
    let user = options
        .opt_str("sip-user")
        .expect(&print_err("--sip-user required"));
    let pass = options
        .opt_str("sip-pass")
        .expect(&print_err("--sip-pass required"));

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

    // --- PATRON STUFF ---

    if let Some(patron_id) = options.opt_str("patron-barcode") {
        params.set_patron_id(&patron_id);
        if let Some(pass) = options.opt_str("patron-pass") {
            params.set_patron_pwd(&pass);
        }

        // Check patron status
        let resp = client.patron_status(&params).expect("Patron Status Error");

        match resp.ok() {
            true => {
                println!("Patron Info reports valid");
                if let Some(name) = resp.value("AE") {
                    println!("Patron name is '{}'", name);
                }
            }
            false => eprintln!("Patron Info reports not valid"),
        }

        params.set_summary(2); // Return details on "Charged Items"

        // Load patron info
        let resp = client.patron_info(&params).expect("Patron Info Error");

        match resp.ok() {
            true => {
                println!("Patron Info reports valid");
                if let Some(name) = resp.value("AE") {
                    println!("Patron name is '{}'", name);
                }
            }
            false => eprintln!("Patron Info reports not valid"),
        }
    }

    //std::thread::sleep(std::time::Duration::from_secs(7));

    // ----- Item Stuff -----

    if let Some(item_id) = options.opt_str("item-barcode") {
        params.set_item_id(&item_id);

        // Load item info
        let resp = client.item_info(&params).expect("Item Info Failed");

        match resp.ok() {
            true => {
                println!("Item Info reports valid");
                println!(
                    "Item title is '{}'",
                    resp.value("AJ").expect("Item has no title")
                );
            }
            false => eprintln!("Item Info reports not valid"),
        }
    }
}
