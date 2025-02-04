use sip2::*;
use std::env;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

const DEFAULT_HOST: &str = "localhost:6001";

const HELP_TEXT: &str = r#"
Send messages to a SIP server and print info to STDOUT.

Synopsis:

sip2-client-cli --sip-user sip-user --sip-pass sip-pass \
    --item-barcode 30000017113634                       \
    --patron-barcode 394902                             \
    --message-type item-information                     \
    --message-type patron-status                        \
    --message-type patron-information

Parameters:

    --sip-host <host:port> [default="localhost:6001"]
    --sip-user <username>
    --sip-pass <password>

    --parallel <count>
        Number of parallel threads to run with each thread running
        the requested messages.

    --repeat <count>
        Repeat all requested messages this many times.

    --sleep <milliseconds>
        Sleep this long between requests to similulate slower traffic.

        Sleeps occur after each request minus the opening Login and
        SC Status requests.

    --print-raw-messages
        Include the unformatted SIP message responses in the output.

    --quiet
        Print only summary information

Message Parameters:
    --institution <institution>
    --patron-barcode <barcode>
    --patron-pass <password>
    --item-barcode <barcode>
    --pay-amount <amount>

    --fee-id <id>
        Transaction ID within the ILS.

    --transaction-id <id>
        Transaction ID provided by the payer.

    --message-type <mtype> [Repeatable]

        Specify which messages to send to the SIP server.

        Options include:
            * item-information
            * patron-status
            * patron-information
            * checkout
            * checkin
            * fee-paid

    --patron-summary-index
        Specify an index within the Patron Info "summary" section
        to set to "Y".  E.g. setting position 0 means including
        patron hold items in the Patron Info Response

"#;

#[rustfmt::skip]
fn main() {
    let options = read_options();

    if options.opt_present("help") {
        println!("{HELP_TEXT}");
        return;
    }

    let sip_params = setup_params(&options);

    let host = options
        .opt_str("sip-host")
        .unwrap_or(DEFAULT_HOST.to_string());

    let quiet = options.opt_present("quiet");
    let print_raw = options.opt_present("print-raw-messages");
    let repeat = options.opt_get_default("repeat", 1).expect("Valid Repeat Option");
    let parallel = options.opt_get_default("parallel", 1).expect("Valid Parallel Option");
    let messages = Arc::new(options.opt_strs("message-type"));
    let sleep = options.opt_get_default("sleep", 0).expect("Valid Sleep Option");

    let mut handles = Vec::new();

    let start = SystemTime::now();

    for _ in 0..parallel {
        let h = host.clone();
        let m = messages.clone();
        let p = sip_params.clone();
        handles.push(thread::spawn(move || run_one_thread(h, m, p, quiet, print_raw, repeat, sleep)));
    }

    for h in handles {
        h.join().unwrap();
    }

    let duration = start.elapsed().unwrap().as_millis();
    let seconds = (duration as f64) / 1000.0;
    let count = parallel * repeat * messages.len();
    let thput = count as f64 / seconds;

    println!("{count} messages processed in {seconds:.3} seconds; ~{thput:.3} reqs / second");
}

fn run_one_thread(
    host: String,
    messages: Arc<Vec<String>>,
    sip_params: ParamSet,
    quiet: bool,
    print_raw: bool,
    repeat: usize,
    sleep: u64,
) {
    // Connect to the SIP server
    let mut client = Client::new(&host).expect("Cannot Connect");

    // Login to the SIP server
    match client.login(&sip_params).expect("Login Error").ok() {
        true => println!("Login OK"),
        false => {
            eprintln!("Login Failed");
            return;
        }
    }

    // Check the SIP server status
    match client.sc_status().expect("SC Status Error").ok() {
        true => println!("SC Status OK"),
        false => eprintln!("SC Status Says Offline"),
    }

    // Send the requested messages, the requested number of times.
    for _ in 0..repeat {
        for message in messages.iter() {
            let start = SystemTime::now();

            let mut resp = match message.as_str() {
                "item-information" => client.item_info(&sip_params).expect("Item Info Requested"),

                "patron-status" => client
                    .patron_status(&sip_params)
                    .expect("Patron Status Requested"),

                "patron-information" => client
                    .patron_info(&sip_params)
                    .expect("Patron Info Requested"),

                "checkout" => client.checkout(&sip_params).expect("Checkout Requested"),

                "checkin" => client.checkin(&sip_params).expect("Checkin Requested"),

                "fee-paid" => client.fee_paid(&sip_params).expect("Fee Paid Requested"),

                _ => panic!("Unsupported message type: {}", message),
            };

            if print_raw {
                println!("\n{}\n", resp.msg().to_sip());
            }

            // Sort the message fields for consistent output.
            resp.msg_mut()
                .fields_mut()
                .sort_by(|a, b| a.code().cmp(b.code()));

            // Translate duration micros to millis w/ 3 decimal places.
            let duration = start.elapsed().unwrap().as_micros();
            let millis = (duration as f64) / 1000.0;
            let ms = format!("{:0>7}", format!("{:.3}", millis));

            if quiet {
                println!("{:.<35} {} ms", resp.msg().spec().label, ms);
            } else {
                println!("{}{} ms\n", resp.msg(), ms);
            }

            if sleep > 0 {
                std::thread::sleep(std::time::Duration::from_millis(sleep));
            }
        }
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
    opts.optopt("", "repeat", "Repeat Count", "");
    opts.optopt("", "sleep", "Sleep Time (millis)", "");
    opts.optopt("", "parallel", "Parallel Count", "");
    opts.optopt("", "patron-summary-index", "", "");

    opts.optopt("", "pay-amount", "Fee Paid Amount", "");
    opts.optopt("", "transaction-id", "Fee Paid Transaction ID", "");
    opts.optopt("", "fee-id", "Fee Paid Fee ID", "");

    opts.optflag("h", "help", "");
    opts.optflag("q", "quiet", "");
    opts.optflag("", "print-raw-messages", "");

    opts.optmulti("", "message-type", "Message Type", "");

    opts.parse(&args[1..]) // skip the command name
        .expect("Error parsing command line options")
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

    if let Some(ref amount) = options.opt_str("pay-amount") {
        params.set_pay_amount(amount);
    }

    if let Some(ref id) = options.opt_str("fee-id") {
        params.set_fee_id(id);
    }

    if let Some(ref id) = options.opt_str("transaction-id") {
        params.set_transaction_id(id);
    }

    if let Some(ref index) = options.opt_str("patron-summary-index") {
        params.set_summary(
            index
                .parse::<usize>()
                .expect("Summary index should be a usize"),
        );
    }

    params
}
