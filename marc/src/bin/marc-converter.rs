use marc::Record;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optflag("", "to-xml", "");
    opts.optflag("", "to-bin", "");
    opts.optflag("", "to-breaker", "");

    let params = opts.parse(&args[1..]).expect("Options should parse");

    let Some(filename) = params.free.first() else {
        eprintln!("Input file required");
        return;
    };

    let to_xml = params.opt_present("to-xml");
    let to_bin = params.opt_present("to-bin");
    let to_breaker = params.opt_present("to-breaker");

    // TODO --to-file option.

    // Prints one record using the requested output.
    let printer = move |r: &Record| {
        if to_bin {
            print!(
                "{}",
                // Convert the raw bytes to a UTF8 string so the terminal
                // can make sense of it.
                std::str::from_utf8(&r.to_binary().expect("Binary generation failed"))
                    .expect("UTF8 conversion failed")
            );
        } else if to_xml {
            print!("{}", r.to_xml().expect("XML generation failed"));
        } else if to_breaker {
            print!("{}", r.to_breaker());
        };
    };

    // Find the first character of the file so we can determine its
    // type automagically.
    let mut first_char = '\0';

    let reader = BufReader::new(File::open(filename).expect("open failed"));

    // Find the first non-whitespace character.
    for line in reader.lines() {
        for ch in line.expect("lines() failed").chars() {
            if ch != ' ' {
                first_char = ch;
                break;
            }
        }
    }

    match first_char {
        '<' => {
            for rec in Record::from_xml_file(filename).expect("XML file read filed") {
                printer(&rec.expect("XML record read failed"));
            }
        }
        '=' => printer(&Record::from_breaker_file(filename).expect("Breaker parsing failed")),

        // Binary MARC begins with the record length, i.e. number characters
        '0'..='9' => {
            for rec in Record::from_binary_file(filename).expect("Binary parsing failed") {
                printer(&rec.expect("Binary record read failed"));
            }
        }
        _ => {
            eprintln!("Unable to determine file type");
        }
    };
}
