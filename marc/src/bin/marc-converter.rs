use marc::Record;
use marc::MARCXML_NAMESPACE;
use std::env;
use std::fs::File;
use std::io::Read;
use std::io::Write;

const HELP_TEXT: &str = r#"
Converts MARC records between MARC21 UTF-8, MARC XML, and MARC Breaker.

Usage:

    marc-converter --to-xml /path/to/marc-or-xml-or-breaker.file

Synopsis:

Converts a MARC, XML, or Breaker file to MARC, XML, or Breaker output on
STDOUT.  The type of the input file is determined automatically.

Binary and XML files may contain multiple records.

XML output is wrapped in a <collection/> element.

Options:

    --to-xml
        Produce XML output.

    --to-marc
        Produce MARC UTF8 output.

    --to-breaker
        Produce Breaker output.

    --format-xml
        Format XML output with 2-space indent.

"#;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optflag("", "to-xml", "");
    opts.optflag("", "to-marc", "");
    opts.optflag("", "to-breaker", "");
    opts.optflag("", "format-xml", "");
    opts.optflag("h", "help", "");

    let params = opts.parse(&args[1..]).expect("Options should parse");

    if params.opt_present("help") {
        println!("{HELP_TEXT}");
        return;
    }

    let Some(filename) = params.free.first() else {
        eprintln!("Input file required");
        return;
    };

    let to_xml = params.opt_present("to-xml");
    let to_marc = params.opt_present("to-marc");
    let to_breaker = params.opt_present("to-breaker");
    let format_xml = params.opt_present("format-xml");

    let xml_ops = marc::xml::XmlOptions {
        formatted: format_xml,
        with_xml_declaration: false,
    };

    // Prints one record using the requested output.
    let printer = move |r: &Record| {
        if to_marc {
            let bytes = &r.to_binary().expect("Binary generation failed");
            std::io::stdout().write_all(&bytes).expect("Cannot write bytes");
        } else if to_xml {
            print!("{}", r.to_xml_ops(&xml_ops).expect("XML generation failed"));
        } else if to_breaker {
            println!("{}", r.to_breaker());
        };
    };

    // Get the first character of the file so we can determine its type.
    let mut buf: [u8; 1] = [0];
    let mut file = File::open(filename).expect("Cannot open file");

    let first_byte = if file.read(&mut buf).expect("Cannot read file") > 0 {
        buf[0]
    } else {
        eprintln!("File is empty");
        return;
    };

    // XML output has to be wrapped.
    if to_xml {
        println!(r#"<?xml version="1.0"?>"#);
        print!(r#"<collection xmlns="{MARCXML_NAMESPACE}">"#);
    }

    match first_byte {
        b'<' => {
            for rec in Record::from_xml_file(filename).expect("XML file read filed") {
                printer(&rec.expect("XML record read failed"));
            }
        }

        b'=' => printer(&Record::from_breaker_file(filename).expect("Breaker parsing failed")),

        // Binary MARC begins with the record length, i.e. number characters
        b'0'..=b'9' => {
            for rec in Record::from_binary_file(filename).expect("Binary parsing failed") {
                printer(&rec.expect("Binary record read failed"));
            }
        }
        _ => {
            eprintln!("Unable to determine file type");
        }
    };

    if to_xml {
        print!("\n</collection>");
    }
}
