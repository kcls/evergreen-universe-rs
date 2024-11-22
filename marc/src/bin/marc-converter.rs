use marc::Record;
use marc::MARCXML_NAMESPACE;
use std::env;
use std::fs::File;
use std::io::Read;
use std::io::Write;

const HELP_TEXT: &str = r#"
Converts MARC records between UTF8-encoded MARC21, MARC XML, and MARC Breaker.

Synopsis:

    marc-converter --to-xml /path/to/marc-file.mrc

Options

    --to-xml
        Conver to XML

    --to-marc
        Conver to MARC binary

    --to-breaker
        Conver to Breaker text

"#;

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

    let xml_ops = marc::xml::XmlOptions {
        formatted: false, // TODO
        with_xml_declaration: false,
    };

    // Prints one record using the requested output.
    let printer = move |r: &Record| {
        if to_bin {
            let bytes = &r.to_binary().expect("Binary generation failed");
            std::io::stdout().write_all(&bytes).expect("Cannot write bytes");
        } else if to_xml {
            print!("{}", r.to_xml_ops(&xml_ops).expect("XML generation failed"));
        } else if to_breaker {
            print!("{}", r.to_breaker());
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

    match first_byte {
        b'<' => {
            // TODO bake some of this into marc::xml?
            print!(r#"<?xml version="1.0"?>"#);
            print!(r#"<collection xmlns="{MARCXML_NAMESPACE}">"#);
            for rec in Record::from_xml_file(filename).expect("XML file read filed") {
                printer(&rec.expect("XML record read failed"));
            }
            print!("</collection>");
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
}
