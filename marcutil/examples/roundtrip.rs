use getopts;
use marcutil::Record;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("x", "xml-file", "MARC XML File", "MARCXML_FILE");
    opts.optopt("b", "bin-file", "MARC Binary File", "MARC_FILE");

    let params = opts.parse(&args[1..]).expect("Options parsed");

    let xml_file_op = params.opt_str("xml-file");
    let bin_file_op = params.opt_str("bin-file");

    if let Some(filename) = bin_file_op {
        for mut record in
            Record::from_binary_file(&filename).expect("Start Binary File") {
            inspect_record(&mut record);
        }
    }

    if let Some(filename) = xml_file_op {

        let s = std::fs::read_to_string(&filename).unwrap();
        let rec = Record::from_xml(&s).next().expect("XML contains a record");

        println!("From XML String: leader={}", rec.leader);

        for mut record in
            Record::from_xml_file(&filename).expect("Created Iterator") {
            inspect_record(&mut record);
        }
    }
}

fn inspect_record(record: &mut Record) {
    if let Some(title) = record.get_values("245", "a").first() {
        println!("Maintitle => {title}");
    }

    if let Some(field) = record.get_fields_mut("245").first_mut() {
        if let Some(sf) = field.get_subfields_mut("a").first_mut() {
            sf.set_content("I Prefer This Title");
        }
    }

    if let Some(title) = record.get_values("245", "a").first() {
        println!("New Maintitle => {title}");
    }

    record
        .add_control_field("005", "123123123123")
        .expect("Added Control Field");
    record
        .add_data_field("650", "0", " ", vec!["a", "Hobbits", "b", "Fiction"])
        .expect("Added Data Field");

    let breaker = record.to_breaker();

    println!("Create {} bytes of breaker text", breaker.bytes().len());

    let binary = record.to_binary().expect("Created Binary");

    println!("Create {} bytes of binary", binary.len());
}


