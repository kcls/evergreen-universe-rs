use marctk::Record;

fn main() {
    let mut args = std::env::args();
    args.next(); // name of executable

    let Some(file_name) = args.next() else {
        eprintln!("MARC file name required");
        return;
    };

    for record in Record::from_binary_file(&file_name).expect("File should be readable") {
        let mut record = record.expect("Record should be parseable");

        if let Some(field) = record.get_fields_mut("245").first_mut() {
            if let Some(sf) = field.get_subfields_mut("a").first_mut() {
                println!("Maintitle => {}", sf.content());
                sf.set_content("I Prefer This Title");
            }
        }

        if let Some(title) = record.get_values("245", "a").first() {
            println!("New Maintitle => {title}");
        }

        let f = record.add_data_field("650").unwrap();
        f.set_ind1("0").unwrap();
        f.add_subfield("a", "Hobbitz").unwrap();
        f.add_subfield("b", "So Many Wizards").unwrap();

        println!("{}", record.to_breaker());
        // println!("{:?}", record.to_binary());
        // println!("{}", record.to_xml());
    }
}
