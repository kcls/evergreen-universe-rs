# Rust MARC XML / Breaker / Binary Toolkit

Tools for parsing and generating MARC21 binary, XML, and breaker text.

## Example

```rs
use marctk::Record;

// Read a file of MARC records, modify, and print them.

for record in Record::from_binary_file(&file_name).expect("File should be readable") {
    let mut record = record.expect("Record should be parseable");

    if let Some(field) = record.get_fields_mut("245").first_mut() {
        if let Some(sf) = field.get_subfields_mut("a").first_mut() {
            println!("Maintitle => {}", sf.content());
            sf.set_content("I Prefer This Title");
        }
    }

    let f = record.add_data_field("650").unwrap();
    f.set_ind1("0").unwrap();
    f.add_subfield("a", "Hobbitz").unwrap();
    f.add_subfield("b", "So Many Wizards").unwrap();

    println!("{}", record.to_xml_string());
}
```

### Strings & Bytes

For ease of use, the API primarily traffics in &str/String's.  Byte
counts are enforced where needed, but otherwise the user can generally 
use Rust strings without concern for bytes and UTF-8 conversions.

## Requirements

1. Data must be UTF-8 compatible.
1. Indicators and subfield codes must have a byte length of 1.
1. Tags must have a byte length of 3.
1. Leaders must have a byte length of 24.
1. Binary leader/directory metadata must be sane/usable.

In cases where these conditions are not met, routines exit early with
explanatory Err()'s.
