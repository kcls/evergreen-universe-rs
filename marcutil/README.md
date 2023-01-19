# Rust MARC XML / Breaker / Binary Library

## Synopsis

```rs
use marcutil::Record;

// Read a MARC binary file
for rec in Record::from_binary_file("/path/to/records.mrc").unwrap() {
    println!("\nBinary record as xml:\n{}", rec.to_xml_formatted().unwrap());
}

// Read a MARC XML file
let record = Record::from_xml("/path/to/records.xml").next().unwrap();

if let Some(title) = record.get_values("245", "a").first() {
    println!("Maintitle => {title}");
}

// Modify a field value
if let Some(field) = record.get_fields_mut("245").first_mut() {
    if let Some(sf) = field.get_subfields_mut("a").first_mut() {
        sf.set_content("I Prefer This Title");
    }
}

// Confirm we changed the value
if let Some(title) = record.get_values("245", "a").first() {
    println!("New Maintitle => {title}");
}

// Add some fields
record.add_control_field("005", "123123123123").unwrap();

record.add_data_field(
    "650", "1", " ", vec!["a", "Hobbits", "b", "Fiction"]).unwrap();

// Turn the record into Breaker text
let breaker = record.to_breaker();

println!("Breaker: {breaker}");

// Create a new record from previous record's breaker
let record2 = Record::from_breaker(&breaker).unwrap();

// Generate XML from our new record
let xml = record2.to_xml().unwrap();

println!("Generated XML: {xml}");
```

## About

MARC Library for translating to/from MARC XML, MARC Breaker, and Binary MARC.

## Data Validation

Minimal requirements are placed on the validity and format of the data.

1. Data must be UTF-8 compatible.
1. Indicators and subfield codes must have a byte length of 1.
1. Tags must have a byte length of 3.
1. Leaders must have a byte length of 24.
1. Control fields and data fields must have a tag.
1. Binary leader/directory metadata must be sane.

In cases where these conditions are not met, routines exit early with
explanatory Err() strings.

Otherwise, no restrictions are placed on the data values.
