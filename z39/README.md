# Types for Z39.50 ASN.1 Messages and Bib1 Attribute Set Values

* Reference: [ASN.1 Messages Types](https://www.loc.gov/z3950/agency/asn1.html)
* Reference: [Bib1 Attribute Set](https://www.loc.gov/z3950/agency/defns/bib1.html)

## Parsing a Stream of Bytes

```rs
use z39::types::pdu::*;

// Bytes from a sample InitializeRequest from YAZ client.
// https://software.indexdata.com/yaz/doc/yaz-client.html
let bytes = [
    0xb4, 0x52, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9, 0xa2, 0x85, 0x04, 0x04, 0x00,
    0x00, 0x00, 0x86, 0x04, 0x04, 0x00, 0x00, 0x00, 0x9f, 0x6e, 0x02, 0x38, 0x31, 0x9f, 0x6f,
    0x03, 0x59, 0x41, 0x5a, 0x9f, 0x70, 0x2f, 0x35, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x20, 0x63,
    0x33, 0x63, 0x65, 0x61, 0x38, 0x38, 0x31, 0x65, 0x33, 0x65, 0x37, 0x65, 0x38, 0x30, 0x62,
    0x30, 0x36, 0x39, 0x64, 0x64, 0x64, 0x31, 0x34, 0x32, 0x39, 0x39, 0x39, 0x34, 0x65, 0x35,
    0x38, 0x38, 0x34, 0x31, 0x61, 0x63, 0x62, 0x31, 0x34,
];

let msg = Message::from_bytes(&bytes)
    .expect("bytes should parse OK")
    .expect("bytes should produce a whole message");

let MessagePayload::InitializeRequest(ref payload) = msg.payload else {
    panic!("Unexpected payload: {msg:?}");
};

assert_eq!(Some("YAZ"), payload.implementation_name.as_deref());

assert_eq!(bytes, *msg.to_bytes().unwrap());
```

## Manual Message Building

```rs
use z39::types::*;
use z39::types::oid;
use z39::types::pdu::*;

let oc = OctetString::from(b"Pile of MARC Bytes".to_vec());
let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));

external.direct_reference = Some(oid::for_marc21());

let mut npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));
npr.name = Some(DatabaseName::Name("My Database".to_string()));

let records = Records::ResponseRecords(vec![npr]);

let mut pr = PresentResponse::default();
pr.records = Some(records);
pr.number_of_records_returned = 1;
pr.present_status = PresentStatus::Success;

let msg = Message::from_payload(MessagePayload::PresentResponse(pr));

println!("Created: {msg:?}");

```

## Currently Supported Message Types

The driver for this crate is a minimum viable Z30.50 server in Rust.
It's unclear if additional messages types will be added, but patches are
certainly welcome.

* InitializeRequest
* InitializeResponse
* SearchRequest
* SearchResponse
* PresentRequest
* PresentResponse
* Close






