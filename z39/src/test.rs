use crate::types::oid;
use crate::types::pdu::*;
use crate::types::*;

// Print a list of bytes as hex values.
#[allow(dead_code)]
fn hexdump(bytes: &[u8]) {
    println!(
        "\n{}\n",
        bytes
            .iter()
            .map(|b| format!("{b:#04x?}"))
            .collect::<Vec<String>>()
            .join(", ")
    );
}

#[test]
fn test_initialize_request() {
    // Example InitializeRequest from YAZ client.
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
        panic!("Unexpected type parsed: {msg:?}");
    };

    assert_eq!(Some("YAZ"), payload.implementation_name.as_deref());

    assert_eq!(bytes, *msg.to_bytes().unwrap());

    // Verify valid, partial messages return None instead of Err
    assert!(Message::from_bytes(&bytes[0..10]).unwrap().is_none());
}

#[test]
fn test_initialize_response() {
    // Note the 26h byte (a boolean value) in Yaz in 0x01, but it's
    // 0xff in rasn.  Changed here to allow the tests to pass.

    // Bytes taking from a Yaz client init request
    let bytes = [
        0xb5, 0x7f, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9, 0x82, 0x85, 0x04, 0x04, 0x00,
        0x00, 0x00, 0x86, 0x04, 0x04, 0x00, 0x00, 0x00, 0x8c, 0x01, 0xff, 0x9f, 0x6e, 0x05, 0x38,
        0x31, 0x2f, 0x38, 0x31, 0x9f, 0x6f, 0x25, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x32, 0x5a,
        0x4f, 0x4f, 0x4d, 0x20, 0x55, 0x6e, 0x69, 0x76, 0x65, 0x72, 0x73, 0x61, 0x6c, 0x20, 0x47,
        0x61, 0x74, 0x65, 0x77, 0x61, 0x79, 0x2f, 0x47, 0x46, 0x53, 0x2f, 0x59, 0x41, 0x5a, 0x9f,
        0x70, 0x34, 0x31, 0x2e, 0x30, 0x34, 0x2f, 0x35, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x20, 0x63,
        0x33, 0x63, 0x65, 0x61, 0x38, 0x38, 0x31, 0x65, 0x33, 0x65, 0x37, 0x65, 0x38, 0x30, 0x62,
        0x30, 0x36, 0x39, 0x64, 0x64, 0x64, 0x31, 0x34, 0x32, 0x39, 0x39, 0x39, 0x34, 0x65, 0x35,
        0x38, 0x38, 0x34, 0x31, 0x61, 0x63, 0x62, 0x31, 0x34,
    ];

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::InitializeResponse(ref payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    assert_eq!(
        Some("Simple2ZOOM Universal Gateway/GFS/YAZ"),
        payload.implementation_name.as_deref()
    );

    assert_eq!(bytes, *msg.to_bytes().unwrap());
}

#[test]
fn test_payloaduest() {
    // Byte 14 replaces 0x01 with 0xff for rasn-consistent boolean value
    let bytes = [
        0xb6, 0x4e, 0x8d, 0x01, 0x00, 0x8e, 0x01, 0x01, 0x8f, 0x01, 0x00, 0x90, 0x01, 0xff, 0x91,
        0x01, 0x31, 0xb2, 0x07, 0x9f, 0x69, 0x04, 0x6b, 0x63, 0x6c, 0x73, 0xb5, 0x34, 0xa1, 0x32,
        0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x13, 0x03, 0x01, 0xa0, 0x27, 0xbf, 0x66, 0x24, 0xbf,
        0x2c, 0x14, 0x30, 0x08, 0x9f, 0x78, 0x01, 0x01, 0x9f, 0x79, 0x01, 0x07, 0x30, 0x08, 0x9f,
        0x78, 0x01, 0x05, 0x9f, 0x79, 0x01, 0x01, 0x9f, 0x2d, 0x0a, 0x30, 0x38, 0x37, 0x39, 0x33,
        0x30, 0x33, 0x37, 0x32, 0x37,
    ];

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::SearchRequest(ref payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    // Example extracting the ISBN from this query via pattern syntax.
    // Included here for my own reference.
    /*
    let Query::Type1(
        RpnQuery {
            rpn: RpnStructure::Op(Operand::AttrTerm(
                AttributesPlusTerm {
                    term: Term::General(ref isbn),
                    ..
                }
            )),
            ..
        }
    ) = payload.query else {
        panic!();
    };
    */

    // Extract the ISBN from within the query one piece at a time.
    let Query::Type1(ref rpn_query) = payload.query else {
        panic!();
    };
    let RpnStructure::Op(ref operand) = rpn_query.rpn else {
        panic!();
    };
    let Operand::AttrTerm(ref term) = operand else {
        panic!();
    };
    let Term::General(ref isbn) = term.term else {
        panic!();
    };

    assert_eq!(oid::OID_ATTR_SET_BIB1, rpn_query.attribute_set);

    // Compare the bytes
    assert_eq!(*b"0879303727", **isbn);
    // OR the String
    assert_eq!("0879303727", std::str::from_utf8(&isbn.slice(..)).unwrap());

    assert_eq!(bytes, *msg.to_bytes().unwrap());
}

#[test]
fn test_search_response() {
    // Final bool changed from 0x01 to 0xff
    let bytes = [
        0xb7, 0x0c, 0x97, 0x01, 0x01, 0x98, 0x01, 0x00, 0x99, 0x01, 0x01, 0x96, 0x01, 0xff,
    ];

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::SearchResponse(ref payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    assert_eq!(payload.result_count, 1);

    assert_eq!(bytes, *msg.to_bytes().unwrap());
}

#[test]
fn test_present_request() {
    let bytes = [
        0xb8, 0x14, 0x9f, 0x1f, 0x01, 0x31, 0x9e, 0x01, 0x01, 0x9d, 0x01, 0x01, 0x9f, 0x68, 0x07,
        0x2a, 0x86, 0x48, 0xce, 0x13, 0x05, 0x0a,
    ];

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::PresentRequest(ref payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    //println!("\n{payload:?}");

    assert_eq!(
        &oid::OID_MARC21,
        payload.preferred_record_syntax.as_ref().unwrap()
    );

    assert_eq!(bytes, *msg.to_bytes().unwrap());
}

#[test]
fn test_present_response() {
    /*
    *  Breakdown of the bytes up to the MARC record.  From debugging why
    *  messages::External failed to parse.
    *
       0xb9 10111001 // tag 25
       0x80 10000000 // indefinite length
       0x98 10011000 // tag 24
       0x01 00000001 // length 1
       0x01 00000001 // value 1
       0x99 10011001 // tag 25
       0x01 00000001 // length 1
       0x02 00000010 // value 2
       0x9b 10011011 // tag 27
       0x01 00000001 // length 1
       0x00 00000000 // value 0
       0xbc 10111100 // tag 28
       0x80 10000000 // length indefinite
       0x30 00110000 // tag 16, universal, (Sequence Of)
       0x80 10000000 // length indefinite
       0x80 10000000 // tag 0 (name)
       0x00 00000000 // length 0
       0xa1 10100001 // tag 1 "record"
       0x80 10000000 // length, indefinite (of Record)
       0xa1 10100001 // tag 1 RetrievalRecord(External)
       0x80 10000000 // length, indefinite
       0x28 00101000 // tag 8, universal (External)
       0x80 10000000 // length, indefinite
       0x06 00000110 // tag 6, object identifier ("1.2.840.10003.5.10")
       0x07 00000111 // length 7
       0x2a 00101010 // data
       0x86 10000110 // data
       0x48 01001000 // data
       0xce 11001110 // data
       0x13 00010011 // data
       0x05 00000101 // data
       0x0a 00001010 // data
       0x81 10000001 // tag 1 / OctetAligned(OctetString)
       0x82 10000010 // length / long form / size 3
       0x0a 00001010 // length part 1
       0xe8          // length part 2
       0x30          // length part 3
       0x32          // MARC data 1st byte
       */

    let bytes = [
        0xb9, 0x80, 0x98, 0x01, 0x01, 0x99, 0x01, 0x02, 0x9b, 0x01, 0x00, 0xbc, 0x80, 0x30, 0x80,
        0x80, 0x00, 0xa1, 0x80, 0xa1, 0x80, 0x28, 0x80, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x13,
        0x05, 0x0a, 0x81, 0x82, 0x0a, 0xe8, 0x30, 0x32, 0x37, 0x39, 0x32, 0x6e, 0x61, 0x6d, 0x20,
        0x61, 0x32, 0x32, 0x30, 0x30, 0x33, 0x32, 0x35, 0x20, 0x61, 0x20, 0x34, 0x35, 0x30, 0x30,
        0x30, 0x30, 0x31, 0x30, 0x30, 0x30, 0x36, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x33,
        0x30, 0x30, 0x30, 0x36, 0x30, 0x30, 0x30, 0x30, 0x36, 0x30, 0x30, 0x35, 0x30, 0x30, 0x31,
        0x37, 0x30, 0x30, 0x30, 0x31, 0x32, 0x30, 0x30, 0x38, 0x30, 0x30, 0x34, 0x31, 0x30, 0x30,
        0x30, 0x32, 0x39, 0x30, 0x32, 0x30, 0x30, 0x30, 0x32, 0x33, 0x30, 0x30, 0x30, 0x37, 0x30,
        0x30, 0x33, 0x35, 0x30, 0x30, 0x32, 0x31, 0x30, 0x30, 0x30, 0x39, 0x33, 0x30, 0x34, 0x30,
        0x30, 0x30, 0x34, 0x35, 0x30, 0x30, 0x31, 0x31, 0x34, 0x30, 0x39, 0x32, 0x30, 0x30, 0x31,
        0x35, 0x30, 0x30, 0x31, 0x35, 0x39, 0x31, 0x30, 0x30, 0x30, 0x30, 0x33, 0x33, 0x30, 0x30,
        0x31, 0x37, 0x34, 0x32, 0x34, 0x35, 0x30, 0x30, 0x32, 0x38, 0x30, 0x30, 0x32, 0x30, 0x37,
        0x32, 0x35, 0x30, 0x30, 0x30, 0x32, 0x31, 0x30, 0x30, 0x32, 0x33, 0x35, 0x32, 0x36, 0x30,
        0x30, 0x30, 0x33, 0x39, 0x30, 0x30, 0x32, 0x35, 0x36, 0x33, 0x30, 0x30, 0x30, 0x30, 0x35,
        0x34, 0x30, 0x30, 0x32, 0x39, 0x35, 0x35, 0x30, 0x34, 0x30, 0x30, 0x34, 0x39, 0x30, 0x30,
        0x33, 0x34, 0x39, 0x35, 0x30, 0x35, 0x30, 0x34, 0x37, 0x34, 0x30, 0x30, 0x33, 0x39, 0x38,
        0x35, 0x30, 0x35, 0x30, 0x34, 0x38, 0x39, 0x30, 0x30, 0x38, 0x37, 0x32, 0x35, 0x30, 0x35,
        0x30, 0x31, 0x35, 0x32, 0x30, 0x31, 0x33, 0x36, 0x31, 0x35, 0x32, 0x30, 0x30, 0x37, 0x33,
        0x35, 0x30, 0x31, 0x35, 0x31, 0x33, 0x36, 0x35, 0x30, 0x30, 0x30, 0x33, 0x33, 0x30, 0x32,
        0x32, 0x34, 0x38, 0x36, 0x35, 0x30, 0x30, 0x30, 0x33, 0x31, 0x30, 0x32, 0x32, 0x38, 0x31,
        0x39, 0x30, 0x37, 0x30, 0x30, 0x33, 0x35, 0x30, 0x32, 0x33, 0x31, 0x32, 0x39, 0x33, 0x35,
        0x30, 0x30, 0x31, 0x34, 0x30, 0x32, 0x33, 0x34, 0x37, 0x39, 0x33, 0x35, 0x30, 0x30, 0x31,
        0x39, 0x30, 0x32, 0x33, 0x36, 0x31, 0x39, 0x39, 0x38, 0x30, 0x30, 0x34, 0x36, 0x30, 0x32,
        0x33, 0x38, 0x30, 0x39, 0x30, 0x31, 0x30, 0x30, 0x34, 0x30, 0x30, 0x32, 0x34, 0x32, 0x36,
        0x1e, 0x32, 0x37, 0x30, 0x32, 0x34, 0x1e, 0x57, 0x61, 0x4f, 0x4c, 0x4e, 0x1e, 0x32, 0x30,
        0x31, 0x35, 0x30, 0x32, 0x32, 0x33, 0x30, 0x34, 0x31, 0x31, 0x33, 0x36, 0x2e, 0x30, 0x1e,
        0x39, 0x35, 0x31, 0x32, 0x31, 0x33, 0x73, 0x31, 0x39, 0x39, 0x35, 0x20, 0x20, 0x20, 0x20,
        0x63, 0x61, 0x75, 0x61, 0x20, 0x20, 0x20, 0x65, 0x20, 0x6b, 0x20, 0x20, 0x20, 0x20, 0x30,
        0x30, 0x31, 0x20, 0x30, 0x20, 0x65, 0x6e, 0x67, 0x20, 0x64, 0x1e, 0x20, 0x20, 0x1f, 0x61,
        0x30, 0x38, 0x37, 0x39, 0x33, 0x30, 0x33, 0x37, 0x32, 0x37, 0x1f, 0x63, 0x24, 0x33, 0x35,
        0x2e, 0x30, 0x30, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x28, 0x4f, 0x43, 0x6f, 0x4c, 0x43, 0x29,
        0x34, 0x32, 0x35, 0x39, 0x36, 0x33, 0x32, 0x38, 0x30, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x57,
        0x61, 0x53, 0x1f, 0x63, 0x57, 0x61, 0x53, 0x1f, 0x64, 0x4f, 0x72, 0x50, 0x73, 0x73, 0x1f,
        0x64, 0x4f, 0x72, 0x4c, 0x6f, 0x42, 0x2d, 0x42, 0x1f, 0x64, 0x57, 0x61, 0x4f, 0x4c, 0x4e,
        0x1f, 0x64, 0x55, 0x74, 0x4f, 0x72, 0x42, 0x4c, 0x57, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x37,
        0x38, 0x36, 0x2e, 0x32, 0x31, 0x20, 0x43, 0x52, 0x4f, 0x1e, 0x31, 0x20, 0x1f, 0x61, 0x43,
        0x72, 0x6f, 0x6d, 0x62, 0x69, 0x65, 0x2c, 0x20, 0x44, 0x61, 0x76, 0x69, 0x64, 0x2e, 0x1f,
        0x30, 0x28, 0x44, 0x4c, 0x43, 0x29, 0x31, 0x30, 0x37, 0x39, 0x33, 0x35, 0x1e, 0x31, 0x30,
        0x1f, 0x61, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2f, 0x1f, 0x63, 0x44, 0x61, 0x76, 0x69,
        0x64, 0x20, 0x43, 0x72, 0x6f, 0x6d, 0x62, 0x69, 0x65, 0x2e, 0x1e, 0x20, 0x20, 0x1f, 0x61,
        0x31, 0x73, 0x74, 0x20, 0x41, 0x6d, 0x65, 0x72, 0x69, 0x63, 0x61, 0x6e, 0x20, 0x65, 0x64,
        0x2e, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x53, 0x61, 0x6e, 0x20, 0x46, 0x72, 0x61, 0x6e, 0x63,
        0x69, 0x73, 0x63, 0x6f, 0x20, 0x3a, 0x1f, 0x62, 0x47, 0x50, 0x49, 0x20, 0x42, 0x6f, 0x6f,
        0x6b, 0x73, 0x2c, 0x1f, 0x63, 0x31, 0x39, 0x39, 0x35, 0x2e, 0x1e, 0x20, 0x20, 0x1f, 0x61,
        0x31, 0x31, 0x32, 0x20, 0x70, 0x2e, 0x20, 0x28, 0x73, 0x6f, 0x6d, 0x65, 0x20, 0x66, 0x6f,
        0x6c, 0x64, 0x65, 0x64, 0x29, 0x20, 0x3a, 0x1f, 0x62, 0x69, 0x6c, 0x6c, 0x2e, 0x20, 0x28,
        0x73, 0x6f, 0x6d, 0x65, 0x20, 0x63, 0x6f, 0x6c, 0x2e, 0x29, 0x20, 0x3b, 0x1f, 0x63, 0x33,
        0x33, 0x20, 0x63, 0x6d, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x49, 0x6e, 0x63, 0x6c, 0x75, 0x64,
        0x65, 0x73, 0x20, 0x64, 0x69, 0x73, 0x63, 0x6f, 0x67, 0x72, 0x61, 0x70, 0x68, 0x79, 0x20,
        0x28, 0x70, 0x2e, 0x20, 0x31, 0x30, 0x34, 0x2d, 0x31, 0x30, 0x35, 0x29, 0x20, 0x61, 0x6e,
        0x64, 0x20, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x1e, 0x30, 0x30, 0x1f, 0x74, 0x45, 0x61,
        0x72, 0x6c, 0x79, 0x20, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x65, 0x64, 0x20, 0x49, 0x6e,
        0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x49,
        0x6e, 0x74, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20,
        0x74, 0x68, 0x65, 0x20, 0x4b, 0x65, 0x79, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x43, 0x6c,
        0x61, 0x76, 0x69, 0x63, 0x68, 0x6f, 0x72, 0x64, 0x73, 0x2c, 0x20, 0x53, 0x70, 0x69, 0x6e,
        0x65, 0x74, 0x73, 0x2c, 0x20, 0x56, 0x69, 0x72, 0x67, 0x69, 0x6e, 0x61, 0x6c, 0x73, 0x20,
        0x61, 0x6e, 0x64, 0x20, 0x48, 0x61, 0x72, 0x70, 0x73, 0x69, 0x63, 0x68, 0x6f, 0x72, 0x64,
        0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x43, 0x72, 0x69, 0x73, 0x74, 0x6f, 0x66, 0x6f, 0x72,
        0x69, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x53, 0x65, 0x63, 0x6f, 0x6e,
        0x64, 0x20, 0x57, 0x61, 0x76, 0x65, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20,
        0x52, 0x69, 0x73, 0x65, 0x20, 0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x53, 0x71, 0x75,
        0x61, 0x72, 0x65, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54,
        0x68, 0x65, 0x20, 0x46, 0x61, 0x6c, 0x6c, 0x20, 0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20,
        0x53, 0x71, 0x75, 0x61, 0x72, 0x65, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d,
        0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x45, 0x61, 0x72, 0x6c, 0x79, 0x20, 0x50, 0x6f, 0x72,
        0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f,
        0x74, 0x54, 0x68, 0x65, 0x20, 0x45, 0x61, 0x72, 0x6c, 0x79, 0x20, 0x56, 0x69, 0x65, 0x6e,
        0x6e, 0x65, 0x73, 0x65, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74,
        0x54, 0x68, 0x65, 0x20, 0x45, 0x76, 0x6f, 0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f,
        0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x56, 0x69, 0x65, 0x6e, 0x6e, 0x65, 0x73, 0x65, 0x20,
        0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x47,
        0x72, 0x61, 0x6e, 0x64, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x69, 0x6e, 0x20, 0x45,
        0x6e, 0x67, 0x6c, 0x61, 0x6e, 0x64, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20,
        0x45, 0x76, 0x6f, 0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x74, 0x68,
        0x65, 0x20, 0x47, 0x72, 0x61, 0x6e, 0x64, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d,
        0x2d, 0x1f, 0x74, 0x46, 0x72, 0x6f, 0x6d, 0x20, 0x53, 0x74, 0x72, 0x65, 0x6e, 0x67, 0x74,
        0x68, 0x20, 0x74, 0x6f, 0x20, 0x53, 0x74, 0x72, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x20, 0x2d,
        0x2d, 0x1f, 0x74, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x73, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x50,
        0x65, 0x6f, 0x70, 0x6c, 0x65, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x55, 0x70, 0x72, 0x69, 0x67,
        0x68, 0x74, 0x20, 0x47, 0x72, 0x61, 0x6e, 0x64, 0x73, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x55,
        0x70, 0x72, 0x69, 0x67, 0x68, 0x74, 0x20, 0x53, 0x71, 0x75, 0x61, 0x72, 0x65, 0x73, 0x20,
        0x2d, 0x2d, 0x1e, 0x38, 0x30, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x45, 0x61, 0x72, 0x6c,
        0x79, 0x20, 0x55, 0x70, 0x72, 0x69, 0x67, 0x68, 0x74, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f,
        0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x45, 0x76, 0x6f, 0x6c, 0x75, 0x74,
        0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x55, 0x70, 0x72, 0x69,
        0x67, 0x68, 0x74, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54,
        0x68, 0x65, 0x20, 0x47, 0x72, 0x65, 0x61, 0x74, 0x20, 0x45, 0x78, 0x68, 0x69, 0x62, 0x69,
        0x74, 0x69, 0x6f, 0x6e, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x41, 0x20, 0x50, 0x65, 0x72,
        0x69, 0x6f, 0x64, 0x20, 0x6f, 0x66, 0x20, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64,
        0x61, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x56, 0x61, 0x72, 0x69, 0x61,
        0x74, 0x69, 0x6f, 0x6e, 0x73, 0x20, 0x6f, 0x6e, 0x20, 0x61, 0x20, 0x54, 0x68, 0x65, 0x6d,
        0x65, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x42, 0x61, 0x72, 0x72, 0x65,
        0x6c, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65,
        0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x50, 0x6c, 0x61, 0x79, 0x65, 0x72, 0x20, 0x61,
        0x6e, 0x64, 0x20, 0x50, 0x6c, 0x61, 0x79, 0x65, 0x72, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f,
        0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x52, 0x65, 0x70, 0x72, 0x6f, 0x64,
        0x75, 0x63, 0x69, 0x6e, 0x67, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x61, 0x6e, 0x64,
        0x20, 0x4f, 0x74, 0x68, 0x65, 0x72, 0x20, 0x41, 0x75, 0x74, 0x6f, 0x6d, 0x61, 0x74, 0x69,
        0x63, 0x20, 0x49, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x20, 0x2d,
        0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x4d, 0x6f, 0x64, 0x65, 0x72, 0x6e, 0x20, 0x47,
        0x72, 0x61, 0x6e, 0x64, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74,
        0x54, 0x68, 0x65, 0x20, 0x41, 0x72, 0x74, 0x2d, 0x43, 0x61, 0x73, 0x65, 0x20, 0x50, 0x69,
        0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x41, 0x72, 0x74,
        0x2d, 0x43, 0x61, 0x73, 0x65, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x3a, 0x20, 0x49, 0x6e,
        0x74, 0x6f, 0x20, 0x74, 0x68, 0x65, 0x20, 0x4d, 0x6f, 0x64, 0x65, 0x72, 0x6e, 0x20, 0x45,
        0x72, 0x61, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x4b, 0x65, 0x79, 0x62, 0x6f, 0x61, 0x72, 0x64,
        0x20, 0x56, 0x61, 0x72, 0x69, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x20, 0x2d, 0x2d, 0x1f,
        0x74, 0x41, 0x6c, 0x6c, 0x20, 0x53, 0x68, 0x61, 0x70, 0x65, 0x73, 0x20, 0x61, 0x6e, 0x64,
        0x20, 0x53, 0x69, 0x7a, 0x65, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x4f, 0x6e, 0x20, 0x74,
        0x68, 0x65, 0x20, 0x4d, 0x6f, 0x76, 0x65, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65,
        0x20, 0x45, 0x61, 0x72, 0x6c, 0x79, 0x20, 0x45, 0x6c, 0x65, 0x63, 0x74, 0x72, 0x69, 0x63,
        0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x46, 0x72, 0x6f, 0x6d,
        0x20, 0x45, 0x6c, 0x65, 0x63, 0x74, 0x72, 0x69, 0x63, 0x20, 0x74, 0x6f, 0x20, 0x45, 0x6c,
        0x65, 0x63, 0x74, 0x72, 0x6f, 0x6e, 0x69, 0x63, 0x20, 0x2d, 0x2d, 0x1e, 0x38, 0x30, 0x1f,
        0x74, 0x52, 0x65, 0x63, 0x65, 0x6e, 0x74, 0x20, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x20, 0x2d,
        0x2d, 0x1f, 0x74, 0x54, 0x68, 0x65, 0x20, 0x4d, 0x6f, 0x64, 0x65, 0x72, 0x6e, 0x20, 0x55,
        0x70, 0x72, 0x69, 0x67, 0x68, 0x74, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x2d, 0x2d,
        0x1f, 0x74, 0x32, 0x30, 0x74, 0x68, 0x20, 0x43, 0x65, 0x6e, 0x74, 0x75, 0x72, 0x79, 0x20,
        0x49, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x20, 0x6f, 0x66, 0x20,
        0x53, 0x69, 0x67, 0x6e, 0x69, 0x66, 0x69, 0x63, 0x61, 0x6e, 0x63, 0x65, 0x20, 0x2d, 0x2d,
        0x1f, 0x74, 0x48, 0x6f, 0x77, 0x20, 0x61, 0x20, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x57,
        0x6f, 0x72, 0x6b, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20,
        0x48, 0x6f, 0x75, 0x73, 0x65, 0x73, 0x20, 0x2d, 0x2d, 0x1f, 0x74, 0x50, 0x69, 0x61, 0x6e,
        0x6f, 0x73, 0x20, 0x4f, 0x6e, 0x20, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2e, 0x1e, 0x30,
        0x20, 0x1f, 0x61, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x74, 0x72, 0x61, 0x63, 0x65, 0x73,
        0x20, 0x74, 0x68, 0x65, 0x20, 0x65, 0x76, 0x6f, 0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x20,
        0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x69, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65,
        0x6e, 0x74, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x69, 0x74, 0x73, 0x20, 0x62, 0x69, 0x72, 0x74,
        0x68, 0x20, 0x69, 0x6e, 0x20, 0x74, 0x68, 0x65, 0x20, 0x31, 0x38, 0x74, 0x68, 0x20, 0x63,
        0x65, 0x6e, 0x74, 0x75, 0x72, 0x79, 0x2c, 0x20, 0x73, 0x68, 0x6f, 0x77, 0x69, 0x6e, 0x67,
        0x20, 0x61, 0x6c, 0x6c, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6d, 0x61, 0x69, 0x6e, 0x20, 0x76,
        0x61, 0x72, 0x69, 0x65, 0x74, 0x69, 0x65, 0x73, 0x20, 0x6f, 0x66, 0x20, 0x65, 0x61, 0x72,
        0x6c, 0x79, 0x20, 0x70, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x74,
        0x68, 0x65, 0x20, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x6e, 0x65, 0x6e, 0x74, 0x20, 0x6d, 0x61,
        0x6e, 0x75, 0x66, 0x61, 0x63, 0x74, 0x75, 0x72, 0x69, 0x6e, 0x67, 0x20, 0x68, 0x6f, 0x75,
        0x73, 0x65, 0x73, 0x2e, 0x20, 0x41, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c,
        0x20, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x2d, 0x75, 0x70, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x69,
        0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x20, 0x70, 0x68, 0x6f, 0x74, 0x6f, 0x67, 0x72,
        0x61, 0x70, 0x68, 0x79, 0x20, 0x72, 0x65, 0x76, 0x65, 0x61, 0x6c, 0x73, 0x20, 0x74, 0x68,
        0x65, 0x20, 0x64, 0x65, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x20,
        0x69, 0x6e, 0x20, 0x63, 0x6f, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e,
        0x20, 0x61, 0x6e, 0x64, 0x20, 0x27, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x27, 0x2c,
        0x20, 0x6f, 0x72, 0x20, 0x6d, 0x65, 0x63, 0x68, 0x61, 0x6e, 0x69, 0x73, 0x6d, 0x73, 0x2c,
        0x20, 0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x73, 0x65, 0x20, 0x6d, 0x61, 0x72, 0x76, 0x65,
        0x6c, 0x6f, 0x75, 0x73, 0x20, 0x61, 0x6e, 0x74, 0x69, 0x71, 0x75, 0x69, 0x74, 0x69, 0x65,
        0x73, 0x2e, 0x20, 0x4d, 0x6f, 0x72, 0x65, 0x20, 0x73, 0x70, 0x65, 0x63, 0x69, 0x61, 0x6c,
        0x6c, 0x79, 0x20, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64,
        0x20, 0x70, 0x68, 0x6f, 0x74, 0x6f, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x20, 0x69, 0x6c,
        0x6c, 0x75, 0x73, 0x74, 0x72, 0x61, 0x74, 0x65, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6f, 0x75,
        0x74, 0x65, 0x72, 0x20, 0x62, 0x65, 0x61, 0x75, 0x74, 0x79, 0x20, 0x61, 0x6e, 0x64, 0x20,
        0x69, 0x6e, 0x6e, 0x65, 0x72, 0x20, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x78, 0x69, 0x74,
        0x79, 0x20, 0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6d, 0x6f, 0x64, 0x65, 0x72, 0x6e,
        0x20, 0x70, 0x69, 0x61, 0x6e, 0x6f, 0x2c, 0x20, 0x77, 0x69, 0x74, 0x68, 0x20, 0x73, 0x70,
        0x65, 0x63, 0x74, 0x61, 0x63, 0x75, 0x6c, 0x61, 0x72, 0x20, 0x73, 0x68, 0x6f, 0x74, 0x73,
        0x20, 0x6f, 0x66, 0x20, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x20, 0x53, 0x74, 0x65,
        0x69, 0x6e, 0x77, 0x61, 0x79, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x42, 0x6f, 0x73, 0x65, 0x6e,
        0x64, 0x6f, 0x72, 0x66, 0x65, 0x72, 0x20, 0x69, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65,
        0x6e, 0x74, 0x73, 0x2c, 0x20, 0x77, 0x68, 0x69, 0x6c, 0x65, 0x20, 0x6e, 0x65, 0x77, 0x20,
        0x74, 0x65, 0x63, 0x68, 0x6e, 0x6f, 0x6c, 0x6f, 0x67, 0x79, 0x20, 0x65, 0x78, 0x61, 0x6d,
        0x69, 0x6e, 0x65, 0x64, 0x20, 0x69, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x73, 0x20, 0x74,
        0x68, 0x65, 0x20, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x72, 0x69, 0x63, 0x20, 0x70, 0x69, 0x61,
        0x6e, 0x6f, 0x20, 0x61, 0x73, 0x20, 0x77, 0x65, 0x6c, 0x6c, 0x20, 0x61, 0x73, 0x20, 0x6d,
        0x6f, 0x72, 0x65, 0x20, 0x72, 0x65, 0x63, 0x65, 0x6e, 0x74, 0x20, 0x64, 0x65, 0x76, 0x65,
        0x6c, 0x6f, 0x70, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x20, 0x73, 0x75, 0x63, 0x68, 0x20, 0x61,
        0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x64, 0x69, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x20, 0x70,
        0x69, 0x61, 0x6e, 0x6f, 0x2e, 0x20, 0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x6d, 0x70, 0x6f,
        0x72, 0x74, 0x61, 0x6e, 0x74, 0x20, 0x6e, 0x65, 0x77, 0x20, 0x62, 0x6f, 0x6f, 0x6b, 0x20,
        0x69, 0x73, 0x20, 0x61, 0x6e, 0x20, 0x65, 0x73, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c,
        0x20, 0x70, 0x75, 0x72, 0x63, 0x68, 0x61, 0x73, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x65,
        0x76, 0x65, 0x72, 0x79, 0x20, 0x70, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x70, 0x6c, 0x61, 0x79,
        0x65, 0x72, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x65, 0x76, 0x65, 0x72, 0x79, 0x20, 0x6d, 0x75,
        0x73, 0x69, 0x63, 0x61, 0x6c, 0x20, 0x69, 0x6e, 0x73, 0x74, 0x72, 0x75, 0x6d, 0x65, 0x6e,
        0x74, 0x20, 0x65, 0x6e, 0x74, 0x68, 0x75, 0x73, 0x69, 0x61, 0x73, 0x74, 0x2e, 0x1e, 0x20,
        0x30, 0x1f, 0x61, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x1f, 0x78, 0x48, 0x69, 0x73, 0x74, 0x6f,
        0x72, 0x79, 0x2e, 0x1f, 0x30, 0x28, 0x44, 0x4c, 0x43, 0x29, 0x35, 0x33, 0x36, 0x39, 0x39,
        0x33, 0x1e, 0x20, 0x30, 0x1f, 0x61, 0x50, 0x69, 0x61, 0x6e, 0x6f, 0x20, 0x6d, 0x61, 0x6b,
        0x65, 0x72, 0x73, 0x2e, 0x1f, 0x30, 0x28, 0x44, 0x4c, 0x43, 0x29, 0x35, 0x33, 0x37, 0x30,
        0x31, 0x35, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x2e, 0x62, 0x31, 0x30, 0x34, 0x34, 0x37, 0x34,
        0x30, 0x34, 0x1f, 0x62, 0x30, 0x36, 0x2d, 0x32, 0x31, 0x2d, 0x31, 0x30, 0x1f, 0x63, 0x30,
        0x39, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x34, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x28, 0x29, 0x31,
        0x36, 0x35, 0x36, 0x35, 0x30, 0x34, 0x1e, 0x20, 0x20, 0x1f, 0x61, 0x28, 0x57, 0x61, 0x4f,
        0x4c, 0x4e, 0x29, 0x39, 0x39, 0x36, 0x37, 0x33, 0x30, 0x31, 0x1e, 0x20, 0x20, 0x1f, 0x61,
        0x63, 0x73, 0x1f, 0x61, 0x6b, 0x6d, 0x1f, 0x62, 0x30, 0x39, 0x2d, 0x30, 0x31, 0x2d, 0x30,
        0x34, 0x1f, 0x63, 0x6d, 0x1f, 0x64, 0x61, 0x1f, 0x65, 0x2d, 0x1f, 0x66, 0x65, 0x6e, 0x67,
        0x1f, 0x67, 0x63, 0x61, 0x75, 0x1f, 0x68, 0x30, 0x1f, 0x69, 0x32, 0x1e, 0x20, 0x20, 0x1f,
        0x61, 0x32, 0x37, 0x30, 0x32, 0x34, 0x1f, 0x62, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e,
        0x1f, 0x63, 0x32, 0x37, 0x30, 0x32, 0x34, 0x1f, 0x74, 0x62, 0x69, 0x62, 0x6c, 0x69, 0x6f,
        0x1f, 0x73, 0x6f, 0x63, 0x6c, 0x63, 0x1e, 0x1d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::PresentResponse(ref _payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    let rec_bytes = msg.to_bytes().unwrap();

    let msg2 = Message::from_bytes(&rec_bytes).unwrap().unwrap();

    assert_eq!(msg, msg2);

    // We cannot compare bytes directly because the source bytes use
    // indefinite-length encoding whereas the rasn-generated bytes use
    // know length values.  This has a noticeable cascade effect on the
    // generated bytes, making the output quite different.
    //assert_eq!(bytes, *msg.to_bytes().unwrap());

    // Build a PresentResponse message manually.
    let oc = OctetString::from(b"Pile of MARC Bytes".to_vec());
    let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));

    external.direct_reference = Some(oid::for_marc21());

    let mut npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));
    npr.name = Some(DatabaseName::Name("YYYYY".to_string()));

    let records = Records::ResponseRecords(vec![npr]);

    let mut pr = PresentResponse::default();
    pr.records = Some(records);
    pr.number_of_records_returned = 1;
    pr.present_status = PresentStatus::Partial1;

    let m2 = Message::from_payload(MessagePayload::PresentResponse(pr));

    // Round-trip to bytes and back to a Message.
    let bytes2 = m2.to_bytes().unwrap();

    let m3 = Message::from_bytes(&bytes2).unwrap().unwrap();

    assert_eq!(m2, m3);

    // We can compare bytes from
    assert_eq!(bytes2, m3.to_bytes().unwrap());

    println!("{}", oid::for_marcxml().to_string());
}

#[test]
fn test_close() {
    let mut close = Close::default();

    close.close_reason = CloseReason::ProtocolError;
    // who doesn't want reports as MARC?
    close.resource_report_format =
        Some(rasn::types::ObjectIdentifier::new(&oid::OID_MARC21).unwrap());

    let msg = Message::from_payload(MessagePayload::Close(close));

    let bytes = msg.to_bytes().unwrap();

    let msg = Message::from_bytes(&bytes).unwrap().unwrap();

    let MessagePayload::Close(ref payload) = msg.payload else {
        panic!("Wrong message type parsed: {msg:?}");
    };

    assert_eq!(
        &oid::OID_MARC21,
        payload.resource_report_format.as_ref().unwrap()
    );

    assert_eq!(bytes, *msg.to_bytes().unwrap());
}
