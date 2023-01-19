use marcutil::record::DEFAULT_LEADER;
use marcutil::Record;

// Avoiding newlines / formatting for testing purposes.
const MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>07649cim a2200913 i 4500</leader><controlfield tag="001">233</controlfield><controlfield tag="003">CONS</controlfield><controlfield tag="005">20140128084328.0</controlfield><controlfield tag="008">140128s2013    nyuopk|zqdefhi n  | ita d</controlfield><datafield tag="010" ind1=" " ind2=" "><subfield code="a">  2013565186</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">9781480328532</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">1480328537</subfield></datafield><datafield tag="024" ind1="1" ind2=" "><subfield code="a">884088883249</subfield></datafield><datafield tag="028" ind1="3" ind2="2"><subfield code="a">HL50498721</subfield><subfield code="b">Hal Leonard</subfield><subfield code="q">(bk.)</subfield></datafield></record>"#;

const EMPTY_MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>                        </leader></record>"#;

const MARC_BINARY: &str = r#"00260nz  a2200109O  450000100030000000300050000300500170000800800410002503500180006610000480008490100180013254CONS19981117195632.0970601 nbacannbabn           a ana     d  a(CONIFER)48741 aHandel, George Frideric, 1685-1759.xOperas  c54tauthority"#;

#[test]
fn breaker_round_trip() {
    let record = Record::from_xml(MARC_XML).next().unwrap();

    let breaker = record.to_breaker();

    let record2 = Record::from_breaker(&breaker).unwrap();
    let breaker2 = record2.to_breaker();

    assert_eq!(breaker, breaker2);
}

#[test]
fn xml_round_trip() {
    let record = Record::from_xml(MARC_XML).next().unwrap();

    let xml = record.to_xml().unwrap();

    assert_eq!(MARC_XML, xml);
}

#[test]
fn all_round_trip() {
    let record = Record::from_xml(MARC_XML).next().unwrap();

    let breaker = record.to_breaker();

    let record2 = Record::from_breaker(&breaker).unwrap();
    let xml = record2.to_xml().unwrap();

    assert_eq!(MARC_XML, xml);
}

#[test]
fn odd_records() {
    let record = Record::from_xml(EMPTY_MARC_XML).next().unwrap();

    let brk = record.to_breaker();
    assert_eq!(brk, format!("LDR {}", DEFAULT_LEADER));

    let op = Record::from_breaker(&brk);
    assert!(op.is_ok());

    let xml_op = op.unwrap().to_xml();
    assert!(xml_op.is_ok());

    assert_eq!(EMPTY_MARC_XML, xml_op.unwrap());

    let op = Record::from_xml(r#"<record><controlfield tag="123">"#).next();
    assert!(op.is_none());

    let op = Record::from_xml(
        r#"<record><controlfield tag="1234"></controlfield></record>"#).next();

    assert!(op.is_none());
}

#[test]
fn binary() {
    let src_bytes = MARC_BINARY.as_bytes().to_vec();

    let record = Record::from_binary(&src_bytes).unwrap();

    let author = record.get_values("100", "a").pop().unwrap();

    assert_eq!(author, "Handel, George Frideric, 1685-1759.");

    let bytes = record.to_binary().unwrap();

    assert_eq!(src_bytes, bytes);
}

#[test]
fn set_values() {
    let v = "Hello, Mars!";

    let mut record = Record::from_xml(MARC_XML).next().unwrap();

    let breaker1 = record.to_breaker();
    let field = &mut record.get_fields_mut("028")[0];
    let sf = &mut field.get_subfields_mut("a")[0];

    sf.set_content(v);

    let w = record.get_values("028", "a")[0];
    assert_eq!(v, w);

    let breaker2 = record.to_breaker();
    assert_ne!(breaker1, breaker2);
}

#[test]
fn delete_values() {
    let mut record = Record::from_xml(MARC_XML).next().unwrap();
    let field = &mut record.get_fields_mut("028")[0];
    assert_eq!(field.subfields.len(), 3);

    let list = field.remove_subfields("a");

    assert_eq!(list.len(), 1);
    assert_eq!(field.subfields.len(), 2);
}
