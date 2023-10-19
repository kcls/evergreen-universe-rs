use marc::Controlfield;
use marc::Field;
use marc::Leader;
use marc::Record;
use marc::Subfield;

// Avoiding newlines / formatting for testing purposes.
const MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>07649cim a2200913 i 4500</leader><controlfield tag="001">233</controlfield><controlfield tag="003">CONS</controlfield><controlfield tag="005">20140128084328.0</controlfield><controlfield tag="008">140128s2013    nyuopk|zqdefhi n  | ita d</controlfield><datafield tag="010" ind1=" " ind2=" "><subfield code="a">  2013565186</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">9781480328532</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">1480328537</subfield></datafield><datafield tag="024" ind1="1" ind2=" "><subfield code="a">884088883249</subfield></datafield><datafield tag="028" ind1="3" ind2="2"><subfield code="a">HL50498721</subfield><subfield code="b">Hal Leonard</subfield><subfield code="q">(bk.)</subfield></datafield></record>"#;

const EMPTY_MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>                        </leader></record>"#;

const MARC_BINARY: &str = r#"00260nz  a2200109O  450000100030000000300050000300500170000800800410002503500180006610000480008490100180013254CONS19981117195632.0970601 nbacannbabn           a ana     d  a(CONIFER)48741 aHandel, George Frideric, 1685-1759.xOperas  c54tauthority"#;

const MARK_BREAKER: &str = r#"=LDR 02675cam a2200481Ii 4500
=001 ocn953985896
=003 OCoLC
=005 20170714170059.0
=008 160724s2017\\\\flua\\\e\\\\\\000\0\spa\d
=020 \\$a9781945540042$q(paperback)
=020 \\$a1945540044$q(paperback)
=035 \\$a(OCoLC)953985896
=040 \\$aBTCTA$beng$erda$cBTCTA$dYDXCP$dBDX$dGK8$dOI6$dTXWBR$dOCLCF$dIGA$dNTG$dUtOrBLW
=049 \\$aNTGA
=082 04$a158.1$223
=092 \\$a158.1 CAL SPANISH
=100 1\$aCala, Ismael.$0(DLC)304291
=245 10$aDespierta con Cala :$binspiraciones para una vida en equilibrio /$cIsmael Cala.
=250 \\$aPrimera edición.
=264 \1$aMiami, FL :$bAguilar :$bPenguin Random House Grupo Editorial USA LLC,$c2017.
=300 \\$a333 pages :$bcolor illustrations ;$c23 cm
=336 \\$atext$btxt$2rdacontent
=337 \\$aunmediated$bn$2rdamedia
=338 \\$avolume$bnc$2rdacarrier
=546 \\$aText in Spanish = Texto en español.
=500 \\$aIncludes bibliographic references.
=520 \\$aEs hora de poner todos los aspectos de tu vida en armonía: tu mente, tu cuerpo, el amor, la familia, los amigos, las finanzas... ¡tú! Cada semana en el show Despierta América de Univision, Ismael Cala nos inspira para despertar a la vida y hallar la felicidad. Y ahora, en las páginas de "Despierta con Cala" encontrarás la motivación para equilibrar tu vida y seguir adelante, con paz y alegría. Ismael Cala te invita a que visualices tu vida como una cuerda floja en la que avanzas con los brazos abiertos, intentado hacer malabares con siete pelotas ―siete aspectos de la vida, algunos más delicados que otros―, que no puedes dejar caer... Y mucho menos puedes caer tú mismo al vacío.
=505 00$tIntroducción --$tMente y espíritu --$tSalud y cuerpo --$tAmor y relaciones de pareja --$tFamilia y hogar --$tAmigos y yo social --$tFinanzas personales --$tTiempo para ti --$tConclusiones.
=650 \0$aSelf-actualization (Psychology)$0(DLC)533061
=650 \0$aSelf-help techniques.$0(DLC)533096
=650 \0$aSuccess.$0(DLC)540413
=650 \0$aMind and body.$0(DLC)522262
=650 \7$aMind and body.$2fast$0(OCoLC)fst01021997
=650 \7$aSelf-actualization (Psychology)$2fast$0(OCoLC)fst01111481
=650 \7$aSelf-help techniques.$2fast$0(OCoLC)fst01111754
=650 \7$aSuccess.$2fast$0(OCoLC)fst01137041
=655 \7$aSelf-help publications.$2lcgft$0(DLC)680047
=655 \7$aSelf-help publications.$2fast$0(OCoLC)fst01941328
=655 \7$aSpanish language edition$vNonfiction.$2local
=915 \\$almc$d2017-05-11
=998 \\$da
=994 \\$aC0$bNTG
=901 \\$a1705072$b$c1705072$tbiblio$soclc"#;

#[test]
fn manual_record() {
    let mut record = Record::default();

    let cf = Controlfield::new(b"001", b"MyMagicNumber");
    record.insert_control_field(cf);

    let mut field = Field::new(b"245");
    field.set_ind1(b'1');

    field.add_subfield_data(&[
        (b'a', b"Harry Potter".as_slice()),
        (b'b', b"So Many Wizards".as_slice()),
    ]);

    record.insert_field(field);

    let field = record
        .matching_fields(b"245")
        .pop()
        .expect("We have a field");
    let sf = field
        .matching_subfields(b'a')
        .pop()
        .expect("We have a subfield");

    assert_eq!(sf.content(), b"Harry Potter".as_slice());
}

#[test]
fn breaker_round_trip() {
    let record = Record::from_breaker(MARK_BREAKER).expect("Parse Breaker OK");

    let field = record.matching_fields(b"998").pop().expect("Has a field");
    let sf = field
        .matching_subfields(b'd')
        .pop()
        .expect("Has a subfield");

    assert_eq!(sf.content(), b"a");

    let breaker = record.to_breaker();
    assert_eq!(MARK_BREAKER, breaker);
}

#[test]
fn xml_round_trip() {
    let record = Record::from_xml(MARC_XML)
        .next()
        .expect("Parsed an XML record");

    let xml = record.to_xml().unwrap();

    assert_eq!(MARC_XML, xml);
}

#[test]
fn xml_breaker_round_trip() {
    let record1 = Record::from_xml(MARC_XML)
        .next()
        .expect("Parsed an XML record");
    let breaker1 = record1.to_breaker();

    let record2 = Record::from_breaker(&breaker1).expect("Parsed Breaker");
    let breaker2 = record2.to_breaker();

    assert_eq!(record1, record2);

    assert_eq!(breaker1, breaker2);

    let xml = record2.to_xml().expect("Generated some XML");

    assert_eq!(MARC_XML, xml);
}

#[test]
fn binary() {
    let src_bytes = MARC_BINARY.as_bytes().to_vec();

    let record = Record::from_binary(&src_bytes).expect("Parse from binary");
    let field = record.matching_fields(b"100").pop().unwrap();
    let sf = field.matching_subfields(b'a').pop().unwrap();

    assert_eq!(
        sf.content_string().expect("utf8 content").as_str(),
        "Handel, George Frideric, 1685-1759."
    );

    let bytes = record.to_binary().expect("Create binary");

    assert_eq!(src_bytes, bytes);
}

#[test]
fn odd_records() {
    let record = Record::from_xml(EMPTY_MARC_XML).next().unwrap();

    let brk = record.to_breaker();
    assert_eq!(brk, format!("=LDR {}", Leader::default()));

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
fn set_values() {
    let v = "Hello, Mars!";

    let mut record = Record::from_xml(MARC_XML).next().unwrap();

    let breaker1 = record.to_breaker();
    let field = record.matching_fields_mut(b"028").pop().expect("has 028");
    let sf = field
        .matching_subfields_mut(b'a')
        .pop()
        .expect("Has subfield a");

    sf.set_content(v.as_bytes());
    let value = record.values(b"028", b'a').pop().expect("Has a value");

    assert_eq!(v.as_bytes(), value);

    let breaker2 = record.to_breaker();
    assert_ne!(breaker1, breaker2);
}

#[test]
fn delete_values() {
    let mut record = Record::from_xml(MARC_XML).next().unwrap();
    let field = record.matching_fields_mut(b"028").pop().unwrap();
    assert_eq!(field.subfields().len(), 3);

    field.remove_subfields(b'a');

    assert_eq!(field.subfields().len(), 2);
}

#[test]
fn delete_fields() {
    let mut record = Record::from_xml(MARC_XML).next().unwrap();

    let mut field = Field::new(b"200");
    field.add_subfield(Subfield::new(b'a', b"baz"));
    record.insert_field(field);

    let mut field = Field::new(b"200");
    field.add_subfield(Subfield::new(b'a', b"foo"));
    record.insert_field(field);

    let mut field = Field::new(b"200");
    field.add_subfield(Subfield::new(b'a', b"xxxadf"));
    record.insert_field(field);

    assert_eq!(record.matching_fields(b"200").len(), 3);

    record.remove_fields(b"200");

    assert_eq!(record.matching_fields(b"200").len(), 0);
}

#[test]
fn now_with_strings() {
    let mut record = Record::default();

    let cf = Controlfield::from_strs("001", "MyMagicNumber").unwrap();
    record.insert_control_field(cf);

    let field = Field::from_strs(
        "245",
        " ",
        "1",
        &[("a", "Harry Potter"), ("b", "So Many Wizards")],
    )
    .unwrap();

    record.insert_field(field);

    let value = record.value_strings("245", "a").unwrap().pop().unwrap();

    assert_eq!(value, "Harry Potter");

    let value_bytes = record.values(b"245", b'a').pop().expect("Has a value");

    assert_eq!(value_bytes, value.as_bytes());
}
