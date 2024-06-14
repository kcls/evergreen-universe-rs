use marc::Record;

// Avoiding newlines / formatting for testing purposes.
const MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>00305cim a2200133 i 4500</leader><controlfield tag="001">233</controlfield><controlfield tag="003">CONS</controlfield><controlfield tag="005">20140128084328.0</controlfield><controlfield tag="008">140128s2013    nyuopk|zqdefhi n  | ita d</controlfield><datafield tag="010" ind1=" " ind2=" "><subfield code="a">  2013565186</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">9781480328532</subfield></datafield><datafield tag="020" ind1=" " ind2=" "><subfield code="a">1480328537</subfield></datafield><datafield tag="024" ind1="1" ind2=" "><subfield code="a">"884088883249"</subfield></datafield><datafield tag="028" ind1="3" ind2="2"><subfield code="a">HL50498721</subfield><subfield code="b">Hal Leonard</subfield><subfield code="q">(bk.)</subfield></datafield></record>"#;

const EMPTY_MARC_XML: &str = r#"<?xml version="1.0"?><record xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>                        </leader></record>"#;

const MARC_BINARY: &str = r#"00260nz  a2200109O  450000100030000000300050000300500170000800800410002503500180006610000480008490100180013254CONS19981117195632.0970601 nbacannbabn           a ana     d  a(CONIFER)48741 aHandel, George Frideric, 1685-1759.xOperas  c54tauthority"#;

const MARK_BREAKER: &str = r#"=LDR 02677cam a2200481Ii 4500
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
=245 10$aDespierta con Cala :$binspiraciones para "una vida" en equilibrio /$cIsmael Cala.
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
fn breaker_round_trip() {
    let record = Record::from_breaker(MARK_BREAKER).unwrap();
    let field = record.get_fields("998").pop().unwrap();
    let sf = field.get_subfields("d").pop().unwrap();

    assert_eq!(sf.content(), "a");

    let breaker = record.to_breaker();
    assert_eq!(MARK_BREAKER, breaker);
}

#[test]
fn mixed_breaker_round_trip() {
    let record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

    let breaker = record.to_breaker();

    let record2 = Record::from_breaker(&breaker).unwrap();
    let breaker2 = record2.to_breaker();

    assert_eq!(breaker, breaker2);
}

#[test]
fn mixed_round_trips() {
    let record1 = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");
    assert_eq!(MARC_XML, record1.to_xml().unwrap());

    let breaker1 = record1.to_breaker();

    let record2 = Record::from_breaker(&breaker1).unwrap();

    assert_eq!(MARC_XML, record2.to_xml().unwrap());
    assert_eq!(record1, record2);

    let binary1 = record2.to_binary().unwrap();
    let record3 = Record::from_binary(&binary1).unwrap();
    let binary3 = record2.to_binary().unwrap();

    assert_eq!(binary1, binary3);

    let xml = record3.to_xml().unwrap();

    assert_eq!(xml, MARC_XML);
}

#[test]
fn xml_round_trip() {
    let record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

    let xml = record.to_xml().unwrap();

    assert_eq!(MARC_XML, xml);
}

#[test]
fn all_round_trip() {
    let record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

    let breaker = record.to_breaker();

    let record2 = Record::from_breaker(&breaker).unwrap();
    let xml = record2.to_xml().unwrap();

    assert_eq!(MARC_XML, xml);
}

#[test]
fn odd_records() {
    let record = Record::from_xml(EMPTY_MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

    let brk = record.to_breaker();
    assert_eq!(brk, format!("=LDR {}", Record::new().leader()));

    let res = Record::from_breaker(&brk);
    assert!(res.is_ok());

    let xml_op = res.unwrap().to_xml();
    assert!(xml_op.is_ok());

    assert_eq!(EMPTY_MARC_XML, xml_op.unwrap());

    let res = Record::from_xml(r#"<record><controlfield tag="123">"#)
        .next()
        .unwrap();
    assert!(res.is_err());

    let res = Record::from_xml(r#"<record><controlfield tag="1234"></controlfield></record>"#)
        .next()
        .unwrap();
    assert!(res.is_err());
}

#[test]
fn binary() {
    let src_bytes = MARC_BINARY.as_bytes();

    let record = Record::from_binary(src_bytes).unwrap();

    let author = record.get_values("100", "a").pop().unwrap();

    assert_eq!(author, "Handel, George Frideric, 1685-1759.");

    let bytes = record.to_binary().unwrap();

    assert_eq!(src_bytes, bytes);
}

#[test]
fn set_values() {
    let v = "Hello, Mars!";

    let mut record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

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
    let mut record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");
    let field = &mut record.get_fields_mut("028")[0];
    assert_eq!(field.subfields().len(), 3);

    let count = field.remove_subfields("a");

    assert_eq!(count, 1);
    assert_eq!(field.subfields().len(), 2);
}

#[test]
fn delete_fields() {
    let mut record = Record::from_xml(MARC_XML)
        .next()
        .unwrap()
        .expect("Parse Failed");

    let field = record.add_data_field("200").unwrap();
    field.add_subfield("a", "baz").unwrap();

    let field = record.add_data_field("200").unwrap();
    field.add_subfield("a", "bar").unwrap();

    let field = record.add_data_field("200").unwrap();
    field.add_subfield("a", "ssdfsdfdfs").unwrap();

    assert_eq!(record.get_fields("200").len(), 3);

    record.remove_fields("200");

    assert_eq!(record.get_fields("200").len(), 0);
}
