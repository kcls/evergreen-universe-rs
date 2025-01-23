use rasn::ber::de::DecodeErrorKind;
use rasn::prelude::*;
use rasn::AsnType;

/// Message/Record size values copied from Yaz.
const PREF_VALUE_SIZE: u32 = 67108864;

const IMPLEMENTATION_ID: &str = "EG";
const IMPLEMENTATION_NAME: &str = "Evergreen Z39";
const IMPLEMENTATION_VERSION: &str = "0.1.0";

#[derive(Debug, Default, AsnType, Decode, Encode)]
#[rasn(tag(context, 20))]
pub struct InitializeRequest {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(3))]
    pub protocol_version: BitString,
    #[rasn(tag(4))]
    pub options: BitString,
    #[rasn(tag(5))]
    pub preferred_message_size: u32,
    #[rasn(tag(6))]
    pub exceptional_record_size: u32,
    #[rasn(tag(110))]
    pub implementation_id: Option<String>,
    #[rasn(tag(111))]
    pub implementation_name: Option<String>,
    #[rasn(tag(112))]
    pub implementation_version: Option<String>,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 21))]
pub struct InitializeResponse {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(3))]
    pub protocol_version: BitString,
    #[rasn(tag(4))]
    pub options: BitString,
    #[rasn(tag(5))]
    pub preferred_message_size: u32,
    #[rasn(tag(6))]
    pub exceptional_record_size: u32,
    #[rasn(tag(12))]
    pub result: Option<bool>,
    #[rasn(tag(110))]
    pub implementation_id: Option<String>,
    #[rasn(tag(111))]
    pub implementation_name: Option<String>,
    #[rasn(tag(112))]
    pub implementation_version: Option<String>,
}

// InitializeResponse will always be a canned response.
impl Default for InitializeResponse {
    fn default() -> Self {
        let mut options = BitString::repeat(false, 16);

        options.set(0, true); // search
        options.set(1, true); // present

        InitializeResponse {
            reference_id: None,
            protocol_version: BitString::repeat(true, 3),
            options,
            preferred_message_size: PREF_VALUE_SIZE,
            exceptional_record_size: PREF_VALUE_SIZE,
            result: Some(true),
            implementation_id: Some(IMPLEMENTATION_ID.to_string()),
            implementation_name: Some(IMPLEMENTATION_NAME.to_string()),
            implementation_version: Some(IMPLEMENTATION_VERSION.to_string()),
        }
    }
}

pub enum KnownProximityUnit {
    Character = 1,
    Word = 2,
    Sentence = 3,
    Paragraph = 4,
    Section = 5,
    Chapter = 6,
    Document = 7,
    Element = 8,
    Subelement = 9,
    ElementType = 10,
    // Version 3 only
    Byte = 11,
}

pub enum RelationType {
    LessThan = 1,
    LessThanOrEqual = 2,
    Equal = 3,
    GreaterThanOrEqual = 4,
    GreaterThan = 5,
    NotEqual = 6,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ProximityUnitCode {
    #[rasn(tag(0))]
    Known(u32), // KnownProximityUnit
    #[rasn(tag(1))]
    Private(u32),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct ProximityOperator {
    #[rasn(tag(1))]
    exclusion: bool,
    #[rasn(tag(2))]
    distance: u32,
    #[rasn(tag(3))]
    ordered: bool,
    #[rasn(tag(4))]
    relation_type: u32,
    #[rasn(tag(5))]
    proximity_unit_code: ProximityUnitCode,
}

// NOTE a single-item enum works where 'struct DatabaseName(String)' does not.
#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum DatabaseName {
    #[rasn(tag(105))]
    Name(String),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ElementSetName {
    #[rasn(tag(103))]
    Name(String),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct DatabaseSpecific {
    db_name: DatabaseName,
    esn: ElementSetName,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ElementSetNames {
    #[rasn(tag(0))]
    GenericElementSetName(String),
    #[rasn(tag(1))]
    DatabaseSpecific(SequenceOf<DatabaseSpecific>),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum StringOrNumeric {
    #[rasn(tag(1))]
    String(String),
    #[rasn(tag(2))]
    Numeric(u32),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct ComplexAttributeValue {
    #[rasn(tag(1))]
    list: SequenceOf<StringOrNumeric>,
    #[rasn(tag(2))]
    semantic_action: Option<SequenceOf<u32>>,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum AttributeValue {
    #[rasn(tag(121))]
    Numeric(u32),
    #[rasn(tag(224))]
    Complex(ComplexAttributeValue),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
#[rasn(tag(46))]
pub enum Operator {
    #[rasn(tag(0))]
    And,
    #[rasn(tag(1))]
    Or,
    #[rasn(tag(2))]
    AndNot,
    #[rasn(tag(3))]
    Prox(ProximityOperator),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct Unit {
    #[rasn(tag(1))]
    unit_system: Option<String>,
    #[rasn(tag(2))]
    unit_type: Option<StringOrNumeric>,
    #[rasn(tag(3))]
    unit: Option<StringOrNumeric>,
    #[rasn(tag(4))]
    scale_factor: Option<u32>,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct IntUnit {
    #[rasn(tag(1))]
    value: u32,
    #[rasn(tag(2))]
    unit_used: Unit,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Term {
    #[rasn(tag(45))]
    General(OctetString),
    #[rasn(tag(215))]
    Numeric(u32),
    #[rasn(tag(216))]
    CharacterString(String),
    #[rasn(tag(217))]
    Oid(ObjectIdentifier),
    #[rasn(tag(218))]
    DateTime(GeneralizedTime),
    #[rasn(tag(219))]
    External(Any),
    #[rasn(tag(220))]
    IntegerAndUnit(IntUnit),
    #[rasn(tag(221))]
    Null,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct AttributeElement {
    #[rasn(tag(1))]
    attribute_set: Option<ObjectIdentifier>,
    #[rasn(tag(120))]
    attribute_type: u32,
    attribute_value: AttributeValue,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct ResultSetPlusAttributes {
    result_set: ObjectIdentifier,
    #[rasn(tag(44))]
    attributes: Vec<AttributeElement>,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct AttributesPlusTerm {
    #[rasn(tag(44))]
    attributes: Vec<AttributeElement>,
    term: Term,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Operand {
    #[rasn(tag(102))]
    AttrTerm(AttributesPlusTerm),
    #[rasn(tag(31))]
    ResultSet(String),
    #[rasn(tag(214))]
    ResultAttr(ResultSetPlusAttributes),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct RpnOp {
    rpn1: RpnStructure,
    rpn2: RpnStructure,
    op: Operator,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum RpnStructure {
    #[rasn(tag(0))]
    Op(Operand),
    #[rasn(tag(1))]
    RpnOp(Box<RpnOp>),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct RpnQuery {
    attribute_set: ObjectIdentifier,
    rpn: RpnStructure,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Query {
    #[rasn(tag(0))]
    Type0(Any),
    #[rasn(tag(1))]
    Type1(RpnQuery),
    #[rasn(tag(2))]
    Type2(OctetString),
    #[rasn(tag(100))]
    Type100(OctetString),
    #[rasn(tag(101))]
    Type101(RpnQuery),
    #[rasn(tag(102))]
    Type102(OctetString),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 22))]
pub struct SearchRequest {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(13))]
    small_set_upper_bound: u32,
    #[rasn(tag(14))]
    large_set_lower_bound: u32,
    #[rasn(tag(15))]
    medium_set_present_number: u32,
    #[rasn(tag(16))]
    replace_indicator: bool,
    #[rasn(tag(17))]
    result_set_name: String,
    #[rasn(tag(18))]
    database_names: Vec<DatabaseName>,
    #[rasn(tag(21))]
    query: Query,
    #[rasn(tag(100))]
    small_set_element_names: Option<ElementSetNames>,
    #[rasn(tag(101))]
    medium_set_element_names: Option<ElementSetNames>,
    #[rasn(tag(104))]
    preferred_record_syntax: Option<ObjectIdentifier>,
    #[rasn(tag(203))]
    additional_search_info: Option<Any>, // TODO
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum AddInfo {
    V2AddInfo(VisibleString),
    V3AddInfo(GeneralString),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct DefaultDiagFormat {
    diagnostic_set_id: ObjectIdentifier,
    condition: u32,
    addinfo: AddInfo,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum DiagRec {
    DefaultFormat(DefaultDiagFormat),
    ExternallyDefined(Any),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum FragmentSyntax {
    ExternallyTagged(Any),
    NotExternallyTagged(OctetString),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Record {
    #[rasn(tag(1))]
    RetrievalRecord(Any),
    #[rasn(tag(2))]
    SurrogateDiagnostic(DiagRec),
    #[rasn(tag(3))]
    StartingFragment(FragmentSyntax),
    #[rasn(tag(4))]
    IntermediateFragment(FragmentSyntax),
    #[rasn(tag(5))]
    FinalFragment(FragmentSyntax),
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct NamePlusRecord {
    #[rasn(tag(0))]
    name: Option<DatabaseName>,
    #[rasn(tag(1))]
    record: Record,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Records {
    #[rasn(tag(28))]
    ResponseRecords(Vec<NamePlusRecord>),
    #[rasn(tag(130))]
    NonSurrogateDiagnostic(DefaultDiagFormat),
    #[rasn(tag(205))]
    MultipleNonSurDiagnostics(Vec<DiagRec>),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 23))]
pub struct SearchResponse {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(23))]
    pub result_count: u32,
    #[rasn(tag(24))]
    pub number_of_records_returned: u32,
    #[rasn(tag(25))]
    pub next_result_set_position: u32,
    #[rasn(tag(22))]
    pub search_status: bool,
    #[rasn(tag(26))]
    pub result_set_status: Option<u32>,  // TODO enum
    #[rasn(tag(27))]
    pub present_status: Option<u32>, // TODO enum
    pub records: Option<Records>,
    #[rasn(tag(203))]
    additional_search_info: Option<Any>, // TODO
    other_info: Option<Any>, // TODO
}

#[derive(Debug)]
pub enum MessagePayload {
    InitializeRequest(InitializeRequest),
    InitializeResponse(InitializeResponse),
    SearchRequest(SearchRequest),
    SearchResponse(SearchResponse),
}

#[derive(Debug)]
pub struct Message {
    payload: MessagePayload,
}

impl Message {
    pub fn payload(&self) -> &MessagePayload {
        &self.payload
    }

    /// Parses a collection of bytes into a Message.
    ///
    /// Returns None if more bytes are needed to complete the message.
    pub fn from_bytes(bytes: &[u8]) -> Result<Option<Self>, String> {
        if bytes.is_empty() {
            return Ok(None);
        }

        // TODO matching on the binary representation of the first byte
        // is hacky.  Parse the bits for real into a Tag.
        let payload = match format!("{:b}", &bytes[0]).as_str() {
            // The tag component are the final 5 bits, 10100=20 in this case.
            "10110100" => {
                // Tag(20)
                let msg: InitializeRequest = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::InitializeRequest(msg)
            }
            "10110101" => {
                // Tag(21)
                let msg: InitializeResponse = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::InitializeResponse(msg)
            }

            "10110110" => {
                // Tag(22)
                let msg: SearchRequest = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => {
                            eprintln!("\n{e:?}\n");
                            return Err(e.to_string());
                        }
                    },
                };

                MessagePayload::SearchRequest(msg)
            }
            "10110111" => {
                // Tag(23)
                let msg: SearchResponse = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => {
                            eprintln!("\n{e:?}\n");
                            return Err(e.to_string());
                        }
                    },
                };

                MessagePayload::SearchResponse(msg)
            }

            _ => todo!(),
        };

        Ok(Some(Message { payload }))
    }

    pub fn from_payload(payload: MessagePayload) -> Self {
        Message { payload }
    }

    /// Translate a message into a collection of bytes suitable for delivery.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let res = match &self.payload {
            MessagePayload::InitializeRequest(m) => rasn::ber::encode(&m),
            MessagePayload::InitializeResponse(m) => rasn::ber::encode(&m),
            MessagePayload::SearchRequest(m) => rasn::ber::encode(&m),
            MessagePayload::SearchResponse(m) => rasn::ber::encode(&m),
        };

        res.map_err(|e| e.to_string())
    }
}

#[test]
fn test_encode_decode() {
    // Example InitializeRequest from YAZ client.
    let init_req_bytes = [
        0xb4, 0x52, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9, 0xa2, 0x85, 0x04, 0x04, 0x00,
        0x00, 0x00, 0x86, 0x04, 0x04, 0x00, 0x00, 0x00, 0x9f, 0x6e, 0x02, 0x38, 0x31, 0x9f, 0x6f,
        0x03, 0x59, 0x41, 0x5a, 0x9f, 0x70, 0x2f, 0x35, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x20, 0x63,
        0x33, 0x63, 0x65, 0x61, 0x38, 0x38, 0x31, 0x65, 0x33, 0x65, 0x37, 0x65, 0x38, 0x30, 0x62,
        0x30, 0x36, 0x39, 0x64, 0x64, 0x64, 0x31, 0x34, 0x32, 0x39, 0x39, 0x39, 0x34, 0x65, 0x35,
        0x38, 0x38, 0x34, 0x31, 0x61, 0x63, 0x62, 0x31, 0x34,
    ];

    let init_req_msg = Message::from_bytes(&init_req_bytes).unwrap().unwrap();

    let MessagePayload::InitializeRequest(init_req) = &init_req_msg.payload else {
        panic!("Wrong message type parsed: {init_req_msg:?}");
    };

    assert_eq!("YAZ", init_req.implementation_name.as_ref().unwrap());

    assert_eq!(init_req_bytes, *init_req_msg.to_bytes().unwrap());

    // Test partial values.
    assert!(Message::from_bytes(&init_req_bytes[0..10])
        .unwrap()
        .is_none());

    // Note the 26h byte (a boolean value) in Yaz in 0x01, but it's
    // 0xff in rasn.  Changed here to allow the tests to pass.

    // Bytes taking from a Yaz client init request
    let init_resp_bytes = [
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

    let init_resp_msg = Message::from_bytes(&init_resp_bytes).unwrap().unwrap();

    let MessagePayload::InitializeResponse(init_resp) = &init_resp_msg.payload else {
        panic!("Wrong message type parsed: {init_resp_msg:?}");
    };

    assert_eq!(
        "Simple2ZOOM Universal Gateway/GFS/YAZ",
        init_resp.implementation_name.as_ref().unwrap()
    );

    assert_eq!(init_resp_bytes, *init_resp_msg.to_bytes().unwrap());

    // Byte 14 replaces 0x01 with 0xff for rasn-consistent boolean value
    let search_req_bytes = [
        0xb6, 0x4e, 0x8d, 0x01, 0x00, 0x8e, 0x01, 0x01, 0x8f, 0x01, 0x00, 0x90, 0x01, 0xff, 0x91,
        0x01, 0x31, 0xb2, 0x07, 0x9f, 0x69, 0x04, 0x6b, 0x63, 0x6c, 0x73, 0xb5, 0x34, 0xa1, 0x32,
        0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x13, 0x03, 0x01, 0xa0, 0x27, 0xbf, 0x66, 0x24, 0xbf,
        0x2c, 0x14, 0x30, 0x08, 0x9f, 0x78, 0x01, 0x01, 0x9f, 0x79, 0x01, 0x07, 0x30, 0x08, 0x9f,
        0x78, 0x01, 0x05, 0x9f, 0x79, 0x01, 0x01, 0x9f, 0x2d, 0x0a, 0x30, 0x38, 0x37, 0x39, 0x33,
        0x30, 0x33, 0x37, 0x32, 0x37,
    ];

    let search_req_msg = Message::from_bytes(&search_req_bytes).unwrap().unwrap();

    let MessagePayload::SearchRequest(search_req) = &search_req_msg.payload else {
        panic!("Wrong message type parsed: {search_req_msg:?}");
    };

    //println!("\n{search_req:?}\n");

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
    ) = search_req.query else {
        panic!("Search request has unexpected structure; no isbn found");
    };
    */

    // Extract the ISBN from within the query one piece at a time.
    let Query::Type1(ref rpn_query) = search_req.query else {
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

    assert_eq!(*b"0879303727", **isbn);
    // OR
    assert_eq!("0879303727", std::str::from_utf8(&isbn.slice(..)).unwrap());

    assert_eq!(search_req_bytes, *search_req_msg.to_bytes().unwrap());

    // Final bool changed from 0x01 to 0xff
    let search_resp_bytes = 
        [0xb7, 0x0c, 0x97, 0x01, 0x01, 0x98, 0x01, 0x00, 0x99, 0x01, 0x01, 0x96, 0x01, 0xff];

    let search_resp_msg = Message::from_bytes(&search_resp_bytes).unwrap().unwrap();

    let MessagePayload::SearchResponse(search_resp) = &search_resp_msg.payload else {
        panic!("Wrong message type parsed: {search_resp_msg:?}");
    };

    assert_eq!(search_resp_bytes, *search_resp_msg.to_bytes().unwrap());
}
