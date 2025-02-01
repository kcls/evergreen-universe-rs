//! Z39.50 Message Components
//!
//! See https://www.loc.gov/z3950/agency/asn1.html
use crate::settings::Settings;

use getset::{Getters, Setters};
use rasn::ber::de::DecodeErrorKind;
use rasn::prelude::*;
use rasn::AsnType;

// https://oid-base.com/get/1.2.840.10003.5.10
pub const OID_MARC21: [u32; 6] = [1, 2, 840, 10003, 5, 10];

// https://oid-base.com/get/1.2.840.10003.3.1
pub const OID_ATTR_SET_BIB1: [u32; 6] = [1, 2, 840, 10003, 3, 1];

#[derive(Debug, Default, AsnType, Decode, Encode)]
#[rasn(tag(context, 20))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct InitializeRequest {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
    #[rasn(tag(3))]
    protocol_version: BitString,
    #[rasn(tag(4))]
    options: BitString,
    #[rasn(tag(5))]
    preferred_message_size: u32,
    #[rasn(tag(6))]
    exceptional_record_size: u32,
    #[rasn(tag(110))]
    implementation_id: Option<String>,
    #[rasn(tag(111))]
    implementation_name: Option<String>,
    #[rasn(tag(112))]
    implementation_version: Option<String>,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 21))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct InitializeResponse {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
    #[rasn(tag(3))]
    protocol_version: BitString,
    #[rasn(tag(4))]
    options: BitString,
    #[rasn(tag(5))]
    preferred_message_size: u32,
    #[rasn(tag(6))]
    exceptional_record_size: u32,
    #[rasn(tag(12))]
    result: Option<bool>,
    #[rasn(tag(110))]
    implementation_id: Option<String>,
    #[rasn(tag(111))]
    implementation_name: Option<String>,
    #[rasn(tag(112))]
    implementation_version: Option<String>,
}

// InitializeResponse will always be a canned response.
impl Default for InitializeResponse {
    fn default() -> Self {
        let settings = Settings::global();

        // Translate the InitOptions values into the required BitString
        let mut options = BitString::repeat(false, 16);
        for (idx, val) in settings
            .init_options
            .as_positioned_values()
            .iter()
            .enumerate()
        {
            if *val {
                options.set(idx, true);
            }
        }

        InitializeResponse {
            reference_id: None,
            protocol_version: BitString::repeat(true, 3),
            options,
            result: Some(true),
            preferred_message_size: settings.preferred_message_size,
            exceptional_record_size: settings.exceptional_record_size,
            implementation_id: settings.implementation_id.clone(),
            implementation_name: settings.implementation_name.clone(),
            implementation_version: settings.implementation_version.clone(),
        }
    }
}

#[repr(u32)]
pub enum KnownProximityUnit {
    Character = 1,
    Word,
    Sentence,
    Paragraph,
    Section,
    Chapter,
    Document,
    Element,
    Subelement,
    ElementType,
    Byte,
    Unknown(u32),
}

#[repr(u32)]
pub enum RelationType {
    LessThan = 1,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
    NotEqual,
    Unknown(u32),
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

// NOTE a single-item enum Encodes/Decodes as expected, whereas a
// 'struct DatabaseName(String)' does not.
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

#[derive(Debug, AsnType, Decode, Encode, Getters, Setters)]
#[getset(set = "pub", get = "pub")]
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

#[derive(Debug, AsnType, Decode, Encode, Getters, Setters)]
#[getset(set = "pub", get = "pub")]
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
#[rasn(choice)]
pub enum Information {
    #[rasn(tag(2))]
    CharacterInfo(String),
    #[rasn(tag(3))]
    BinaryInfo(OctetString),
    #[rasn(tag(4))]
    ExternallyDefinedInfo(Any),
    #[rasn(tag(5))]
    Oid(ObjectIdentifier),
}

#[derive(Debug, AsnType, Decode, Encode, Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct InfoCategory {
    #[rasn(tag(1))]
    category_type_id: Option<ObjectIdentifier>,
    #[rasn(tag(2))]
    category_value: Option<u32>,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(201))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct OtherInformation {
    #[rasn(tag(1))]
    category: Option<InfoCategory>,
    information: Information,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 22))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct SearchRequest {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
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
    additional_search_info: Option<OtherInformation>,
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
pub enum Encoding {
    #[rasn(tag(0))]
    SingleAsn1Type(Any),
    #[rasn(tag(1))]
    OctetAligned(OctetString),
    #[rasn(tag(2))]
    Arbitrary(BitString),
}


#[derive(Debug, AsnType, Encode, Decode)]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
#[rasn(tag(universal, 8))]
pub struct ExternalBody {
    direct_reference: Option<ObjectIdentifier>,
    indirect_reference: Option<u32>,
    data_value_descriptor: Option<String>,
    encoding: Encoding
}

impl ExternalBody {
    pub fn new(encoding: Encoding) -> Self {
        Self {
            direct_reference: None,
            indirect_reference: None,
            data_value_descriptor: None,
            encoding,
        }
    }
}

// Wrapper around our ExternalBody type, which seems to be required
// to make rasn honor the struct-level UNIVERSAL tag on our ExternalBody
// type.  Otherwise, it either ignores the tag or, if explicit is used,
// it adds the tag and an unnecessary SEQUENCE tag.  *shrug*.  This 
// fixes it, and gives us the EXTERNAL tag without the SEQUENCE.
#[derive(Debug, AsnType, Decode, Encode)]
pub struct External {
    pub ext: ExternalBody
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Record {
    #[rasn(tag(1))]
    RetrievalRecord(External),
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
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct NamePlusRecord {
    #[rasn(tag(0))]
    name: Option<DatabaseName>,
    #[rasn(tag(1))]
    record: Record,
}

impl NamePlusRecord {
    pub fn new(record: Record) -> Self {
        Self {
            name: None,
            record,
        }
    }
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
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct SearchResponse {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
    #[rasn(tag(23))]
    result_count: u32,
    #[rasn(tag(24))]
    number_of_records_returned: u32,
    #[rasn(tag(25))]
    next_result_set_position: u32,
    #[rasn(tag(22))]
    search_status: bool,
    #[rasn(tag(26))]
    result_set_status: Option<u32>, // TODO will an enum work for an int value?
    #[rasn(tag(27))]
    present_status: Option<u32>, // TODO enum?
    records: Option<Records>,
    #[rasn(tag(203))]
    additional_search_info: Option<OtherInformation>,
    other_info: Option<OtherInformation>,
}

#[derive(Debug, AsnType, Decode, Encode, Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct Range {
    #[rasn(tag(1))]
    starting_position: u32,
    #[rasn(tag(2))]
    number_of_records: u32,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct ElementSpec {
    #[rasn(tag(1))]
    element_set_name: String,
    #[rasn(tag(2))]
    external_espec: Option<Any>,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct Specification {
    #[rasn(tag(1))]
    schema: Option<ObjectIdentifier>,
    #[rasn(tag(2))]
    element_spec: Option<ElementSpec>,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct CompSpecDatabaseSpecific {
    #[rasn(tag(1))]
    db: DatabaseName,
    #[rasn(tag(2))]
    spec: Specification,
}

#[derive(Debug, AsnType, Decode, Encode)]
pub struct CompSpec {
    #[rasn(tag(1))]
    select_alternative_syntax: bool,
    #[rasn(tag(2))]
    generic: Option<Specification>,
    #[rasn(tag(3))]
    db_specific: Option<CompSpecDatabaseSpecific>,
    #[rasn(tag(4))]
    record_syntax: Option<Vec<ObjectIdentifier>>,
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum RecordComposition {
    #[rasn(tag(19))]
    Simple(ElementSetNames),
    #[rasn(tag(209))]
    Complex(CompSpec),
}

#[derive(Debug, AsnType, Decode, Encode)]
#[rasn(tag(context, 24))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct PresentRequest {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
    #[rasn(tag(31))]
    result_set_id: String,
    #[rasn(tag(30))]
    reset_set_start_point: u32,
    #[rasn(tag(29))]
    number_of_records_requested: u32,
    #[rasn(tag(212))]
    additional_ranges: Option<Vec<Range>>,
    record_composition: Option<RecordComposition>,
    #[rasn(tag(104))]
    preferred_record_syntax: Option<ObjectIdentifier>,
    #[rasn(tag(204))]
    max_segment_count: Option<u32>,
    #[rasn(tag(206))]
    max_record_size: Option<u32>,
    #[rasn(tag(207))]
    max_segment_size: Option<u32>,
    other_info: Option<OtherInformation>,
}



#[derive(Debug, AsnType, Decode, Encode, Default)]
#[rasn(tag(context, 25))]
#[derive(Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct PresentResponse {
    #[rasn(tag(2))]
    reference_id: Option<OctetString>,
    #[rasn(tag(24))]
    number_of_records_returned: u32,
    #[rasn(tag(25))]
    next_result_set_position: u32,
    #[rasn(tag(27))]
    present_status: u32, // TODO try enum
    records: Option<Records>,
    other_info: Option<OtherInformation>,
}

#[derive(Debug)]
pub enum MessagePayload {
    InitializeRequest(InitializeRequest),
    InitializeResponse(InitializeResponse),
    SearchRequest(SearchRequest),
    SearchResponse(SearchResponse),
    PresentRequest(PresentRequest),
    PresentResponse(PresentResponse),
}

#[derive(Debug, Getters, Setters)]
#[getset(set = "pub", get = "pub")]
pub struct Message {
    payload: MessagePayload,
}

impl Message {
    /// Parses a collection of bytes into a Message.
    ///
    /// Returns None if more bytes are needed to complete the message.
    pub fn from_bytes(bytes: &[u8]) -> Result<Option<Self>, String> {
        if bytes.is_empty() {
            return Ok(None);
        }

        // The first byte of a Z39 ASN message is structed like so:
        // [
        //   76543210   - bit index
        //   10         - class = context-specific
        //     1        - structured data
        //      nnnnn   - PDU / message tag.
        //  ]
        //
        //  As such, the Initialize Request message, with tag 20, has a
        //  first-byte value of 10110100 == 180 decimal, i.e. 160 + 20.
        let tag = if bytes[0] >= 180 { bytes[0] - 160 } else { 0 };

        let payload = match tag {
            20 => {
                let msg: InitializeRequest = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::InitializeRequest(msg)
            }
            21 => {
                let msg: InitializeResponse = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::InitializeResponse(msg)
            }
            22 => {
                let msg: SearchRequest = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::SearchRequest(msg)
            }
            23 => {
                let msg: SearchResponse = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::SearchResponse(msg)
            }
            24 => {
                let msg: PresentRequest = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::PresentRequest(msg)
            }
            25 => {
                let msg: PresentResponse = match rasn::ber::decode(bytes) {
                    Ok(m) => m,
                    Err(e) => match *e.kind {
                        DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                        _ => return Err(e.to_string()),
                    },
                };

                MessagePayload::PresentResponse(msg)
            }

            _ => {
                return Err(format!(
                    "Cannot handle message with first byte: {}",
                    bytes[0]
                ))
            }
        };

        Ok(Some(Message { payload }))
    }

    pub fn from_payload(payload: MessagePayload) -> Self {
        Message { payload }
    }

    /// Translate a message into a collection of bytes suitable for dropping
    /// onto the wire.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let res = match self.payload() {
            MessagePayload::InitializeRequest(m) => rasn::ber::encode(&m),
            MessagePayload::InitializeResponse(m) => rasn::ber::encode(&m),
            MessagePayload::SearchRequest(m) => rasn::ber::encode(&m),
            MessagePayload::SearchResponse(m) => rasn::ber::encode(&m),
            MessagePayload::PresentRequest(m) => rasn::ber::encode(&m),
            MessagePayload::PresentResponse(m) => rasn::ber::encode(&m),
        };

        res.map_err(|e| e.to_string())
    }
}
