//! Z39.50 ASN.1 Primary Data Units (i.e. Messages) and Related Types
//!
//! See https://www.loc.gov/z3950/agency/asn1.html
use crate::error::{LocalError, LocalResult};
use crate::prefs::ImplementationPrefs;

use rasn::ber::de::DecodeErrorKind;
use rasn::prelude::*;
use rasn::AsnType;

#[derive(Debug, Clone, PartialEq, Default, AsnType, Decode, Encode)]
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

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
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
        let settings = ImplementationPrefs::global();

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

#[derive(Debug, Clone, Copy, PartialEq, AsnType, Decode, Encode)]
#[rasn(enumerated)]
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
}

#[derive(Debug, Clone, Copy, PartialEq, AsnType, Decode, Encode)]
#[rasn(enumerated)]
pub enum RelationType {
    LessThan = 1,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
    NotEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, AsnType, Decode, Encode)]
#[rasn(enumerated)]
pub enum ResultSetStatus {
    Empty = 1,
    Interim,
    Unchanged,
    None,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ProximityUnitCode {
    #[rasn(tag(0))]
    Known(KnownProximityUnit),
    #[rasn(tag(1))]
    Private(u32),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct ProximityOperator {
    #[rasn(tag(1))]
    pub exclusion: bool,
    #[rasn(tag(2))]
    pub distance: u32,
    #[rasn(tag(3))]
    pub ordered: bool,
    #[rasn(tag(4))]
    pub relation_type: RelationType,
    #[rasn(tag(5))]
    pub proximity_unit_code: ProximityUnitCode,
}

// NOTE a single-item enum Encodes/Decodes as expected, whereas a
// 'struct DatabaseName(String)' does not.
#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum DatabaseName {
    #[rasn(tag(105))]
    Name(String),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ElementSetName {
    #[rasn(tag(103))]
    Name(String),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct DatabaseSpecific {
    pub db_name: DatabaseName,
    pub esn: ElementSetName,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum ElementSetNames {
    #[rasn(tag(0))]
    GenericElementSetName(String),
    #[rasn(tag(1))]
    DatabaseSpecific(Vec<DatabaseSpecific>),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum StringOrNumeric {
    #[rasn(tag(1))]
    String(String),
    #[rasn(tag(2))]
    Numeric(u32),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct ComplexAttributeValue {
    #[rasn(tag(1))]
    pub list: Vec<StringOrNumeric>,
    #[rasn(tag(2))]
    pub semantic_action: Option<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum AttributeValue {
    #[rasn(tag(121))]
    Numeric(u32),
    #[rasn(tag(224))]
    Complex(ComplexAttributeValue),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
// #[rasn(tag(46))]
// Uses tag 46 but that's best encoded in the containing struct, instead
// of attached to the Operator struct, to make rasn happy.
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

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct Unit {
    #[rasn(tag(1))]
    pub unit_system: Option<String>,
    #[rasn(tag(2))]
    pub unit_type: Option<StringOrNumeric>,
    #[rasn(tag(3))]
    pub unit: Option<StringOrNumeric>,
    #[rasn(tag(4))]
    pub scale_factor: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct IntUnit {
    #[rasn(tag(1))]
    pub value: u32,
    #[rasn(tag(2))]
    pub unit_used: Unit,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
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

impl Term {
    pub fn general_from_str(s: &str) -> Term {
        Term::General(OctetString::copy_from_slice(s.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct AttributeElement {
    #[rasn(tag(1))]
    pub attribute_set: Option<ObjectIdentifier>,
    #[rasn(tag(120))]
    pub attribute_type: u32,
    pub attribute_value: AttributeValue,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct ResultSetPlusAttributes {
    pub result_set: ObjectIdentifier,
    #[rasn(tag(44))]
    pub attributes: Vec<AttributeElement>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct AttributesPlusTerm {
    #[rasn(tag(44))]
    pub attributes: Vec<AttributeElement>,
    pub term: Term,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Operand {
    #[rasn(tag(102))]
    AttrTerm(AttributesPlusTerm),
    #[rasn(tag(31))]
    ResultSet(String),
    #[rasn(tag(214))]
    ResultAttr(ResultSetPlusAttributes),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct RpnOp {
    pub rpn1: RpnStructure,
    pub rpn2: RpnStructure,
    #[rasn(tag(46))]
    pub op: Operator,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum RpnStructure {
    #[rasn(tag(0))]
    Op(Operand),
    #[rasn(tag(1))]
    RpnOp(Box<RpnOp>),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct RpnQuery {
    pub attribute_set: ObjectIdentifier,
    pub rpn: RpnStructure,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
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

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
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

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct InfoCategory {
    #[rasn(tag(1))]
    pub category_type_id: Option<ObjectIdentifier>,
    #[rasn(tag(2))]
    pub category_value: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(tag(201))]
pub struct OtherInformation {
    #[rasn(tag(1))]
    pub category: Option<InfoCategory>,
    pub information: Information,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(tag(context, 22))]
pub struct SearchRequest {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(13))]
    pub small_set_upper_bound: u32,
    #[rasn(tag(14))]
    pub large_set_lower_bound: u32,
    #[rasn(tag(15))]
    pub medium_set_present_number: u32,
    #[rasn(tag(16))]
    pub replace_indicator: bool,
    #[rasn(tag(17))]
    pub result_set_name: String,
    #[rasn(tag(18))]
    pub database_names: Vec<DatabaseName>,
    #[rasn(tag(21))]
    pub query: Query,
    #[rasn(tag(100))]
    pub small_set_element_names: Option<ElementSetNames>,
    #[rasn(tag(101))]
    pub medium_set_element_names: Option<ElementSetNames>,
    #[rasn(tag(104))]
    pub preferred_record_syntax: Option<ObjectIdentifier>,
    #[rasn(tag(203))]
    pub additional_search_info: Option<OtherInformation>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum AddInfo {
    V2AddInfo(VisibleString),
    V3AddInfo(GeneralString),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct DefaultDiagFormat {
    pub diagnostic_set_id: ObjectIdentifier,
    pub condition: u32,
    pub addinfo: AddInfo,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum DiagRec {
    DefaultFormat(DefaultDiagFormat),
    ExternallyDefined(Any),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum FragmentSyntax {
    ExternallyTagged(Any),
    NotExternallyTagged(OctetString),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Encoding {
    #[rasn(tag(0))]
    SingleAsn1Type(Any),
    #[rasn(tag(1))]
    OctetAligned(OctetString),
    #[rasn(tag(2))]
    Arbitrary(BitString),
}

#[derive(Debug, Clone, PartialEq, AsnType, Encode, Decode)]
#[rasn(tag(universal, 8))]
pub struct ExternalMessage {
    pub direct_reference: Option<ObjectIdentifier>,
    pub indirect_reference: Option<u32>,
    pub data_value_descriptor: Option<String>,
    pub encoding: Encoding,
}

impl ExternalMessage {
    pub fn new(encoding: Encoding) -> Self {
        Self {
            direct_reference: None,
            indirect_reference: None,
            data_value_descriptor: None,
            encoding,
        }
    }
}

// Wrapper around our ExternalMessage type, which seems to be
// required to make rasn honor the struct-level UNIVERSAL tag on our
// ExternalMessage.  Otherwise, it either ignores the tag or, if
// `explicit` is used, it adds the tag and an unwanted SEQUENCE tag.
// This gives us the EXTERNAL tag without the SEQUENCE, without having
// to maually implement Encode/Decode.
#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct External(pub ExternalMessage);

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
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

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct NamePlusRecord {
    #[rasn(tag(0))]
    pub name: Option<DatabaseName>,
    #[rasn(tag(1))]
    pub record: Record,
}

impl NamePlusRecord {
    pub fn new(record: Record) -> Self {
        Self { name: None, record }
    }
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum Records {
    #[rasn(tag(28))]
    ResponseRecords(Vec<NamePlusRecord>),
    #[rasn(tag(130))]
    NonSurrogateDiagnostic(DefaultDiagFormat),
    #[rasn(tag(205))]
    MultipleNonSurDiagnostics(Vec<DiagRec>),
}

#[derive(Debug, Clone, PartialEq, Default, AsnType, Decode, Encode)]
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
    pub result_set_status: Option<ResultSetStatus>,
    #[rasn(tag(27))]
    pub present_status: Option<PresentStatus>,
    pub records: Option<Records>,
    #[rasn(tag(203))]
    pub additional_search_info: Option<OtherInformation>,
    pub other_info: Option<OtherInformation>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct Range {
    #[rasn(tag(1))]
    pub starting_position: u32,
    #[rasn(tag(2))]
    pub number_of_records: u32,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct ElementSpec {
    #[rasn(tag(1))]
    pub element_set_name: String,
    #[rasn(tag(2))]
    pub external_espec: Option<Any>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct Specification {
    #[rasn(tag(1))]
    pub schema: Option<ObjectIdentifier>,
    #[rasn(tag(2))]
    pub element_spec: Option<ElementSpec>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct CompSpecDatabaseSpecific {
    #[rasn(tag(1))]
    pub db: DatabaseName,
    #[rasn(tag(2))]
    pub spec: Specification,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
pub struct CompSpec {
    #[rasn(tag(1))]
    pub select_alternative_syntax: bool,
    #[rasn(tag(2))]
    pub generic: Option<Specification>,
    #[rasn(tag(3))]
    pub db_specific: Option<CompSpecDatabaseSpecific>,
    #[rasn(tag(4))]
    pub record_syntax: Option<Vec<ObjectIdentifier>>,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(choice)]
pub enum RecordComposition {
    #[rasn(tag(19))]
    Simple(ElementSetNames),
    #[rasn(tag(209))]
    Complex(CompSpec),
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(tag(context, 24))]
pub struct PresentRequest {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(31))]
    pub result_set_id: String,
    #[rasn(tag(30))]
    pub reset_set_start_point: u32,
    #[rasn(tag(29))]
    pub number_of_records_requested: u32,
    #[rasn(tag(212))]
    pub additional_ranges: Option<Vec<Range>>,
    pub record_composition: Option<RecordComposition>,
    #[rasn(tag(104))]
    pub preferred_record_syntax: Option<ObjectIdentifier>,
    #[rasn(tag(204))]
    pub max_segment_count: Option<u32>,
    #[rasn(tag(206))]
    pub max_record_size: Option<u32>,
    #[rasn(tag(207))]
    pub max_segment_size: Option<u32>,
    pub other_info: Option<OtherInformation>,
}

#[derive(Debug, Clone, Copy, PartialEq, AsnType, Decode, Encode)]
#[rasn(enumerated)]
pub enum PresentStatus {
    Success = 0,
    Partial1,
    Partial2,
    Partial3,
    Partial4,
    Failure,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(tag(context, 25))]
pub struct PresentResponse {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(24))]
    pub number_of_records_returned: u32,
    #[rasn(tag(25))]
    pub next_result_set_position: u32,
    #[rasn(tag(27))]
    pub present_status: PresentStatus,
    pub records: Option<Records>,
    pub other_info: Option<OtherInformation>,
}

impl Default for PresentResponse {
    fn default() -> Self {
        PresentResponse {
            reference_id: None,
            number_of_records_returned: 0,
            next_result_set_position: 0,
            present_status: PresentStatus::Success,
            records: None,
            other_info: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, AsnType, Decode, Encode)]
#[rasn(enumerated)]
pub enum CloseReason {
    Finished = 0,
    Shutdown,
    SystemProblem,
    CostLimit,
    Resources,
    SecurityViolation,
    ProtocolError,
    LackOfActivity,
    PeerAbort,
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, AsnType, Decode, Encode)]
#[rasn(tag(context, 48))]
pub struct Close {
    #[rasn(tag(2))]
    pub reference_id: Option<OctetString>,
    #[rasn(tag(211))]
    pub close_reason: CloseReason,
    #[rasn(tag(3))]
    pub diagnostic_information: Option<String>,
    #[rasn(tag(4))]
    pub resource_report_format: Option<ObjectIdentifier>,
    #[rasn(tag(5))]
    pub resource_report: Option<External>,
    pub other_info: Option<OtherInformation>,
}

impl Default for Close {
    fn default() -> Self {
        Self {
            reference_id: None,
            close_reason: CloseReason::Finished,
            diagnostic_information: None,
            resource_report_format: None,
            resource_report: None,
            other_info: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessagePayload {
    InitializeRequest(InitializeRequest),
    InitializeResponse(InitializeResponse),
    SearchRequest(SearchRequest),
    SearchResponse(SearchResponse),
    PresentRequest(PresentRequest),
    PresentResponse(PresentResponse),
    Close(Close),
}

/// Models a single Z39.50 message whose payload is one of the known
/// message types.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub payload: MessagePayload,
}

impl Message {
    /// Parses a collection of bytes into a Message.
    ///
    /// Returns None if more bytes are needed to complete the message,
    /// which can happen e.g. when reading bytes from a TcpStream.
    pub fn from_bytes(bytes: &[u8]) -> LocalResult<Option<Self>> {
        if bytes.is_empty() {
            return Ok(None);
        }

        // Parse error handler.
        // Return None if more bytes are needed, LocalError otherwise.
        let handle_error = |e: rasn::error::DecodeError| match *e.kind {
            DecodeErrorKind::Incomplete { needed: _ } => Ok(None),
            _ => Err(LocalError::DecodeError(e)),
        };

        // The first byte of a Z39 ASN1 BER message is structed like so:
        // [
        //   10......   - context-specific tag class
        //   ..1.....   - structured data
        //   ...nnnnn   - PDU / message tag.
        //  ]
        //
        //  The Initialize Request message, with tag 20, has a
        //  first-byte value of 10110100 == 180 decimal, IOW 20 + 160.
        //
        //  However, if the last 5 bits of the first byte are all 1's,
        //  the tag value is stored in the second byte (to accommodate
        //  larger tag values, for 31 <= tag <= 127).
        //
        //  There are other rules for tags > 127, but they are not needed for Z39
        let tag = if bytes[0] == 191 {
            // bytes[0] == 10111111
            bytes[1]
        } else if bytes[0] >= 180 {
            bytes[0] - 160
        } else {
            0
        };

        let payload = match tag {
            20 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::InitializeRequest(m),
                Err(e) => return handle_error(e),
            },
            21 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::InitializeResponse(m),
                Err(e) => return handle_error(e),
            },
            22 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::SearchRequest(m),
                Err(e) => return handle_error(e),
            },
            23 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::SearchResponse(m),
                Err(e) => return handle_error(e),
            },
            24 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::PresentRequest(m),
                Err(e) => return handle_error(e),
            },
            25 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::PresentResponse(m),
                Err(e) => return handle_error(e),
            },
            48 => match rasn::ber::decode(bytes) {
                Ok(m) => MessagePayload::Close(m),
                Err(e) => return handle_error(e),
            },
            _ => {
                return Err(LocalError::ProtocolError(format!(
                    "Cannot process message with first byte: {}",
                    bytes[0]
                )))
            }
        };

        Ok(Some(Message { payload }))
    }

    pub fn from_payload(payload: MessagePayload) -> Self {
        Message { payload }
    }

    /// Translate a message into a collection of bytes suitable for dropping
    /// onto the wire.
    pub fn to_bytes(&self) -> LocalResult<Vec<u8>> {
        let res = match &self.payload {
            MessagePayload::InitializeRequest(m) => rasn::ber::encode(&m),
            MessagePayload::InitializeResponse(m) => rasn::ber::encode(&m),
            MessagePayload::SearchRequest(m) => rasn::ber::encode(&m),
            MessagePayload::SearchResponse(m) => rasn::ber::encode(&m),
            MessagePayload::PresentRequest(m) => rasn::ber::encode(&m),
            MessagePayload::PresentResponse(m) => rasn::ber::encode(&m),
            MessagePayload::Close(m) => rasn::ber::encode(&m),
        };

        res.map_err(LocalError::EncodeError)
    }
}
