use der_parser::ber::*;
use der_parser::der::Tag;

// ASN definitions graciously provided via Wireshark template
// https://github.com/wireshark/wireshark/blob/master/epan/dissectors/asn1/z3950/z3950.asn

/// Most messages are a few layers deep at most.
const MAX_BER_RECURSION: usize = 100;

/// Returns None with logged warning on invalid data.
fn bytes2u32(octets: &[u8]) -> Option<u32> {
    BerObjectContent::Integer(octets).as_u32().map_err(|e| {
        log::warn!("bytes2u32() octets={octets:?} => {e:?}");
        e
    }).ok()
}

/// Returns None with logged warning on invalid data.
fn bytes2string(octets: &[u8]) -> Option<String> {
    if octets.is_empty() {
        None
    } else {
        std::str::from_utf8(octets)
        .map_err(|e| {
            log::warn!("Invalid utf8 string: {octets:?} => {e:?}");
            e
        })
        .map(|s| s.to_string())
        .ok()
    }
}


#[derive(Debug)]
pub enum MessageType {
    InitializeRequest = 20,
    InitializeResponse = 21,
}

#[derive(Debug)]
pub enum MessagePayload {
    InitializeRequest(InitializeRequest),
    InitializeResponse(InitializeResponse),
}

#[derive(Debug)]
pub struct Message {
    /// [2] OCTET STRING
    reference_id: Option<String>,
    payload: MessagePayload,
}

impl Message {
    pub fn from_ber(ber: &BerObject) -> Result<Self, String> {
        let tag: u32 = ber.header.tag().0; // single-entry tuple

        let payload = if tag == MessageType::InitializeRequest as u32 {
            MessagePayload::InitializeRequest(InitializeRequest::from_ber(ber)?)
        } else if tag == MessageType::InitializeResponse as u32 {
            MessagePayload::InitializeResponse(InitializeResponse::from_ber(ber)?)
        } else {
            todo!();
        };

        Ok(Message {
            reference_id: None, // TODO
            payload,
        })
    }
}

#[derive(Debug)]
pub struct InitializeRequest {
    /// [5] INTEGER
    preferred_message_size: Option<u32>,
    /// [6] INTEGER
    exceptional_record_size: Option<u32>,
    /// [110] InternationalString
    implementation_id: Option<String>,
    /// [111] InternationalString
    implementation_name: Option<String>,
    /// [112] InternationalString
    implementation_version: Option<String>,
}

impl InitializeRequest {
    pub fn from_ber(ber: &BerObject) -> Result<InitializeRequest, String> {
        let mut req = InitializeRequest {
            preferred_message_size: None,
            exceptional_record_size: None,
            implementation_id: None,
            implementation_name: None,
            implementation_version: None,
        };

        req.from_ber_r(ber)?;

        Ok(req)
    }

    fn from_ber_r(&mut self, ber: &BerObject) -> Result<(), String> {
        let BerObjectContent::Unknown(ref any) = ber.content else { return Ok(()) };

        match ber.header.tag() {
            Tag(5) => self.preferred_message_size = bytes2u32(&any.data),
            Tag(6) => self.exceptional_record_size = bytes2u32(&any.data),
            Tag(110) => self.implementation_id = bytes2string(&any.data),
            Tag(111) => self.implementation_name = bytes2string(&any.data),
            Tag(112) => self.implementation_version = bytes2string(&any.data),
            _ => {
                let mut octets = any.data;

                while !octets.is_empty() {
                    match parse_ber_any_r(octets, MAX_BER_RECURSION) {
                        Ok((rem, obj)) => {
                            self.from_ber_r(&obj)?;
                            octets = rem;
                        }
                        Err(e) => {
                            if ber.is_constructed() {
                                log::error!("parsing error: {e:?} bytes={octets:?}");
                            } else {
                                // No encapsulated data?
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum InitializeOptions {
    Search = 0,
    Present = 1,
    DelSet = 2,
    ResourceReport = 3,
    TriggerResourceCtrl = 4,
    ResourceCtrl = 5,
    AccessCtrl = 6,
    Scan = 7,
    Sort = 8,
    Reserved = 9,
    ExtendedServices = 10,
    Level1Segmentation = 11,
    Level2Segmentation = 12,
    ConcurrentOperations = 13,
    NamedResultSets = 14,
    Unused = 15,
}

impl From<usize> for InitializeOptions {
    fn from(n: usize) -> Self {
        match n {
            0 => Self::Search,
            1 => Self::Present,
            2 => Self::DelSet,
            3 => Self::ResourceReport,
            4 => Self::TriggerResourceCtrl,
            5 => Self::ResourceCtrl,
            6 => Self::AccessCtrl,
            7 => Self::Scan,
            8 => Self::Sort,
            9 => Self::Reserved,
            10 => Self::ExtendedServices,
            11 => Self::Level1Segmentation,
            12 => Self::Level2Segmentation,
            13 => Self::ConcurrentOperations,
            14 => Self::NamedResultSets,
            _ => Self::Unused,
        }
    }
}


impl InitializeOptions {
    pub fn range() -> impl Iterator<Item = usize> {
        0..15 // Slot #15 is unusued
    }
}

#[derive(Debug)]
pub struct InitializeResponse {
    /// [4] BIT STRING
    options: Vec<InitializeOptions>,
    /// [5] INTEGER
    preferred_message_size: Option<u32>,
    /// [6] INTEGER
    exceptional_record_size: Option<u32>,
    /// [12]
    result: bool,
    /// [110] InternationalString
    implementation_id: Option<String>,
    /// [111] InternationalString
    implementation_name: Option<String>,
    /// [112] InternationalString
    implementation_version: Option<String>,
}

impl InitializeResponse {

    /// Translate a Ber object into an InitializeResponse object.
    pub fn from_ber(ber: &BerObject) -> Result<InitializeResponse, String> {
        let mut req = InitializeResponse {
            options: Vec::new(),
            result: false,
            preferred_message_size: None,
            exceptional_record_size: None,
            implementation_id: None,
            implementation_name: None,
            implementation_version: None,
        };

        req.from_ber_r(ber)?;

        Ok(req)
    }

    fn from_ber_r(&mut self, ber: &BerObject) -> Result<(), String> {
        let BerObjectContent::Unknown(ref any) = ber.content else { return Ok(()) };

        match ber.header.tag() {
            Tag(4) => {
                let bs = BitStringObject { data: any.data };

                for pos in InitializeOptions::range() { // slot #15 is unused
                    let op = InitializeOptions::from(pos);

                    // The Options data is 3 bytes, but only the second 2 contain
                    // flag data.  Add 8 to the bit offset since BitStringObject
                    // includes the initial byte in the calculation.
                    if bs.is_set(op as usize + 8) {
                        self.options.push(op);
                    }
                }

            }
            Tag(5) => self.preferred_message_size = bytes2u32(&any.data),
            Tag(6) => self.exceptional_record_size = bytes2u32(&any.data),
            Tag(12) => {
                println!("bOOL = {:?}", any.data);
                self.result = any.data[0] != 0;
            }
            Tag(110) => self.implementation_id = bytes2string(&any.data),
            Tag(111) => self.implementation_name = bytes2string(&any.data),
            Tag(112) => self.implementation_version = bytes2string(&any.data),
            _ => {
                let mut octets = any.data;

                while !octets.is_empty() {
                    match parse_ber_any_r(octets, MAX_BER_RECURSION) {
                        Ok((rem, obj)) => {
                            self.from_ber_r(&obj)?;
                            octets = rem;
                        }
                        Err(e) => {
                            if ber.is_constructed() {
                                log::error!("parsing error: {e:?} bytes={octets:?}");
                            } else {
                                // No encapsulated data?
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[test]
fn test_create_initalize_response_from_ber() {
    let bytes = [0xb5, 0x7f, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9, 0x82, 0x85, 0x04, 0x04, 0x00, 0x00, 0x00, 0x86, 0x04, 0x04, 0x00, 0x00, 0x00, 0x8c, 0x01, 0x01, 0x9f, 0x6e, 0x05, 0x38, 0x31, 0x2f, 0x38, 0x31, 0x9f, 0x6f, 0x25, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x32, 0x5a, 0x4f, 0x4f, 0x4d, 0x20, 0x55, 0x6e, 0x69, 0x76, 0x65, 0x72, 0x73, 0x61, 0x6c, 0x20, 0x47, 0x61, 0x74, 0x65, 0x77, 0x61, 0x79, 0x2f, 0x47, 0x46, 0x53, 0x2f, 0x59, 0x41, 0x5a, 0x9f, 0x70, 0x34, 0x31, 0x2e, 0x30, 0x34, 0x2f, 0x35, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x20, 0x63, 0x33, 0x63, 0x65, 0x61, 0x38, 0x38, 0x31, 0x65, 0x33, 0x65, 0x37, 0x65, 0x38, 0x30, 0x62, 0x30, 0x36, 0x39, 0x64, 0x64, 0x64, 0x31, 0x34, 0x32, 0x39, 0x39, 0x39, 0x34, 0x65, 0x35, 0x38, 0x38, 0x34, 0x31, 0x61, 0x63, 0x62, 0x31, 0x34];

    let (r, o) = parse_ber(&bytes).unwrap();

    let msg = Message::from_ber(&o).unwrap();

    println!("InitializeResponse: {msg:?}");
}


