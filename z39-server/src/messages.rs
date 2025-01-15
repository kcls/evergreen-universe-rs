use der_parser::ber::*;
use der_parser::error::*;
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

