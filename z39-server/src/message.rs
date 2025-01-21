use rasn::AsnType;
use rasn::prelude::*;
use rasn::ber::de::DecodeErrorKind;

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

#[derive(Debug, Default, AsnType, Decode, Encode)]
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

#[derive(Debug)]
pub enum MessagePayload {
    InitializeRequest(InitializeRequest),
    InitializeResponse(InitializeResponse),
}

#[derive(Debug)]
pub struct Message {
    payload: MessagePayload
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

        let payload = match format!("{:b}", &bytes[0]).as_str() {
            // TODO matching on the binary representation of the first byte
            // is hacky.  Parse the bits for real.

            // The tag component are the final 5 bits, 10100=20 in this case.
            "10110100" => { // Tag(20)
                let msg: InitializeRequest = match rasn::ber::decode(&bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        match *e.kind {
                            DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                            _ => return Err(e.to_string()),
                        }
                    }
                };

                MessagePayload::InitializeRequest(msg)
            }
            "10110101" => {  // Tag(21)
                let msg: InitializeResponse = match rasn::ber::decode(&bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        match *e.kind {
                            DecodeErrorKind::Incomplete { needed: _ } => return Ok(None),
                            _ => return Err(e.to_string()),
                        }
                    }
                };

                MessagePayload::InitializeResponse(msg)
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
        };

        res.map_err(|e| e.to_string())
    }
}


#[test]
fn test_encode_decode() {

    // Example InitializeRequest from YAZ client.
    let init_req_bytes = [
        0xb4, 0x52, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9,
        0xa2, 0x85, 0x04, 0x04, 0x00, 0x00, 0x00, 0x86, 0x04, 0x04,
        0x00, 0x00, 0x00, 0x9f, 0x6e, 0x02, 0x38, 0x31, 0x9f, 0x6f,
        0x03, 0x59, 0x41, 0x5a, 0x9f, 0x70, 0x2f, 0x35, 0x2e, 0x33,
        0x31, 0x2e, 0x31, 0x20, 0x63, 0x33, 0x63, 0x65, 0x61, 0x38,
        0x38, 0x31, 0x65, 0x33, 0x65, 0x37, 0x65, 0x38, 0x30, 0x62,
        0x30, 0x36, 0x39, 0x64, 0x64, 0x64, 0x31, 0x34, 0x32, 0x39,
        0x39, 0x39, 0x34, 0x65, 0x35, 0x38, 0x38, 0x34, 0x31, 0x61,
        0x63, 0x62, 0x31, 0x34
    ];

    let init_req_msg = Message::from_bytes(&init_req_bytes).unwrap().unwrap();

    let MessagePayload::InitializeRequest(init_req) = &init_req_msg.payload else {
        panic!("Wrong message type parsed: {init_req_msg:?}");
    };

    assert_eq!("YAZ", init_req.implementation_name.as_ref().unwrap());

    assert_eq!(init_req_bytes, *init_req_msg.to_bytes().unwrap());

    // Test partial values.
    assert!(Message::from_bytes(&init_req_bytes[0..10]).unwrap().is_none());

    // YAZ encodes true as 0x01, whereas rasn encodes it as 0xff.
    let tag_12_result = 0xff;

    // Bytes taking from a Yaz client init request
    let init_resp_bytes = [
        0xb5, 0x7f, 0x83, 0x02, 0x00, 0xe0, 0x84, 0x03, 0x00, 0xe9,
        0x82, 0x85, 0x04, 0x04, 0x00, 0x00, 0x00, 0x86, 0x04, 0x04,
        0x00, 0x00, 0x00, 0x8c, 0x01, tag_12_result, 0x9f, 0x6e, 0x05,
        0x38, 0x31, 0x2f, 0x38, 0x31, 0x9f, 0x6f, 0x25, 0x53, 0x69,
        0x6d, 0x70, 0x6c, 0x65, 0x32, 0x5a, 0x4f, 0x4f, 0x4d, 0x20,
        0x55, 0x6e, 0x69, 0x76, 0x65, 0x72, 0x73, 0x61, 0x6c, 0x20,
        0x47, 0x61, 0x74, 0x65, 0x77, 0x61, 0x79, 0x2f, 0x47, 0x46,
        0x53, 0x2f, 0x59, 0x41, 0x5a, 0x9f, 0x70, 0x34, 0x31, 0x2e,
        0x30, 0x34, 0x2f, 0x35, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x20,
        0x63, 0x33, 0x63, 0x65, 0x61, 0x38, 0x38, 0x31, 0x65, 0x33,
        0x65, 0x37, 0x65, 0x38, 0x30, 0x62, 0x30, 0x36, 0x39, 0x64,
        0x64, 0x64, 0x31, 0x34, 0x32, 0x39, 0x39, 0x39, 0x34, 0x65,
        0x35, 0x38, 0x38, 0x34, 0x31, 0x61, 0x63, 0x62, 0x31, 0x34
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
}

