use crate::osrf::message::Message;
use crate::osrf::message::Payload;
use crate::osrf::message::TransportMessage;
use json;

const TRANSPORT_MSG_JSON: &str = r#"{
    "to":"my-to",
    "from":"my-from",
    "thread":"my-thread",
    "body":[{
        "__c":"osrfMessage",
        "__p":{
            "threadTrace":1,
            "type":"REQUEST",
            "locale":"en-US",
            "timezone":"America/New_York",
            "api_level":1,
            "ingress":"opensrf",
            "payload":{
                "__c":"osrfMethod",
                "__p":{
                    "method":"opensrf.system.echo",
                    "params":["Hello","World"]
                }
            }
        }
    }]
}"#;

#[test]
fn parse_transport_message() {
    let json_value = json::parse(TRANSPORT_MSG_JSON).unwrap();
    let tm = TransportMessage::from_json_value(json_value, true).unwrap();

    assert_eq!(tm.thread(), "my-thread");

    let msg = &tm.body()[0];
    let type_str: &str = (*msg.mtype()).into();
    assert_eq!(type_str, "REQUEST");

    if let Payload::Method(method) = msg.payload() {
        assert_eq!(method.params()[0].as_str().unwrap(), "Hello");
    } else {
        panic!("Transport message failed to parse as Method");
    }
}

#[test]
fn parse_opensrf_message() {
    let mut json_value = json::parse(TRANSPORT_MSG_JSON).unwrap();
    let body = json_value["body"][0].take();
    let msg_op = Message::from_json_value(body, true);
    assert!(msg_op.is_ok());
    let msg = msg_op.unwrap();
    assert_eq!(msg.ingress(), Some("opensrf"));
}
