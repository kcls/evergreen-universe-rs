use super::message::Field;
use super::message::FixedField;
use super::message::Message;
use super::spec;

#[test]
fn invalid_fixed_field() {
    assert_eq!(FixedField::new(&spec::FF_STATUS_CODE, "123").is_err(), true);
}

#[test]
fn ok_fixed_field() {
    assert_eq!(FixedField::new(&spec::FF_STATUS_CODE, "3").is_ok(), true);
}

#[test]
fn sc_status_message() {
    // Move message creation into client.rs and just test what it creates
    let msg = Message::new(
        &spec::M_SC_STATUS,
        vec![
            FixedField::new(&spec::FF_STATUS_CODE, "0").unwrap(),
            FixedField::new(&spec::FF_MAX_PRINT_WIDTH, "999").unwrap(),
            FixedField::new(&spec::FF_PROTOCOL_VERSION, &spec::SIP_PROTOCOL_VERSION).unwrap(),
        ],
        vec![],
    );

    assert_eq!(msg.to_sip(), "9909992.00");
}

#[test]
fn login_message() {
    let msg = Message::new(
        &spec::M_LOGIN,
        vec![
            FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
            FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
        ],
        vec![
            Field::new(spec::F_LOGIN_UID.code, "sip_username"),
            Field::new(spec::F_LOGIN_PWD.code, "sip_password"),
        ],
    );

    assert_eq!(msg.to_sip(), "9300CNsip_username|COsip_password|");
}

#[test]
fn fixed_field_to_str() {
    let ff = FixedField::new(&spec::FF_MAX_PRINT_WIDTH, "999").unwrap();
    assert_eq!(ff.to_sip(), "999");
}
