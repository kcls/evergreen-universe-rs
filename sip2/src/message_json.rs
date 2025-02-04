//! JSON serializatoin routintes for SIP messages.
use super::Message;
use std::collections::HashMap;
use std::error;
use std::fmt;

/// Errors related specifically to SIP <=> JSON routines
#[derive(Debug)]
pub enum SipJsonError {
    /// Data does not contain the correct content, e.g. sip message code.
    MessageFormatError(String),

    /// Data cannot be successfully minipulated as JSON
    JsonError(json::Error),
}

impl error::Error for SipJsonError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            SipJsonError::JsonError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for SipJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SipJsonError::JsonError(ref err) => err.fmt(f),
            SipJsonError::MessageFormatError(s) => {
                write!(f, "SIP message could not be translated to/from JSON: {}", s)
            }
        }
    }
}

impl Message {
    /// Translate a SIP Message into a JSON object.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// use sip2::spec;
    /// use json;
    ///
    /// let msg = Message::new(
    ///     &spec::M_LOGIN,
    ///     vec![
    ///         FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
    ///         FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
    ///     ],
    ///     vec![
    ///         Field::new(spec::F_LOGIN_UID.code, "sip_username"),
    ///         Field::new(spec::F_LOGIN_PWD.code, "sip_password"),
    ///     ]
    /// );
    ///
    /// let json_val = msg.to_json_value();
    /// let expected = json::object!{
    ///   "code":"93",
    ///   "fixed_fields":["0","0"],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]};
    ///
    /// assert_eq!(expected, json_val);
    /// ```
    pub fn to_json_value(&self) -> json::JsonValue {
        let ff: Vec<String> = self
            .fixed_fields()
            .iter()
            .map(|f| f.value().to_string())
            .collect();

        let mut fields: Vec<HashMap<String, String>> = Vec::new();

        for f in self.fields().iter() {
            let mut map = HashMap::new();
            map.insert(f.code().to_string(), f.value().to_string());
            fields.push(map);
        }

        json::object! {
            "code": self.spec().code,
            "fixed_fields": ff,
            "fields": fields
        }
    }

    /// Translate a SIP Message into a JSON string.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// use sip2::spec;
    ///
    /// let msg = Message::new(
    ///     &spec::M_LOGIN,
    ///     vec![
    ///         FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
    ///         FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
    ///     ],
    ///     vec![
    ///         Field::new(spec::F_LOGIN_UID.code, "sip_username"),
    ///         Field::new(spec::F_LOGIN_PWD.code, "sip_password"),
    ///     ]
    /// );
    ///
    /// let json_str = msg.to_json();
    ///
    /// // Comparing JSON strings is nontrivial with hashes.
    /// // Assume completion means success.  See to_json_value() for
    /// // more rigorous testing.
    /// assert_eq!(true, true);
    /// ```
    pub fn to_json(&self) -> String {
        self.to_json_value().dump()
    }

    /// Translate a JSON object into a SIP Message.
    ///
    /// Field and FixedField values must be JSON strings or numbers.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// use sip2::spec;
    /// use json;
    ///
    /// let expected = Message::new(
    ///     &spec::M_LOGIN,
    ///     vec![
    ///         FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
    ///         FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
    ///     ],
    ///     vec![
    ///         Field::new(spec::F_LOGIN_UID.code, "sip_username"),
    ///         Field::new(spec::F_LOGIN_PWD.code, "sip_password"),
    ///     ]
    /// );
    ///
    /// let json_val = json::object!{
    ///   "code":"93",
    ///   "fixed_fields":["0",0],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]};
    ///
    /// let msg = Message::from_json_value(json_val).unwrap();
    ///
    /// assert_eq!(expected, msg);
    ///
    /// let m = Message::from_json_value(json::object! {"code":"93","fixed_fields":[{"bad":"news"}]});
    /// assert!(m.is_err());
    /// ```
    pub fn from_json_value(mut json_value: json::JsonValue) -> Result<Message, SipJsonError> {
        // Start with a message that's just the code plus fixed fields
        // as a SIP string.
        let mut strbuf = json_value["code"].take_string().ok_or_else(|| {
            SipJsonError::MessageFormatError("Message requires a code".to_string())
        })?;

        for ff in json_value["fixed_fields"].members() {
            if let Some(s) = ff.as_str() {
                strbuf += s;
            } else if ff.is_number() {
                strbuf += &format!("{ff}");
            } else {
                return Err(SipJsonError::MessageFormatError(format!(
                    "Fixed field values must be JSON strings or numbers: {}",
                    ff.dump()
                )));
            }
        }

        // Since we're creating this partial SIP string from raw
        // JSON values and the buffer this far should not contain
        // any separater chars, clean it up before parsing as SIP.
        strbuf = super::util::sip_string(&strbuf);

        let mut msg = Message::from_sip(&strbuf).map_err(|e| {
            SipJsonError::MessageFormatError(format!(
                "Message is not correctly formatted: {e} {}",
                json_value.dump()
            ))
        })?;

        for field in json_value["fields"].members() {
            for (code, value) in field.entries() {
                if let Some(s) = value.as_str() {
                    msg.add_field(code, s);
                } else if value.is_number() {
                    msg.add_field(code, &format!("{value}"));
                } else {
                    return Err(SipJsonError::MessageFormatError(format!(
                        "Message is not correctly formatted: {}",
                        json_value.dump()
                    )));
                }
            }
        }

        Ok(msg)
    }

    /// Translate a JSON string into a SIP Message.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// use sip2::spec;
    /// use json;
    ///
    /// let expected = Message::new(
    ///     &spec::M_LOGIN,
    ///     vec![
    ///         FixedField::new(&spec::FF_UID_ALGO, "0").unwrap(),
    ///         FixedField::new(&spec::FF_PWD_ALGO, "0").unwrap(),
    ///     ],
    ///     vec![
    ///         Field::new(spec::F_LOGIN_UID.code, "sip_username"),
    ///         Field::new(spec::F_LOGIN_PWD.code, "sip_password"),
    ///     ]
    /// );
    ///
    /// let json_str = r#"
    ///   {
    ///     "code":"93",
    ///     "fixed_fields":["0","0"],
    ///     "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]
    ///   }
    /// "#;
    ///
    /// let msg = Message::from_json(&json_str).unwrap();
    ///
    /// assert_eq!(expected, msg);
    /// ```
    pub fn from_json(msg_json: &str) -> Result<Message, SipJsonError> {
        let json_value: json::JsonValue = match json::parse(msg_json) {
            Ok(v) => v,
            Err(e) => {
                return Err(SipJsonError::JsonError(e));
            }
        };

        Message::from_json_value(json_value)
    }
}
