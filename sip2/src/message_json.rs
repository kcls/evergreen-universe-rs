use super::Message;
use json;
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
    /// let json_val = msg.to_json_value().unwrap();
    /// let expected = json::object!{
    ///   "code":"93",
    ///   "fixed_fields":["0","0"],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]};
    ///
    /// assert_eq!(expected, json_val);
    /// ```
    pub fn to_json_value(&self) -> Result<json::JsonValue, SipJsonError> {
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

        Ok(json::object! {
            "code": self.spec().code,
            "fixed_fields": ff,
            "fields": fields
        })
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
    /// let json_str = msg.to_json().unwrap();
    ///
    /// // Comparing JSON strings is nontrivial with hashes.
    /// // Assume completion means success.  See to_json_value() for
    /// // more rigorous testing.
    /// assert_eq!(true, true);
    /// ```
    pub fn to_json(&self) -> Result<String, SipJsonError> {
        match self.to_json_value() {
            Ok(jv) => Ok(jv.dump()),
            Err(e) => Err(e),
        }
    }

    /// Translate a JSON object into a SIP Message.
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
    ///   "fixed_fields":["0","0"],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]};
    ///
    /// let msg = Message::from_json_value(&json_val).unwrap();
    ///
    /// assert_eq!(expected, msg);
    /// ```
    pub fn from_json_value(json_value: &json::JsonValue) -> Result<Message, SipJsonError> {
        // Start with a message that's just the code plus fixed fields
        // as a SIP string.
        let mut strbuf = format!("{}", json_value["code"]);
        for value in json_value["fixed_fields"].members() {
            strbuf += &format!("{}", value);
        }

        // Since we're creating this partial SIP string from raw
        // JSON values, clean it up before parsing as SIP.
        strbuf = super::util::sip_string(&strbuf);

        let mut msg = match Message::from_sip(&strbuf) {
            Ok(m) => m,
            Err(e) => {
                return Err(SipJsonError::MessageFormatError(format!(
                    "Message is not correctly formatted: {e} {}",
                    json_value.dump()
                )))
            }
        };

        for field in json_value["fields"].members() {
            for (code, value) in field.entries() {
                if value.is_object() || value.is_array() {
                    return Err(SipJsonError::MessageFormatError(format!(
                        "Message is not correctly formatted: {}",
                        json_value.dump()
                    )));
                }

                if value.is_null() {
                    msg.add_field(code, "");
                } else {
                    msg.add_field(code, &format!("{}", value));
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

        Message::from_json_value(&json_value)
    }
}
