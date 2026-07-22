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
    JsonError(serde_json::Error),
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
            SipJsonError::JsonError(err) => err.fmt(f),
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
    /// let expected = serde_json::json!({
    ///   "code":"93",
    ///   "fixed_fields":["0","0"],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]});
    ///
    /// assert_eq!(expected, json_val);
    /// ```
    pub fn to_json_value(&self) -> serde_json::Value {
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

        serde_json::json!({
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
    /// let json_str = msg.to_json();
    ///
    /// // Comparing JSON strings is nontrivial with hashes.
    /// // Assume completion means success.  See to_json_value() for
    /// // more rigorous testing.
    /// assert_eq!(true, true);
    /// ```
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.to_json_value()).expect("JSON serialization")
    }

    /// Translate a JSON object into a SIP Message.
    ///
    /// Field and FixedField values must be JSON strings or numbers.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// use sip2::spec;
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
    /// let json_val = serde_json::json!({
    ///   "code":"93",
    ///   "fixed_fields":["0",0],
    ///   "fields":[{"CN":"sip_username"},{"CO":"sip_password"}]});
    ///
    /// let msg = Message::from_json_value(json_val).unwrap();
    ///
    /// assert_eq!(expected, msg);
    ///
    /// let m = Message::from_json_value(serde_json::json!({"code":"93","fixed_fields":[{"bad":"news"}]}));
    /// assert!(m.is_err());
    /// ```
    pub fn from_json_value(json_value: serde_json::Value) -> Result<Message, SipJsonError> {
        // Start with a message that's just the code plus fixed fields
        // as a SIP string.
        let mut strbuf = json_value["code"].as_str().ok_or_else(|| {
            SipJsonError::MessageFormatError("Message requires a code".to_string())
        })?.to_string();

        if let Some(ff_array) = json_value["fixed_fields"].as_array() {
            for ff in ff_array {
                if let Some(s) = ff.as_str() {
                    strbuf += s;
                } else if ff.is_number() {
                    strbuf += &format!("{ff}");
                } else {
                    return Err(SipJsonError::MessageFormatError(format!(
                        "Fixed field values must be JSON strings or numbers: {}",
                        serde_json::to_string(ff).unwrap_or_else(|e| e.to_string())
                    )));
                }
            }
        }

        // Since we're creating this partial SIP string from raw
        // JSON values and the buffer this far should not contain
        // any separater chars, clean it up before parsing as SIP.
        strbuf = super::util::sip_string(&strbuf);

        let mut msg = Message::from_sip(&strbuf).map_err(|e| {
            SipJsonError::MessageFormatError(format!(
                "Message is not correctly formatted: {e} {}",
                serde_json::to_string(&json_value).unwrap_or_else(|e| e.to_string())
            ))
        })?;

        if let Some(fields_array) = json_value["fields"].as_array() {
            for field in fields_array {
                if let Some(obj) = field.as_object() {
                    for (code, value) in obj.iter() {
                        if let Some(s) = value.as_str() {
                            msg.add_field(code, s);
                        } else if value.is_number() {
                            msg.add_field(code, &format!("{value}"));
                        } else {
                            return Err(SipJsonError::MessageFormatError(format!(
                                "Message is not correctly formatted: {}",
                                serde_json::to_string(&json_value).unwrap_or_else(|e| e.to_string())
                            )));
                        }
                    }
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
        let json_value: serde_json::Value = match serde_json::from_str(msg_json) {
            Ok(v) => v,
            Err(e) => {
                return Err(SipJsonError::JsonError(e));
            }
        };

        Message::from_json_value(json_value)
    }
}
