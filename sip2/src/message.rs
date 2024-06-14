use super::error::Error;
use super::spec;
use super::util;
use log::{error, warn};
use std::fmt;

const PASSWORD_REDACTED: &str = "REDACTED";

/// Fixed field with spec and value.
///
/// Since fixed fields have specific length requirements, a well-known
/// spec::FixedField is required
#[derive(PartialEq, Debug)]
pub struct FixedField {
    spec: &'static spec::FixedField,
    value: String,
}

impl FixedField {
    pub fn new(spec: &'static spec::FixedField, value: &str) -> Result<Self, Error> {
        if value.len() == spec.length {
            Ok(FixedField {
                spec,
                value: value.to_string(),
            })
        } else {
            Err(Error::FixedFieldLengthError)
        }
    }

    pub fn spec(&self) -> &'static spec::FixedField {
        self.spec
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn set_value(&mut self, value: &str) -> Result<(), Error> {
        if value.len() == self.spec.length {
            self.value = value.to_string();
            Ok(())
        } else {
            Err(Error::FixedFieldLengthError)
        }
    }

    /// Translate a FixedField into a string which can be inserted into
    /// a SIP message.
    ///
    /// ```
    /// use sip2::FixedField;
    /// use sip2::spec;
    /// let ff = FixedField::new(&spec::FF_MAX_PRINT_WIDTH, "999").unwrap();
    /// assert_eq!(ff.to_sip(), "999");
    /// ```
    pub fn to_sip(&self) -> String {
        util::sip_string(&self.value)
    }
}

/// SIP Field with code and value.
///
/// To support passing field types that are not known at compile time,
/// store the message code instead of a ref to a well-known spec::Field.
#[derive(PartialEq, Debug)]
pub struct Field {
    /// 2-character code
    // Note we could link to the static spec::Field here, like
    // FixedField, instead of storing a copy of the code/label, but that
    // won't work with fields which are unknown until runtime.
    code: String,

    /// Field value
    value: String,
}

impl Field {
    pub fn new(code: &str, value: &str) -> Self {
        Field {
            code: code.to_string(),
            value: value.to_string(),
        }
    }

    /// value getter
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn set_value(&mut self, value: &str) {
        self.value = value.to_string();
    }

    /// code getter
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Create a SIP string from a field
    ///
    /// String includes the trailing "|" delimiter.
    ///
    /// ```
    /// use sip2::Field;
    /// use sip2::spec;
    /// let f = Field::new(spec::F_LOGIN_UID.code, "sip_username");
    /// assert_eq!(f.to_sip(), "CNsip_username|");
    /// ```
    pub fn to_sip(&self) -> String {
        self.code.to_string() + &util::sip_string(&self.value) + &String::from("|")
    }
}

/// SIP message complete with message code, fixed fields, and fields.
#[derive(PartialEq, Debug)]
pub struct Message {
    /// Link to the specification for this message type
    spec: &'static spec::Message,

    /// List of fixed fields
    fixed_fields: Vec<FixedField>,

    /// List of fields
    fields: Vec<Field>,
}

impl Message {
    pub fn new(
        spec: &'static spec::Message,
        fixed_fields: Vec<FixedField>,
        fields: Vec<Field>,
    ) -> Self {
        let mut msg = Message {
            spec,
            fixed_fields,
            fields,
        };

        // Sorting fields allows for consistent message layout,
        // which is useful for debugging purposes.
        msg.sort_fields();

        msg
    }

    pub fn from_code(msg_code: &str) -> Result<Message, Error> {
        Message::from_ff_values(msg_code, &[])
    }

    /// Creates a new message from a set of fixed field values.
    ///
    /// Returns an error if the fixed field values provided are not
    /// the correct length for the specified message type.
    pub fn from_ff_values(msg_code: &str, fixed_fields: &[&str]) -> Result<Message, Error> {
        let msg_spec = match spec::Message::from_code(msg_code) {
            Some(s) => s,
            None => {
                log::error!("Unknown message code: {msg_code}");
                return Err(Error::UnknownMessageError);
            }
        };

        let mut ff: Vec<FixedField> = Vec::new();

        for (idx, ff_spec) in msg_spec.fixed_fields.iter().enumerate() {
            if let Some(v) = fixed_fields.get(idx) {
                ff.push(FixedField::new(ff_spec, v)?);
            }
        }

        if ff.len() != msg_spec.fixed_fields.len() {
            log::warn!(
                "SIP message {} contains incorrect number of fixed fields",
                msg_spec.code
            );
            return Err(Error::MessageFormatError);
        }

        Ok(Message {
            spec: msg_spec,
            fixed_fields: ff,
            fields: Vec::new(),
        })
    }

    /// Create a new message from a list of fixed field and field string values.
    pub fn from_values(
        msg_code: &str,
        fixed_fields: &[&str],
        fields: &[(&str, &str)],
    ) -> Result<Message, Error> {
        let mut msg = Message::from_ff_values(msg_code, fixed_fields)?;
        for field in fields {
            msg.add_field(field.0, field.1);
        }
        Ok(msg)
    }

    /// Keep fields sorted for consistent to_sip output.
    fn sort_fields(&mut self) {
        self.fields.sort_by(|a, b| a.code.cmp(&b.code));
    }

    /// Adds a Field to a message.
    ///
    /// ```
    /// use sip2::{Message, Field};
    /// use sip2::spec;
    ///
    /// let mut msg = Message::new(
    ///     &spec::M_LOGIN,
    ///     vec![],
    ///     vec![],
    /// );
    ///
    /// msg.add_field("ZZ", "ZZ is a value");
    /// assert_eq!(msg.fields()[0].code(), "ZZ");
    /// ```
    pub fn add_field(&mut self, code: &str, value: &str) {
        self.fields.push(Field::new(code, value));
        self.sort_fields();
    }

    /// Adds a field to a SIP message if the provided value is not None.
    pub fn maybe_add_field(&mut self, code: &str, value: Option<&str>) {
        if let Some(v) = value {
            self.fields.push(Field::new(code, v));
            self.sort_fields();
        }
    }

    /// Remove a field by its code.  If 'all' is true, remove all occurrences.
    pub fn remove_field(&mut self, code: &str, all: bool) -> usize {
        let mut count: usize = 0;

        loop {
            let pos = match self.fields.iter().position(|f| f.code().eq(code)) {
                Some(p) => p,
                None => return count, // got them all
            };

            self.fields.remove(pos);

            count += 1;

            if !all {
                return count;
            }
        }
    }

    /// Return the first value with the specified field code.
    pub fn get_field_value(&self, code: &str) -> Option<&str> {
        if let Some(f) = self.fields().iter().find(|f| f.code() == code) {
            Some(f.value.as_str())
        } else {
            None
        }
    }

    pub fn spec(&self) -> &'static spec::Message {
        self.spec
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
    }

    pub fn fixed_fields(&self) -> &Vec<FixedField> {
        &self.fixed_fields
    }

    pub fn fixed_fields_mut(&mut self) -> &mut Vec<FixedField> {
        &mut self.fixed_fields
    }

    /// Create a SIP string of a message.
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
    /// assert_eq!(msg.to_sip(), "9300CNsip_username|COsip_password|");
    /// ```
    pub fn to_sip(&self) -> String {
        let mut s = self.spec.code.to_string();

        for ff in self.fixed_fields.iter() {
            s.push_str(&ff.to_sip());
        }

        for f in self.fields.iter() {
            s.push_str(&f.to_sip());
        }

        s
    }

    /// Same as to_sip() but replaces the patron password 'AD' value
    /// with redacted text.
    ///
    /// Useful for logging.
    pub fn to_sip_redacted(&self) -> String {
        let mut s = self.spec.code.to_string();

        for ff in self.fixed_fields.iter() {
            s.push_str(&ff.to_sip());
        }

        for f in self.fields.iter() {
            if f.code() == spec::F_PATRON_PWD.code {
                s += f.code();
                s += PASSWORD_REDACTED;
                s += "|";
            } else {
                s.push_str(&f.to_sip());
            }
        }

        s
    }

    /// Turns a SIP string into a Message
    ///
    /// Assumes the trailing message terminator character has been removed.
    ///
    /// Message types and Fixed Field types must be known in advance
    /// (see sip2::spec), but Field's do not necessarily have to match
    /// a known spec::Field.  Any value of 3 or more characters will be
    /// treated as a valid field.
    ///
    /// ```
    /// use sip2::{Message, Field, FixedField};
    /// let sip_text = "9300CNsip_username|COsip_password|";
    /// let msg = Message::from_sip(sip_text).unwrap();
    /// assert_eq!(msg.spec().code, "93");
    /// assert_eq!(msg.fields()[0].code(), "CN");
    /// assert_eq!(msg.fields()[1].value(), "sip_password");
    /// ```
    pub fn from_sip(text: &str) -> Result<Message, Error> {
        if text.len() < 2 {
            log::warn!("SIP message is incomplete: {text}");
            return Err(Error::MessageFormatError);
        }

        let msg_spec = match spec::Message::from_code(&text[0..2]) {
            Some(m) => m,
            None => {
                // Message spec must match a known value.
                error!("Unknown message type: {}", &text[0..2]);
                return Err(Error::MessageFormatError);
            }
        };

        let mut msg = Message {
            spec: msg_spec,
            fixed_fields: vec![],
            fields: vec![],
        };

        // Remove the message code
        let mut msg_text = &text[2..];

        for ff_spec in msg_spec.fixed_fields.iter() {
            if msg_text.len() < ff_spec.length {
                // Fixed Fields must match known values.

                warn!(
                    "Message has invalid fixed field: {} : {}",
                    ff_spec.label, msg_text
                );
                return Err(Error::MessageFormatError);
            }

            let value = &msg_text[0..ff_spec.length];
            msg_text = &msg_text[ff_spec.length..];

            // unwrap() is OK because we have confirmed the value has
            // the correct length above.
            msg.fixed_fields
                .push(FixedField::new(ff_spec, value).unwrap());
        }

        // Not all messages have fixed fields and/or fields
        if msg_text.is_empty() {
            return Ok(msg);
        }

        for part in msg_text.split('|') {
            if part.len() > 1 {
                let val = match part.len() > 2 {
                    true => &part[2..],
                    _ => "",
                };
                msg.fields.push(Field::new(&part[0..2], val));
            }
        }

        Ok(msg)
    }
}

/// Message display support for logging / debugging.
impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{} {}", self.spec.code, self.spec.label)?;

        for ff in self.fixed_fields.iter() {
            writeln!(f, "   {:.<35} {}", ff.spec.label, ff.value)?;
        }

        for field in self.fields.iter() {
            if let Some(spec) = spec::Field::from_code(&field.code) {
                writeln!(f, "{} {:.<35} {}", spec.code, spec.label, field.value)?;
            } else {
                writeln!(f, "{} {:.<35} {}", field.code, "custom", field.value)?;
            }
        }

        write!(f, "")
    }
}
