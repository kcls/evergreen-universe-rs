use json;
use std::fmt;

pub struct EgEvent {
    code: isize,
    textcode: String,
    payload: json::JsonValue, // json::JsonValue::Null if empty
    desc: Option<String>,
    debug: Option<String>,
    note: Option<String>,
    servertime: Option<String>,
    ilsperm: Option<String>,
    ilspermloc: isize,
    success: bool,
}

impl fmt::Display for EgEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = format!("Event: {}:{}", self.code, self.textcode);

        if let Some(ref d) = self.desc {
            s = s + " -> " + d;
        }

        if let Some(ref p) = self.ilsperm {
            s = format!("{} {}@{}", s, p, self.ilspermloc);
        }

        if let Some(ref n) = self.note {
            s = s + "\n" + n;
        }

        write!(f, "{}", s)
    }
}

impl EgEvent {
    pub fn new(textcode: &str) -> Self {
        EgEvent {
            code: 0,
            textcode: textcode.to_string(),
            payload: json::JsonValue::Null,
            desc: None,
            debug: None,
            note: None,
            servertime: None,
            ilsperm: None,
            ilspermloc: 0,
            success: textcode.eq("SUCCESS"),
        }
    }

    pub fn code(&self) -> isize {
        self.code
    }

    pub fn textcode(&self) -> &str {
        &self.textcode
    }

    pub fn payload(&self) -> &json::JsonValue {
        &self.payload
    }

    pub fn desc(&self) -> Option<&str> {
        self.desc.as_deref()
    }

    pub fn debug(&self) -> Option<&str> {
        self.debug.as_deref()
    }

    pub fn note(&self) -> Option<&str> {
        self.note.as_deref()
    }

    pub fn servertime(&self) -> Option<&str> {
        self.servertime.as_deref()
    }

    pub fn ilsperm(&self) -> Option<&str> {
        self.ilsperm.as_deref()
    }

    pub fn ilspermloc(&self) -> isize {
        self.ilspermloc
    }

    pub fn success(&self) -> bool {
        self.success
    }

    /// Parses a JsonValue and optionally returns an EgEvent.
    ///
    /// ```
    /// use json;
    /// use evergreen as eg;
    /// use eg::event::EgEvent;
    ///
    /// let jv = json::object! {
    ///     code: json::from(100),
    ///     textcode: json::from("SUCCESS"),
    ///     ilsperm: json::from("STAFF_LOGIN"),
    ///     ilspermloc: 1
    /// };
    ///
    /// let evt = EgEvent::parse(&jv).expect("Event Parsing Failed");
    /// assert!(evt.success());
    ///
    /// assert_eq!(format!("{}", evt), String::from("Event: -1:SUCCESS STAFF_LOGIN@1"));
    ///
    /// let jv2 = json::object! {
    ///     howdy: json::from(123)
    /// };
    ///
    /// let evt_op = EgEvent::parse(&jv2);
    /// assert!(evt_op.is_none());
    /// ```
    pub fn parse(jv: &json::JsonValue) -> Option<EgEvent> {
        if !jv.is_object() {
            return None;
        }

        // textcode is the only required field.
        let textcode = match jv["textcode"].as_str() {
            Some(c) => String::from(c),
            _ => {
                return None;
            }
        };

        let success = textcode.eq("SUCCESS");

        let mut evt = EgEvent {
            code: -1,
            textcode: textcode,
            payload: jv["payload"].clone(),
            desc: None,
            debug: None,
            note: None,
            servertime: None,
            ilsperm: None,
            ilspermloc: -1,
            success: success,
        };

        if let Some(code) = jv["ilsevent"].as_isize() {
            evt.code = code;
        };

        if let Some(permloc) = jv["ilspermloc"].as_isize() {
            evt.ilspermloc = permloc;
        }

        for field in vec!["desc", "debug", "note", "servertime", "ilsperm"] {
            if let Some(value) = jv[field].as_str() {
                let v = String::from(value);
                match field {
                    "desc" => evt.desc = Some(v),
                    "debug" => evt.debug = Some(v),
                    "note" => evt.note = Some(v),
                    "servertime" => evt.servertime = Some(v),
                    "ilsperm" => evt.ilsperm = Some(v),
                    _ => {}
                }
            }
        }

        Some(evt)
    }
}
