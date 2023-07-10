use chrono::Local;
use json;
use std::fmt;

#[derive(Debug, Clone)]
pub struct EgEvent {
    code: isize,
    textcode: String,
    payload: json::JsonValue, // json::JsonValue::Null if empty
    desc: Option<String>,
    debug: Option<String>,
    note: Option<String>,
    servertime: Option<String>,
    ilsperm: Option<String>,
    ilspermloc: i64,
    success: bool,
    org: Option<i64>,
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

impl From<&EgEvent> for json::JsonValue {
    fn from(evt: &EgEvent) -> Self {
        let mut obj = json::object! {
            code: evt.code(),
            success: evt.success(),
            textcode: evt.textcode(),
            payload: evt.payload().clone(),
            ilspermloc: evt.ilspermloc(),
        };

        if let Some(v) = evt.desc() {
            obj["desc"] = json::from(v);
        }
        if let Some(v) = evt.debug() {
            obj["debug"] = json::from(v);
        }
        if let Some(v) = evt.note() {
            obj["note"] = json::from(v);
        }
        if let Some(v) = evt.org() {
            obj["org"] = json::from(*v);
        }
        if let Some(v) = evt.servertime() {
            obj["servertime"] = json::from(v);
        }
        if let Some(v) = evt.ilsperm() {
            obj["ilsperm"] = json::from(v);
        }

        obj
    }
}

impl EgEvent {
    pub fn new(textcode: &str) -> Self {
        let servertime = Local::now().to_rfc3339();

        EgEvent {
            code: 0,
            textcode: textcode.to_string(),
            payload: json::JsonValue::Null,
            desc: None,
            debug: None,
            note: None,
            org: None,
            servertime: Some(servertime),
            ilsperm: None,
            ilspermloc: 0,
            success: textcode.eq("SUCCESS"),
        }
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        self.into()
    }

    pub fn set_ils_perm(&mut self, p: &str) {
        self.ilsperm = Some(p.to_string());
    }

    pub fn set_ils_perm_loc(&mut self, loc: i64) {
        self.ilspermloc = loc;
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
    pub fn set_payload(&mut self, payload: json::JsonValue) {
        self.payload = payload
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

    pub fn ilspermloc(&self) -> i64 {
        self.ilspermloc
    }

    pub fn success(&self) -> bool {
        self.success
    }

    pub fn org(&self) -> &Option<i64> {
        &self.org
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
            org: None,
            servertime: None,
            ilsperm: None,
            ilspermloc: -1,
            success: success,
        };

        if let Some(code) = jv["ilsevent"].as_isize() {
            evt.code = code;
        };

        if let Some(permloc) = jv["ilspermloc"].as_i64() {
            evt.ilspermloc = permloc;
        }

        if let Some(org) = jv["org"].as_i64() {
            evt.org = Some(org);
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
