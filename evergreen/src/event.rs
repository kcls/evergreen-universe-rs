use crate::util;
use chrono::Local;
use json::JsonValue;
use std::fmt;

/// Common argument to API calls that allow for targeted overrides.
#[derive(Debug, PartialEq, Clone)]
pub enum Overrides {
    All,
    Events(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct EgEvent {
    code: isize,
    textcode: String,
    payload: JsonValue, // JsonValue::Null if empty
    desc: Option<String>,
    debug: Option<String>,
    note: Option<String>,
    servertime: Option<String>,
    ilsperm: Option<String>,
    ilspermloc: i64,
    org: Option<i64>,
    /// Some code adds ad-hoc bits to the event proper instead of putting
    /// them into the "payload".
    ad_hoc: Option<JsonValue>,
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

impl From<&EgEvent> for JsonValue {
    fn from(evt: &EgEvent) -> Self {
        let mut obj = json::object! {
            code: evt.code(),
            textcode: evt.textcode(),
            payload: evt.payload().clone(),
            ilspermloc: evt.ilspermloc(),
        };

        if let Some(ad_hoc) = evt.ad_hoc.as_ref() {
            for (k, v) in ad_hoc.entries() {
                obj[k] = v.clone();
            }
        }

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
    /// Create a new event with the provided code.
    pub fn new(textcode: &str) -> Self {
        let servertime = Local::now().to_rfc3339();

        EgEvent {
            code: -1,
            textcode: textcode.to_string(),
            payload: JsonValue::Null,
            desc: None,
            debug: None,
            note: None,
            org: None,
            servertime: Some(servertime),
            ilsperm: None,
            ilspermloc: 0,
            ad_hoc: None,
        }
    }

    /// Create a new SUCCESS event
    pub fn success() -> Self {
        EgEvent::new("SUCCESS")
    }

    pub fn to_json_value(&self) -> JsonValue {
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

    pub fn payload(&self) -> &JsonValue {
        &self.payload
    }
    pub fn set_payload(&mut self, payload: JsonValue) {
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

    pub fn is_success(&self) -> bool {
        self.textcode.eq("SUCCESS")
    }

    pub fn org(&self) -> &Option<i64> {
        &self.org
    }
    pub fn set_org(&mut self, id: i64) {
        self.org = Some(id);
    }

    pub fn ad_hoc(&self) -> Option<&JsonValue> {
        self.ad_hoc.as_ref()
    }

    pub fn set_ad_hoc_value(&mut self, key: &str, value: JsonValue) {
        if self.ad_hoc.is_none() {
            self.ad_hoc = Some(JsonValue::new_object());
        }

        let ad_hoc = self.ad_hoc.as_mut().unwrap();
        ad_hoc[key] = value;
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
    /// assert!(evt.is_success());
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
    pub fn parse(jv: &JsonValue) -> Option<EgEvent> {
        if !jv.is_object() {
            return None;
        }

        // textcode is the only required field.
        let textcode = match jv["textcode"].as_str() {
            Some(c) => String::from(c),
            _ => return None,
        };

        let mut evt = EgEvent::new(&textcode);
        evt.set_payload(jv["payload"].clone());

        if let Some(code) = jv["ilsevent"].as_isize() {
            evt.code = code;
        };

        if let Some(permloc) = jv["ilspermloc"].as_i64() {
            evt.ilspermloc = permloc;
        }

        if let Some(org) = jv["org"].as_i64() {
            evt.org = Some(org);
        }

        let mut ad_hoc = JsonValue::new_object();
        for (field, value) in jv.entries() {
            match field {
                "textcode" | "payload" | "ilsevent" | "ilspermloc" | "org" => {
                    // These are already handled.
                }
                "desc" | "debug" | "note" | "servertime" | "ilsperm" => {
                    // These are well-known string values.
                    if let Some(v) = value.as_str() {
                        match field {
                            "desc" => evt.desc = Some(v.to_string()),
                            "debug" => evt.debug = Some(v.to_string()),
                            "note" => evt.note = Some(v.to_string()),
                            "servertime" => evt.servertime = Some(v.to_string()),
                            "ilsperm" => evt.ilsperm = Some(v.to_string()),
                             _ => {} // shold not happen
                        }
                    }
                }
                // Tack any unknown values onto the ad_hoc blob.
                _ => ad_hoc[field] = value.clone()
            }
        }

        if ad_hoc.len() > 0 {
            evt.ad_hoc = Some(ad_hoc);
        }

        Some(evt)
    }
}
