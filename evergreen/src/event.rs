//! Evergreen API Response Events
use crate as eg;
use eg::date;
use eg::EgValue;
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
    payload: EgValue, // EgValue::Null if empty
    desc: Option<String>,
    debug: Option<String>,
    note: Option<String>,
    servertime: Option<String>,
    ilsperm: Option<String>,
    ilspermloc: i64,
    org: Option<i64>,
    /// Some code adds ad-hoc bits to the event proper instead of putting
    /// them into the "payload".
    ad_hoc: Option<EgValue>,
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

impl From<EgEvent> for EgValue {
    fn from(mut evt: EgEvent) -> Self {
        let mut obj: EgValue = eg::hash! {
            "code": evt.code(),
            "textcode": evt.textcode(),
            "payload": evt.payload_mut().take(),
            "ilspermloc": evt.ilspermloc(),
        };

        if let Some(ad_hoc) = evt.ad_hoc.as_mut() {
            for (k, v) in ad_hoc.entries_mut() {
                obj[k] = v.take();
            }
        }

        if let Some(v) = evt.desc() {
            obj["desc"] = v.into();
        }
        if let Some(v) = evt.debug() {
            obj["debug"] = v.into();
        }
        if let Some(v) = evt.note() {
            obj["note"] = v.into();
        }
        if let Some(v) = evt.org() {
            obj["org"] = v.into();
        }
        if let Some(v) = evt.servertime() {
            obj["servertime"] = v.into();
        }
        if let Some(v) = evt.ilsperm() {
            obj["ilsperm"] = v.into();
        }

        obj
    }
}

impl From<&EgEvent> for EgValue {
    fn from(evt: &EgEvent) -> Self {
        EgValue::from(evt.clone())
    }
}

impl EgEvent {
    /// Create a new event with the provided code.
    pub fn new(textcode: &str) -> Self {
        let servertime = date::to_iso(&date::now());

        EgEvent {
            code: -1,
            textcode: textcode.to_string(),
            payload: EgValue::Null,
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

    /// Shorthand for creating an event from a textcode as an EgValue.
    pub fn value(textcode: &str) -> EgValue {
        EgValue::from(EgEvent::new(textcode))
    }

    /// Shorthand for creating a SUCCESS event as an EgValue.
    pub fn success_value() -> EgValue {
        EgValue::from(EgEvent::success())
    }

    /// Create a new SUCCESS event
    pub fn success() -> Self {
        EgEvent::new("SUCCESS")
    }

    pub fn to_value(&self) -> EgValue {
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

    pub fn payload(&self) -> &EgValue {
        &self.payload
    }
    pub fn payload_mut(&mut self) -> &mut EgValue {
        &mut self.payload
    }
    pub fn set_payload(&mut self, payload: EgValue) {
        self.payload = payload
    }

    /// Get the description of the EgEvent
    ///
    /// # Examples
    ///
    /// ```
    /// use evergreen::EgEvent;
    ///
    /// let mut event = EgEvent::new("INTERNAL_SERVER_ERROR");
    /// assert!(event.desc().is_none());
    ///
    /// event.set_desc("Server Error: it did not go well :-(");
    /// let new_description = event.desc();
    /// if let Some(d) = new_description {
    ///   println!("The event is described thusly: {}", d)
    /// }
    /// assert_eq!(new_description, Some("Server Error: it did not go well :-("));
    /// ```
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

    pub fn ad_hoc(&self) -> Option<&EgValue> {
        self.ad_hoc.as_ref()
    }

    pub fn set_desc(&mut self, s: &str) {
        self.desc = Some(s.to_string());
    }

    pub fn set_debug(&mut self, s: &str) {
        self.debug = Some(s.to_string());
    }

    pub fn set_note(&mut self, s: &str) {
        self.note = Some(s.to_string());
    }

    pub fn set_ad_hoc_value(&mut self, key: &str, value: EgValue) {
        if self.ad_hoc.is_none() {
            self.ad_hoc = Some(EgValue::new_object());
        }

        let ad_hoc = self.ad_hoc.as_mut().unwrap();
        ad_hoc[key] = value;
    }

    /// Parses a EgValue and optionally returns an EgEvent.
    ///
    /// ```
    /// use evergreen as eg;
    /// use eg::EgEvent;
    /// use eg::EgValue;
    ///
    /// let jv = eg::hash! {
    ///     code: EgValue::from(100),
    ///     textcode: EgValue::from("SUCCESS"),
    ///     ilsperm: EgValue::from("STAFF_LOGIN"),
    ///     ilspermloc: 1,
    ///     foo: EgValue::from("bar"),
    /// };
    ///
    /// let evt = EgEvent::parse(&jv).expect("Event Parsing Failed");
    /// assert!(evt.is_success());
    ///
    /// assert_eq!(format!("{}", evt), String::from("Event: -1:SUCCESS STAFF_LOGIN@1"));
    /// assert!(evt.ad_hoc().unwrap().has_key("foo"));
    ///
    /// let jv2 = eg::hash! {
    ///     howdy: EgValue::from(123)
    /// };
    ///
    /// let evt_op = EgEvent::parse(&jv2);
    /// assert!(evt_op.is_none());
    /// ```
    pub fn parse(jv: &EgValue) -> Option<EgEvent> {
        if !jv.is_object() || jv.is_blessed() {
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

        let mut ad_hoc = EgValue::new_object();
        for (field, value) in jv.entries() {
            match field {
                "textcode" | "payload" | "ilsevent" | "ilspermloc" | "org" => {
                    // These are already handled above.
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
                _ => ad_hoc[field] = value.clone(),
            }
        }

        if ad_hoc.len() > 0 {
            evt.ad_hoc = Some(ad_hoc);
        }

        Some(evt)
    }
}
