use super::app;
use super::message;
use super::session;
use std::fmt;

pub type MethodHandler = fn(
    &mut Box<dyn app::ApplicationWorker>,
    &mut session::ServerSession,
    &message::Method,
) -> Result<(), String>;

#[derive(Debug, Copy, Clone)]
pub enum ParamCount {
    Any,
    Zero,
    Exactly(u8),
    AtLeast(u8),
    Range(u8, u8), // Inclusive
}

impl ParamCount {
    /// Returns true if the number of params provided matches the
    /// number specified by the ParamCount enum.
    ///
    /// ```
    /// use opensrf::method::ParamCount;
    /// assert!(ParamCount::matches(&ParamCount::Any, 0));
    /// assert!(!ParamCount::matches(&ParamCount::Exactly(1), 10));
    /// assert!(ParamCount::matches(&ParamCount::AtLeast(10), 20));
    /// assert!(!ParamCount::matches(&ParamCount::AtLeast(20), 10));
    /// assert!(ParamCount::matches(&ParamCount::Range(4, 6), 5));
    /// ```
    pub fn matches(pc: &ParamCount, count: u8) -> bool {
        match *pc {
            ParamCount::Any => {
                return true;
            }
            ParamCount::Zero => {
                return count == 0;
            }
            ParamCount::Exactly(c) => {
                return count == c;
            }
            ParamCount::AtLeast(c) => {
                return count >= c;
            }
            ParamCount::Range(s, e) => {
                return s <= count && e >= count;
            }
        }
    }
}

impl fmt::Display for ParamCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParamCount::Any => write!(f, "Any"),
            ParamCount::Zero => write!(f, "Zero"),
            ParamCount::Exactly(c) => write!(f, "Exactly {}", c),
            ParamCount::AtLeast(c) => write!(f, "AtLeast {}", c),
            ParamCount::Range(s, e) => write!(f, "Range {}..{}", s, e),
        }
    }
}

/// Simplest possible breakdown of supported parameter base types.
#[derive(Clone, Copy, Debug)]
pub enum ParamDataType {
    String,
    Number,
    Array,
    Object, // JsonValue::Object or other object-y thing
    Boolish,
    Scalar, // Not an Object or Array.
    Any,
}

impl fmt::Display for ParamDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParamDataType::String => write!(f, "String"),
            ParamDataType::Number => write!(f, "Number"),
            ParamDataType::Array => write!(f, "Array"),
            ParamDataType::Object => write!(f, "Object"),
            ParamDataType::Boolish => write!(f, "Boolish"),
            ParamDataType::Scalar => write!(f, "Scalar"),
            ParamDataType::Any => write!(f, "Any"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StaticParam {
    pub name: &'static str,
    pub required: bool,
    pub datatype: ParamDataType,
    pub desc: &'static str,
}

#[derive(Clone, Debug)]
pub struct Param {
    pub name: String,
    pub required: bool,
    pub datatype: ParamDataType,
    pub desc: Option<String>,
}

impl Param {
    pub fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            name: self.name.as_str(),
            required: self.required,
            datatype: self.datatype.to_string(),
            desc: match self.desc.as_ref() {
                Some(d) => d.as_str().into(),
                _ => json::JsonValue::Null,
            }
        }
    }
}

/// A variation of a Method that can be used when creating static
/// method definitions.
pub struct StaticMethod {
    pub name: &'static str,
    pub desc: &'static str,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
    pub params: &'static [StaticParam],
}

impl StaticMethod {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }
    pub fn handler(&self) -> &MethodHandler {
        &self.handler
    }

    /// Translate static method content into proper Method's
    pub fn into_method(&self, api_prefix: &str) -> Method {
        let mut params: Vec<Param> = Vec::new();

        for p in self.params {
            let mut param = Param {
                name: p.name.to_string(),
                required: p.required,
                datatype: p.datatype,
                desc: None,
            };

            if p.desc.ne("") {
                param.desc = Some(p.desc.to_string());
            }

            params.push(param)
        }

        let mut m = Method::new(
            &format!("{}.{}", api_prefix, self.name()),
            self.param_count().clone(),
            self.handler,
        );

        if params.len() > 0 {
            m.params = Some(params);
        }

        if self.desc.len() > 0 {
            m.desc = Some(self.desc.to_string());
        }

        m
    }
}

#[derive(Clone)]
pub struct Method {
    pub name: String,
    pub desc: Option<String>,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
    pub params: Option<Vec<Param>>,
    pub atomic: bool,
}

impl Method {
    pub fn new(name: &str, param_count: ParamCount, handler: MethodHandler) -> Method {
        Method {
            handler,
            param_count,
            params: None,
            desc: None,
            atomic: false,
            name: name.to_string(),
        }
    }

    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }

    pub fn handler(&self) -> MethodHandler {
        self.handler
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    pub fn atomic(&self) -> bool {
        self.atomic
    }
    pub fn set_atomic(&mut self, atomic: bool) {
        self.atomic = atomic;
    }

    pub fn params(&self) -> Option<&Vec<Param>> {
        self.params.as_ref()
    }

    pub fn desc(&self) -> Option<&str> {
        self.desc.as_deref()
    }
    pub fn set_desc(&mut self, desc: &str) {
        self.desc = Some(desc.to_string());
    }
    pub fn add_param(&mut self, param: Param) {
        let params = match self.params.as_mut() {
            Some(p) => p,
            None => {
                self.params = Some(Vec::new());
                self.params.as_mut().unwrap()
            }
        };

        params.push(param);
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let mut pa = json::JsonValue::new_array();
        if let Some(params) = self.params() {
            for param in params {
                pa.push(param.to_json_value()).unwrap();
            }
        }

        json::object! {
            name: self.name(),
            param_count: self.param_count().to_string(),
            params: pa,
            desc: match self.desc() {
                Some(d) => d.into(),
                _ => json::JsonValue::Null,
            }
        }
    }
}
