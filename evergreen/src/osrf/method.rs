use crate::osrf::app;
use crate::osrf::message;
use crate::osrf::session;
use crate::EgResult;
use crate::EgValue;
use json::JsonValue;
use std::fmt;

pub type MethodHandler = fn(
    &mut Box<dyn app::ApplicationWorker>,
    &mut session::ServerSession,
    message::MethodCall,
) -> EgResult<()>;

#[derive(Debug, Copy, Clone, PartialEq)]
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
    /// use evergreen::osrf::method::ParamCount;
    /// assert!(ParamCount::matches(&ParamCount::Any, 0));
    /// assert!(!ParamCount::matches(&ParamCount::Exactly(1), 10));
    /// assert!(ParamCount::matches(&ParamCount::AtLeast(10), 20));
    /// assert!(!ParamCount::matches(&ParamCount::AtLeast(20), 10));
    /// assert!(ParamCount::matches(&ParamCount::Range(4, 6), 5));
    /// ```
    pub fn matches(pc: &ParamCount, count: u8) -> bool {
        match *pc {
            ParamCount::Any => true,
            ParamCount::Zero => count == 0,
            ParamCount::Exactly(c) => count == c,
            ParamCount::AtLeast(c) => count >= c,
            ParamCount::Range(s, e) => s <= count && e >= count,
        }
    }

    /// Minimum number of parameters required to satisfy this
    /// ParamCount definition.
    pub fn minimum(&self) -> u8 {
        match *self {
            ParamCount::Any => 0,
            ParamCount::Zero => 0,
            ParamCount::Exactly(c) => c,
            ParamCount::AtLeast(c) => c,
            ParamCount::Range(s, _) => s,
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
        let s = match *self {
            ParamDataType::String => "String",
            ParamDataType::Number => "Number",
            ParamDataType::Array => "Array",
            ParamDataType::Object => "Object",
            ParamDataType::Boolish => "Boolish",
            ParamDataType::Scalar => "Scalar",
            ParamDataType::Any => "Any",
        };
        write!(f, "{s}")
    }
}

impl ParamDataType {
    /// True if the provided parameter value matches our type.
    ///
    /// This is a superficial inspection of the parameter type.  E.g.,
    /// we don't care about the contents of an array.
    pub fn matches(&self, param: &EgValue) -> bool {
        match *self {
            ParamDataType::String => param.is_string(),
            ParamDataType::Number => param.is_number(),
            ParamDataType::Array => param.is_array(),
            ParamDataType::Object => param.is_object(),
            ParamDataType::Boolish => {
                param.is_boolean() || param.is_number() || param.is_string() || param.is_null()
            }
            ParamDataType::Scalar => {
                param.is_boolean() || param.is_number() || param.is_string() || param.is_null()
            }
            ParamDataType::Any => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StaticParam {
    pub name: &'static str,
    pub datatype: ParamDataType,
    pub desc: &'static str,
}

#[derive(Clone, Debug)]
pub struct Param {
    pub name: String,
    pub datatype: ParamDataType,
    pub desc: Option<String>,
}

impl Param {
    pub fn to_eg_value(&self) -> EgValue {
        EgValue::from_json_value_plain(json::object! {
            "name": self.name.as_str(),
            "datatype": self.datatype.to_string(),
            "desc": match self.desc.as_ref() {
                Some(d) => d.as_str().into(),
                _ => JsonValue::Null,
            }
        })
    }
}

/// A variation of a Method that can be used when creating static
/// method definitions.
pub struct StaticMethodDef {
    pub name: &'static str,
    pub desc: &'static str,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
    pub params: &'static [StaticParam],
}

impl StaticMethodDef {
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
    pub fn into_method(&self, api_prefix: &str) -> MethodDef {
        let mut params: Vec<Param> = Vec::new();

        for p in self.params {
            let mut param = Param {
                name: p.name.to_string(),
                datatype: p.datatype,
                desc: None,
            };

            if p.desc.ne("") {
                param.desc = Some(p.desc.to_string());
            }

            params.push(param)
        }

        let mut m = MethodDef::new(
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
pub struct MethodDef {
    pub name: String,
    pub desc: Option<String>,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
    pub params: Option<Vec<Param>>,
}

impl MethodDef {
    pub fn new(name: &str, param_count: ParamCount, handler: MethodHandler) -> MethodDef {
        MethodDef {
            handler,
            param_count,
            params: None,
            desc: None,
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

    pub fn to_eg_value(&self) -> EgValue {
        let mut pa = EgValue::new_array();
        if let Some(params) = self.params() {
            for param in params {
                pa.push(param.to_eg_value()).expect("Is Array");
            }
        }

        EgValue::from_json_value_plain(json::object! {
            "api_name": self.name(),
            "argc": self.param_count().to_string(),
            "params": pa.into_json_value(),
            // All Rust methods are streaming.
            "stream": JsonValue::Boolean(true),
            "desc": match self.desc() {
                Some(d) => d.into(),
                _ => JsonValue::Null,
            }
        })
    }

    /// Produces e.g. "foo.bar.baz('param1', 'param2')"
    pub fn to_summary_string(&self) -> String {
        let mut s = format!("{}", self.name());

        match self.param_count {
            ParamCount::Zero => {}
            _ => s += " (",
        }

        if let Some(params) = self.params() {
            let minimum = self.param_count.minimum();
            for (idx, param) in params.iter().enumerate() {
                let required = if idx <= minimum as usize {
                    "*" // required
                } else {
                    ""
                };

                s += &format!("{required}'{}',", param.name);
            }
            s.pop(); // remove trailing ","
        } else if self.param_count == ParamCount::Any {
            s += "..";
        }

        match self.param_count {
            ParamCount::AtLeast(_) => s += ",..",
            _ => {}
        }

        match self.param_count {
            ParamCount::Zero => {}
            _ => s += ")",
        }

        s
    }
}
