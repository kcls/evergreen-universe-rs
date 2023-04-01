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
            ParamCount::Range(s, e) => write!(f, "Between {}..{}", s, e),
        }
    }
}

/// A variation of a Method that can be used when creating static
/// method definitions.
pub struct MethodDef {
    pub name: &'static str,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
}

impl MethodDef {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }
    pub fn handler(&self) -> &MethodHandler {
        &self.handler
    }
}

pub struct Method {
    pub name: String,
    pub param_count: ParamCount,
    pub handler: MethodHandler,
}

impl Method {
    pub fn new(name: &str, param_count: ParamCount, handler: MethodHandler) -> Method {
        Method {
            handler,
            param_count,
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
}
