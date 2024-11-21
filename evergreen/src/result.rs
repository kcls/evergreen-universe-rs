//! Common result type for methods/fuctions which may return a `Result`.

use crate::event::EgEvent;
use std::fmt;

/// This is a convenient way to set the error type to EgError on common
/// method/function responses to simplify the declaration of return types.
/// ```
/// use evergreen::result::*;
/// use evergreen::event::*;
///
/// let res = EgResult::Ok("Hello");
/// assert_eq!(res.unwrap(), "Hello");
///
/// fn foo1() -> EgResult<()> {
///   let evt = EgEvent::new("PROBLEM");
///   let err = EgError::from_event(evt);
///   Err(err)
/// }
///
/// // Same result as above.
/// fn foo2() -> EgResult<()> {
///   Err(EgEvent::new("PROBLEM").into())
/// }
///
/// // Same result as above
/// fn foo3() -> EgResult<()> {
///   Err(EgEvent::new("PROBLEM"))?;
///   Ok(())
/// }
///
/// if let EgError::Event(e) = foo1().err().unwrap() {
///     assert_eq!(e.textcode(), "PROBLEM");
/// } else {
///     panic!("unexpected response");
/// }
///
/// if let EgError::Event(e) = foo2().err().unwrap() {
///     assert_eq!(e.textcode(), "PROBLEM");
/// } else {
///     panic!("unexpected response");
/// }
///
/// if let EgError::Event(e) = foo3().err().unwrap() {
///     assert_eq!(e.textcode(), "PROBLEM");
/// } else {
///     panic!("unexpected response");
/// }
///
/// ```
pub type EgResult<T> = std::result::Result<T, EgError>;

#[derive(Debug, Clone)]
pub enum EgError {
    /// General error/failure messages that is not linked to an EgEvent.
    ///
    /// For one thing, this is useful for encapsulating OpenSRF's generic
    /// fatal error strings.
    Debug(Box<String>),
    Event(Box<EgEvent>),
}

impl std::error::Error for EgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl EgError {
    /// Coerce the EgError into an EgEvent regardless of its internal
    /// type.
    ///
    /// If the error is a Debug(string) type, return a new
    /// INTERNAL_SERVER_ERROR event containing the error string.
    /// Otherwise, return a copy of the contained event.
    pub fn event_or_default(&self) -> EgEvent {
        match self {
            EgError::Event(e) => *e.clone(),
            EgError::Debug(s) => {
                let mut evt = EgEvent::new("INTERNAL_SERVER_ERROR");
                // This is for debug purposes only -- i18n not needed.
                evt.set_desc(&format!("Server Error: {s}"));
                evt
            }
        }
    }

    pub fn from_event(e: EgEvent) -> EgError {
        Self::Event(Box::new(e))
    }

    pub fn from_string(s: String) -> EgError {
        Self::Debug(Box::new(s))
    }
}

impl fmt::Display for EgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Debug(ref m) => write!(f, "{m}"),
            Self::Event(ref e) => write!(f, "{e}"),
        }
    }
}

/// Useful for translating generic OSRF Err(String)'s into EgError's
impl From<String> for EgError {
    fn from(msg: String) -> Self {
        EgError::from_string(msg)
    }
}

impl From<&str> for EgError {
    fn from(msg: &str) -> Self {
        EgError::from_string(msg.to_string())
    }
}

/// Useful for translating EgError's into plain strings for
/// methods/functions that return vanilla Result<T, String>, like
/// OpenSRF published APIs
impl From<EgError> for String {
    fn from(err: EgError) -> Self {
        match err {
            EgError::Debug(m) => m.to_string(),
            EgError::Event(e) => e.to_string(),
        }
    }
}

/// Useful for translating EgEvents that are returned as Err's into
/// fully-fledged Err(EgError) responses.
impl From<EgEvent> for EgError {
    fn from(evt: EgEvent) -> Self {
        EgError::from_event(evt)
    }
}

/// Postgres errors can also be converted into EgErrors.  This allows you
/// to use the question mark operator `?` to propagate postgres errors up
/// the stack as EgErrors.
/// # Example
/// ```
/// fn connect_to_a_nonexistant_db() -> evergreen::EgResult<()> {
///   postgres::Client::connect("bad-bad-connection-string", postgres::NoTls)?;
///   Ok(())
/// }
///
/// let result = connect_to_a_nonexistant_db();
/// assert!(result.err().unwrap().to_string().contains("invalid connection string"));
/// ```
impl From<postgres::Error> for EgError {
    fn from(original: postgres::Error) -> Self {
        EgError::from_string(original.to_string())
    }
}

/// ```
/// use evergreen::event::*;
/// use evergreen::result::*;
///
/// fn foo() -> Result<(), EgError> {
///     let evt = EgEvent::new("PROBLEM");
///     Err(evt.into())
/// }
///
/// if let Err(e) = foo() {
///     if let EgError::Event(ee) = e {
///         assert_eq!(ee.textcode(), "PROBLEM");
///     } else {
///         panic!("Unexpected EgError type: {}", e);
///     }
/// } else {
///     panic!("Unexpected result type");
/// }
/// ```
impl From<&EgEvent> for EgError {
    fn from(evt: &EgEvent) -> Self {
        EgError::from_event(evt.clone())
    }
}
