use crate::event::EgEvent;
use std::error::Error;
use std::fmt;

/// This is a convenient way to set the error type to EgError on common
/// method/function responses to simplify the declaration of return types.
/// ```
/// use evergreen::error::*;
/// use evergreen::event::*;
///
/// let res = EgResult::Ok("Hello");
/// assert_eq!(res.unwrap(), "Hello");
///
/// fn foo1() -> EgResult<()> {
///   let evt = EgEvent::new("PROBLEM");
///   let err = EgError::Event(evt);
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
    Debug(String),
    Event(EgEvent),
}

impl Error for EgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            _ => None,
        }
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
        EgError::Debug(msg)
    }
}

impl From<&str> for EgError {
    fn from(msg: &str) -> Self {
        EgError::Debug(msg.to_string())
    }
}

/// Useful for translating EgError's into plain strings for
/// methods/functions that still return Result<T, String>.
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
        EgError::Event(evt)
    }
}

/// ```
/// use evergreen::event::*;
/// use evergreen::error::*;
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
        EgError::Event(evt.clone())
    }
}
