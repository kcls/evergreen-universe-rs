use crate::event::EgEvent;
use std::error::Error;
use std::fmt;

// This is just a convenient way to package an optional EgError into
// the esponse of any methods/functions that return this type.
// Result<String, EgError> == EgResult<String>
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

impl From<EgError> for String {
    fn from(err: EgError) -> Self {
        match err {
            EgError::Debug(m) => m.to_string(),
            EgError::Event(e) => e.to_string(),
        }
    }
}

impl From<EgEvent> for EgError {
    fn from(evt: EgEvent) -> Self {
        EgError::Event(evt)
    }
}

impl From<&EgEvent> for EgError {
    fn from(evt: &EgEvent) -> Self {
        EgError::Event(evt.clone())
    }
}
