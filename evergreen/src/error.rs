use crate::event::EgEvent;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub enum EgError {
    /// General error/failure messages that is not linked to an EgEvent.
    ///
    /// For one thing, this is useful for encapsulating OpenSRF's generic
    /// fatal error strings.
    Message(String),
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
            Self::Message(ref m) => write!(f, "{m}"),
            Self::Event(ref e) => write!(f, "{e}"),
        }
    }
}

/// Useful for translating generic OSRF Err(String)'s into EgError's
impl From<String> for EgError {
    fn from(msg: String) -> Self {
        EgError::Message(msg)
    }
}

impl From<&str> for EgError {
    fn from(msg: &str) -> Self {
        EgError::Message(msg.to_string())
    }
}

impl From<EgError> for String {
    fn from(err: EgError) -> Self {
        match err {
            EgError::Message(m) => m.to_string(),
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
