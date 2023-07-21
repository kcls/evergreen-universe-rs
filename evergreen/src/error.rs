use crate::event::EgEvent;
use std::error::Error;
use std::fmt;
use std::result::Result;

pub type EgResult<T> = Result<T, EgError>;

#[derive(Debug, Clone)]
pub enum EgError {
    /// General error/failure messages that is not linked to an EgEvent.
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

impl<T> From<EgError> for EgResult<T> {
    fn from(err: EgError) -> Self {
        EgResult::Err(err)
    }
}


