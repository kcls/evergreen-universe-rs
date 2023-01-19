use std::error;
use std::fmt;

/// Errors related to SIP2 Client communication
#[derive(Debug)]
pub enum Error {
    DateFormatError,
    FixedFieldLengthError,
    MessageFormatError,
    UnknownMessageError,
    NetworkError,
    NoResponseError,
    MissingParamsError,
}

use self::Error::*;

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DateFormatError => write!(f, "date format error"),
            FixedFieldLengthError => write!(f, "fixed field length error"),
            NetworkError => write!(f, "network error"),
            MessageFormatError => write!(f, "sip message format error"),
            UnknownMessageError => write!(f, "unknown sip message type"),
            NoResponseError => write!(f, "no message was received"),
            MissingParamsError => write!(f, "missing needed parameter values"),
        }
    }
}
