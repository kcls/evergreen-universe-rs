use std::error;
use std::fmt;

/// Errors related to SIP2 Client communication
#[derive(Debug)]
pub enum Error {
    DateFormatError,
    FixedFieldLengthError,
    MessageFormatError,
    UnknownMessageError,
    NetworkError(String),
    NoResponseError,
    MissingParamsError,
}

use self::Error::*;

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DateFormatError => write!(f, "date format error"),
            FixedFieldLengthError => write!(f, "fixed field length error"),
            NetworkError(ref s) => write!(f, "network error: {s}"),
            MessageFormatError => write!(f, "sip message format error"),
            UnknownMessageError => write!(f, "unknown sip message type"),
            NoResponseError => write!(f, "no message was received"),
            MissingParamsError => write!(f, "missing needed parameter values"),
        }
    }
}
