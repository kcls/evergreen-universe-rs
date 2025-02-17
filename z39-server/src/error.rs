use std::error;
use std::fmt;
use z39::Z39Error;

use evergreen::result::EgError;

pub type LocalResult<T> = Result<T, LocalError>;

#[derive(Debug)]
pub enum LocalError {
    // Catch-all for internal server errors whose messages we may
    // want to log but do not want to leak to the client.
    Internal(String),
    DecodeError(Z39Error),
    EncodeError(Z39Error),
    ProtocolError(Z39Error),
    NotSupported(String),
    NoSuchSearchIndex(String),
    NoSuchDatabase(String),
    NoSearchTerm(String),
}

impl error::Error for LocalError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::DecodeError(ref e) => Some(e),
            Self::EncodeError(ref e) => Some(e),
            Self::ProtocolError(ref e) => Some(e),
            _ => None
        }
    }
}

impl fmt::Display for LocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Internal(ref m) => write!(f, "{m}"),
            Self::NotSupported(ref e) => write!(f, "NotSupported: {e}"),
            Self::DecodeError(ref e) => write!(f, "DecodeError: {e}"),
            Self::EncodeError(ref e) => write!(f, "EncodeError: {e}"),
            Self::ProtocolError(ref e) => write!(f, "ProtocolError: {e}"),
            Self::NoSuchDatabase(ref e) => write!(f, "NoSuchDatabase: {e}"),
            Self::NoSuchSearchIndex(ref e) => write!(f, "NoSuchSearchIndex: {e}"),
            Self::NoSearchTerm(ref e) => write!(f, "NoSearchTerm: {e}"),
        }
    }
}

impl From<String> for LocalError {
    fn from(msg: String) -> Self {
        LocalError::Internal(msg)
    }
}

impl From<&str> for LocalError {
    fn from(msg: &str) -> Self {
        LocalError::from(msg.to_string())
    }
}

impl From<EgError> for LocalError {
    fn from(err: EgError) -> Self {
        LocalError::from(err.to_string())
    }
}

impl From<Z39Error> for LocalError {
    fn from(e: Z39Error) -> LocalError {
        match e {
            Z39Error::DecodeError(_) => Self::DecodeError(e),
            Z39Error::EncodeError(_) => Self::EncodeError(e),
            Z39Error::ProtocolError(_) => Self::ProtocolError(e),
        }
    }
}
