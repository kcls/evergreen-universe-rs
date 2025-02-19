use std::error;
use std::fmt;

pub type LocalResult<T> = Result<T, LocalError>;

/// Crate-local errors and rasn error wrappers
#[derive(Debug)]
pub enum LocalError {
    DecodeError(rasn::error::DecodeError),
    EncodeError(rasn::error::EncodeError),
    ProtocolError(String),
}

impl error::Error for LocalError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::DecodeError(ref e) => Some(e),
            Self::EncodeError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for LocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::DecodeError(ref e) => write!(f, "{e}"),
            Self::EncodeError(ref e) => write!(f, "{e}"),
            Self::ProtocolError(ref s) => write!(f, "ProtocolError: {s}"),
        }
    }
}
