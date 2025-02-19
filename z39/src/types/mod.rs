//! # Z39.50 Data Types for ASN.1 Messages and Bib1 Attribute Set Values

pub mod bib1;
pub mod oid;
pub mod pdu;

use crate::error::LocalError;
use crate::error::LocalResult;

/// Local type alias for OctetString
pub type OctetString = rasn::prelude::OctetString;

/// Get the str form of an OctetString.
pub fn octet_string_as_str(s: &OctetString) -> LocalResult<&str> {
    std::str::from_utf8(s).map_err(|e| LocalError::ProtocolError(e.to_string()))
}
