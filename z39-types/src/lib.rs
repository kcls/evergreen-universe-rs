//! # Z39.50 Data Types for ASN.1 Messages and Bib1 Attribute Set Values
#![forbid(unsafe_code)]

pub mod bib1;
pub mod error;
pub mod message;
pub mod settings;

use error::LocalError;
use error::LocalResult;

pub use error::LocalError as Z39Error;
pub use settings::Settings;

#[cfg(test)]
mod test;

/// https://oid-base.com/get/1.2.840.10003.5.10
pub const OID_MARC21: [u32; 6] = [1, 2, 840, 10003, 5, 10];

/// https://software.indexdata.com/yaz/doc/list-oids.html
pub const OID_MARCXML: [u32; 7] = [1, 2, 840, 10003, 5, 109, 10];

/// https://oid-base.com/get/1.2.840.10003.3.1
pub const OID_ATTR_SET_BIB1: [u32; 6] = [1, 2, 840, 10003, 3, 1];

/// Create a new rasn::types::OctetString from a vec of bytes.
pub fn new_octet_string(bytes: Vec<u8>) -> OctetString {
    OctetString::from(bytes)
}

/// Local type for OctetString
pub type OctetString = rasn::prelude::OctetString;

/// Local type for rasn::prelude::ObjectIdentifier
pub type ObjectIdentifier = rasn::prelude::ObjectIdentifier;

/// Get the str form of an OctetString.
pub fn octet_string_as_str(s: &OctetString) -> LocalResult<&str> {
    std::str::from_utf8(s).map_err(|e| LocalError::ProtocolError(e.to_string()))
}

/// Create a MARC21 ObjectIdentifier;
pub fn marc21_identifier() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_MARC21).unwrap()
}

/// Create a MARCXML ObjectIdentifier;
pub fn marcxml_identifier() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_MARCXML).unwrap()
}

/// Create a Bib1 Attribute Set ObjectIdentifier;
pub fn bib1_identifier() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_ATTR_SET_BIB1).unwrap()
}

/// True if the provided identifier is a MARC21 rasn::types::ObjectIdentifier.
///
/// # Reference
///
/// * <https://oid-base.com/get/1.2.840.10003.5.10>
///
/// ```
/// assert!(z39_types::is_marcxml_identifier(&z39_types::marcxml_identifier()));
/// ```
pub fn is_marc21_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_MARC21
}

/// True if the provided identifier is a MARCXML rasn::types::ObjectIdentifier.
///
/// # Reference
///
/// * <https://software.indexdata.com/yaz/doc/list-oids.html>
///
/// ```
/// assert!(z39_types::is_marc21_identifier(&z39_types::marc21_identifier()));
/// ```
pub fn is_marcxml_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_MARCXML
}

/// True if the provided identifier is a Bib1 Attribute Set
/// rasn::types::ObjectIdentifier.
///
/// # Reference
///
/// * <https://oid-base.com/get/1.2.840.10003.3.1>
///
/// ```
/// assert!(z39_types::is_bib1_identifier(&z39_types::bib1_identifier()));
/// ```
pub fn is_bib1_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_ATTR_SET_BIB1
}
