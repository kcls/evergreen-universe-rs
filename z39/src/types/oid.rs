//! Z39.50 / Bib1 Object Identifiers and Helpers

/// https://oid-base.com/get/1.2.840.10003.5.10
pub const OID_MARC21: [u32; 6] = [1, 2, 840, 10003, 5, 10];

/// https://software.indexdata.com/yaz/doc/list-oids.html
pub const OID_MARCXML: [u32; 7] = [1, 2, 840, 10003, 5, 109, 10];

/// https://oid-base.com/get/1.2.840.10003.3.1
pub const OID_ATTR_SET_BIB1: [u32; 6] = [1, 2, 840, 10003, 3, 1];

/// Local type for rasn::prelude::ObjectIdentifier
pub type ObjectIdentifier = rasn::prelude::ObjectIdentifier;

/// Create a MARC21 ObjectIdentifier;
pub fn for_marc21() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_MARC21).unwrap()
}

/// Create a MARCXML ObjectIdentifier;
pub fn for_marcxml() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_MARCXML).unwrap()
}

/// Create a Bib1 Attribute Set ObjectIdentifier;
pub fn for_bib1() -> ObjectIdentifier {
    ObjectIdentifier::new(&OID_ATTR_SET_BIB1).unwrap()
}

/// True if the provided identifier is a MARC21 ObjectIdentifier.
///
/// # Reference
///
/// * <https://oid-base.com/get/1.2.840.10003.5.10>
///
/// ```
/// use z39::types::oid;
/// assert!(oid::is_marc21_identifier(&oid::for_marc21()));
/// ```
pub fn is_marc21_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_MARC21
}

/// True if the provided identifier is a MARCXML ObjectIdentifier.
///
/// # Reference
///
/// * <https://software.indexdata.com/yaz/doc/list-oids.html>
///
/// ```
/// use z39::types::oid;
/// assert!(oid::is_marcxml_identifier(&oid::for_marcxml()));
/// ```
pub fn is_marcxml_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_MARCXML
}

/// True if the provided identifier is a Bib1 Attribute Set
/// ObjectIdentifier.
///
/// # Reference
///
/// * <https://oid-base.com/get/1.2.840.10003.3.1>
///
/// ```
/// use z39::types::oid;
/// assert!(oid::is_bib1_identifier(&oid::for_bib1()));
/// ```
pub fn is_bib1_identifier(oid: &ObjectIdentifier) -> bool {
    **oid == OID_ATTR_SET_BIB1
}
