#![forbid(unsafe_code)]

//! Tools for managing MARC21 records and reading/writing records as
//! binary, XML, and MARC breaker.

pub use self::record::Controlfield;
pub use self::record::Field;
pub use self::record::Record;
pub use self::record::Subfield;
pub use self::xml::MARCXML_NAMESPACE;
pub use self::xml::MARCXML_SCHEMA_LOCATION;
pub use self::xml::MARCXML_XSI_NAMESPACE;

pub mod binary;
pub mod breaker;
pub mod record;
pub mod xml;
