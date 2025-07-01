#![forbid(unsafe_code)]

//! Tools for managing MARC21 records and reading/writing records as
//! binary, XML, and MARC breaker.
//!
//! # Optional features
//!
//! - **marc21_bibliographic**: convenience methods to get
//!   commonly used data from a MARC21 bibliographic record

#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub use self::record::Controlfield;
pub use self::record::Field;
pub use self::record::Record;
pub use self::record::Subfield;
pub use self::xml::MARCXML_NAMESPACE;
pub use self::xml::MARCXML_SCHEMA_LOCATION;
pub use self::xml::MARCXML_XSI_NAMESPACE;

pub mod binary;
pub mod breaker;
mod query;
pub mod record;
pub mod xml;
