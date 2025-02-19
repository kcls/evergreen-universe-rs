//! # Z39.50 Data Types for ASN.1 Messages and Bib1 Attribute Set Values
#![forbid(unsafe_code)]

pub mod error;
pub mod prefs;
pub mod types;

pub use error::LocalError as Z39Error;

#[cfg(test)]
mod test;
