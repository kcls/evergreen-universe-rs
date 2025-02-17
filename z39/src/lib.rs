#![forbid(unsafe_code)]

pub mod bib1;
pub mod error;
pub mod message;
pub mod settings;

pub use error::LocalError as Z39Error;
pub use settings::Settings;

#[cfg(test)]
mod test;
