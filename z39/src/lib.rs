#![forbid(unsafe_code)]

pub mod bib1;
pub mod message;
pub mod settings;

pub use settings::Settings;

#[cfg(feature = "server")]
pub mod server;

#[cfg(test)]
mod test;
