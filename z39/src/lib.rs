#![forbid(unsafe_code)]

pub mod message;
pub mod settings;

pub use settings::Settings;

#[cfg(test)]
mod test;
