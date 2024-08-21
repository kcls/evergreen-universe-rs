#![forbid(unsafe_code)]

pub use editor::Editor;
pub use event::EgEvent;
pub use init::init;
pub use osrf::client::Client;
pub use osrf::conf::Config;
pub use osrf::logging::Logger;
pub use osrf::sclient::HostSettings;
pub use osrf::session::ClientSession;
pub use result::EgError;
pub use result::EgResult;
pub use value::EgValue;

pub const NULL: EgValue = EgValue::Null;

pub mod common;
pub mod constants;
pub mod date;
pub mod db;
pub mod editor;
pub mod event;
pub mod idl;
pub mod idldb;
pub mod init;
pub mod norm;
pub mod osrf;
pub mod result;
pub mod samples;
pub mod script;
pub mod util;
pub mod value;

#[cfg(test)]
mod tests;
