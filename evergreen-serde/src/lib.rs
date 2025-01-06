#![forbid(unsafe_code)]

pub use value::EgValue;

pub const NULL: EgValue = EgValue::Null;

pub mod date;
pub mod event;
pub mod idl;
pub mod value;
