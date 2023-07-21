use crate::error::EgError;
use chrono::prelude::*;
use chrono::DateTime;
use json::JsonValue;
use std::collections::HashSet;

/// We support a variety of true-ish values.
///
/// True if the value is a non-zero number, a string that starts with
/// "t/T", or a JsonValue::Bool(true).  False otherwise.
///
/// ```
/// assert!(!evergreen::util::json_bool(&json::from(vec!["true"])));
/// assert!(evergreen::util::json_bool(&json::from("trooo")));
/// assert!(evergreen::util::json_bool(&json::from("1")));
/// assert!(!evergreen::util::json_bool(&json::from(0i8)));
/// assert!(!evergreen::util::json_bool(&json::from(false)));
/// ```
pub fn json_bool(value: &JsonValue) -> bool {
    if let Some(n) = value.as_i64() {
        n != 0
    } else if let Some(n) = value.as_f64() {
        n != 0.0
    } else if let Some(s) = value.as_str() {
        s.len() > 0 && (s[..1].eq("1") || s[..1].eq("t") || s[..1].eq("T"))
    } else if let Some(b) = value.as_bool() {
        b
    } else {
        false
    }
}

/// Same as json_bool, but value is wrapped in an Option.
pub fn json_bool_op(op: Option<&JsonValue>) -> bool {
    if let Some(v) = op {
        json_bool(v)
    } else {
        false
    }
}

/// Translate a number-ish thing into a float.
///
/// Returns an error if the value cannot be numerified.
///
/// ```
/// assert!(evergreen::util::json_float(&json::JsonValue::new_array()).is_err());
///
/// let res = evergreen::util::json_float(&json::from("1.2"));
/// assert_eq!(res.unwrap(), 1.2);
///
/// let res = evergreen::util::json_float(&json::from(0));
/// assert_eq!(res.unwrap(), 0.0);
/// ```
pub fn json_float(value: &JsonValue) -> Result<f64, EgError> {
    if let Some(n) = value.as_f64() {
        return Ok(n);
    } else if let Some(s) = value.as_str() {
        if let Ok(n) = s.parse::<f64>() {
            return Ok(n);
        }
    }
    Err(format!("Invalid float value: {}", value).into())
}

/// Translate a number-ish thing into a signed int.
///
/// Returns an error if the value cannot be numerified.
/// ```
/// let res = evergreen::util::json_int(&json::JsonValue::new_array());
/// assert!(res.is_err());
///
/// let res = evergreen::util::json_int(&json::from("-11"));
/// assert_eq!(res.unwrap(), -11);
///
/// let res = evergreen::util::json_int(&json::from(12));
/// assert_eq!(res.unwrap(), 12);
pub fn json_int(value: &JsonValue) -> Result<i64, EgError> {
    if let Some(n) = value.as_i64() {
        return Ok(n);
    } else if let Some(s) = value.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Ok(n);
        }
    }
    Err(format!("Invalid int value: {}", value).into())
}

/// Translate a json value into a String.
///
/// Will coerce numeric values into strings.  Return Err if the
/// value is not a string or number.
pub fn json_string(value: &JsonValue) -> Result<String, EgError> {
    if let Some(s) = value.as_str() {
        Ok(s.to_string())
    } else if value.is_number() {
        Ok(format!("{value}"))
    } else {
        Err(format!("Cannot extract value as a string: {value}").into())
    }
}

/// Create a DateTime from a Postgres date string.
///
/// chrono has a parse_from_rfc3339() function, but it does
/// not like time zones without colons.  Dates, amiright?
/// ```
/// let res = evergreen::util::parse_pg_date("2023-02-03T12:23:19-0400");
/// assert!(res.is_ok());
///
/// let d = res.unwrap().to_rfc3339();
/// assert_eq!(d, "2023-02-03T12:23:19-04:00");
///
/// let res = evergreen::util::parse_pg_date("2023-02-03T123");
/// assert!(res.is_err());
/// ```
pub fn parse_pg_date(pg_iso_date: &str) -> Result<DateTime<FixedOffset>, EgError> {
    DateTime::parse_from_str(pg_iso_date, "%Y-%m-%dT%H:%M:%S%z")
        .or_else(|e| Err(format!("Invalid expire date: {e} {pg_iso_date}").into()))
}

/// Turns a PG array string (e.g. '{1,23,456}') into a uniq list of ints.
///
/// ```
/// let mut res = evergreen::util::pg_unpack_int_array("{1,23,NULL,23,456}");
/// res.sort();
/// assert_eq!(res, vec![1,23,456]);
/// ```
///
pub fn pg_unpack_int_array(array: &str) -> Vec<i64> {
    array
        .replace("{", "")
        .replace("}", "")
        .split(",")
        .filter_map(|s| {
            // We only care about int-ish things.
            match s.parse::<i64>() {
                Ok(i) => Some(i),
                Err(_) => None,
            }
        })
        .collect::<HashSet<i64>>() // uniquify
        .iter()
        .map(|v| *v) // &i64
        .collect::<Vec<i64>>()
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pager {
    limit: usize,
    offset: usize,
}

impl Pager {
    pub fn new(limit: usize, offset: usize) -> Self {
        Pager { limit, offset }
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn reset(&mut self) {
        self.limit = 0;
        self.offset = 0
    }
}

/// Subtract value b from value a while compensating for common floating
/// point math problems.
pub fn fpdiff(a: f64, b: f64) -> f64 {
    ((a * 100.00) - (b * 100.00)) / 100.00
}

/// Add value b to value a while  compensating for common floating point
/// math problems.
pub fn fpsum(a: f64, b: f64) -> f64 {
    ((a * 100.00) + (b * 100.00)) / 100.00
}
