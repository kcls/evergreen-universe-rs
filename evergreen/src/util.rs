use chrono::prelude::*;
use chrono::DateTime;
use json::Value;

/// We support a variety of true-ish values.
///
/// True if the value is a non-zero number, a string that starts with
/// "t/T", or a json::Value::Bool(true).  False otherwise.
///
/// ```
/// assert!(!evergreen::util::json_bool(&json::from_str(vec!["true"])));
/// assert!(evergreen::util::json_bool(&json::from_str("trooo")));
/// assert!(evergreen::util::json_bool(&json::from_str("1")));
/// assert!(!evergreen::util::json_bool(&json::from_str(0i8)));
/// assert!(!evergreen::util::json_bool(&json::from_str(false)));
/// ```
pub fn json_bool(value: &json::Value) -> bool {
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

/// Translate a number-ish thing into a float.
///
/// Returns an error if the value cannot be numerified.
///
/// ```
/// assert!(evergreen::util::json_float(&json::Value::new_array()).is_err());
///
/// let res = evergreen::util::json_float(&json::from_str("1.2"));
/// assert_eq!(res.unwrap(), 1.2);
///
/// let res = evergreen::util::json_float(&json::from_str(0));
/// assert_eq!(res.unwrap(), 0.0);
/// ```
pub fn json_float(value: &json::Value) -> Result<f64, String> {
    if let Some(n) = value.as_f64() {
        return Ok(n);
    } else if let Some(s) = value.as_str() {
        if let Ok(n) = s.parse::<f64>() {
            return Ok(n);
        }
    }
    Err(format!("Invalid float value: {}", value))
}

/// Translate a number-ish thing into a signed int.
///
/// Returns an error if the value cannot be numerified.
/// ```
/// let res = evergreen::util::json_int(&json::Value::new_array());
/// assert!(res.is_err());
///
/// let res = evergreen::util::json_int(&json::from_str("-11"));
/// assert_eq!(res.unwrap(), -11);
///
/// let res = evergreen::util::json_int(&json::from_str(12));
/// assert_eq!(res.unwrap(), 12);
pub fn json_int(value: &json::Value) -> Result<i64, String> {
    if let Some(n) = value.as_i64() {
        return Ok(n);
    } else if let Some(s) = value.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Ok(n);
        }
    }
    Err(format!("Invalid int value: {}", value))
}

/// Translate a json value into a String.
///
/// Will coerce numeric values into strings.  Return Err if the
/// value is not a string or number.
pub fn json_string(value: &json::Value) -> Result<String, String> {
    if let Some(s) = value.as_str() {
        Ok(s.to_string())
    } else if value.is_number() {
        Ok(format!("{value}"))
    } else {
        Err(format!("Cannot extract value as a string: {value}"))
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
pub fn parse_pg_date(pg_iso_date: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_str(pg_iso_date, "%Y-%m-%dT%H:%M:%S%z")
        .or_else(|e| Err(format!("Invalid expire date: {e} {pg_iso_date}")))
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
