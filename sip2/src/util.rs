//! SIP utility functions
use super::error;
use super::spec;
use chrono::{DateTime, FixedOffset, Local};
use log::error;

/// Clean up a string for inclusion in a SIP message
///
/// ```
/// use sip2::util;
/// let result = util::sip_string("howdy|par|dner");
/// assert_eq!(result, "howdypardner");
/// ```
///
pub fn sip_string(text: &str) -> String {
    text.replace('|', "")
}

/// Current date + time in SIP format
pub fn sip_date_now() -> String {
    Local::now().format(spec::SIP_DATE_FORMAT).to_string()
}

/// Translate an iso8601-ish to SIP format
///
/// NOTE: Evergreen/Postgres dates are not parseable here, because
/// PG does not use colons in its timezone offsets.  You have to
/// use something like this instead:
/// DateTime::parse_from_str(pg_iso_date, "%Y-%m-%dT%H:%M:%S%z")
///
/// ```
/// use sip2::util;
///
/// let date_op = util::sip_date("1996-12-19T16:39:57-08:00");
/// assert_eq!(date_op.is_ok(), true);
///
/// let result = date_op.unwrap();
/// assert_eq!(result, "19961219    163957");
///
/// let date_op2 = util::sip_date("YARP!");
/// assert_eq!(date_op2.is_err(), true);
/// ```
pub fn sip_date(iso_date: &str) -> Result<String, error::Error> {
    match DateTime::parse_from_rfc3339(iso_date) {
        Ok(dt) => Ok(dt.format(spec::SIP_DATE_FORMAT).to_string()),
        Err(s) => {
            error!("Error parsing sip date: {} : {}", iso_date, s);
            Err(error::Error::DateFormatError)
        }
    }
}

/// Same as sip_date(), but starting from a DateTime object.
pub fn sip_date_from_dt(dt: &DateTime<FixedOffset>) -> String {
    dt.format(spec::SIP_DATE_FORMAT).to_string()
}

/// Returns "Y" on true, " " on false.
pub fn space_bool(value: bool) -> &'static str {
    match value {
        true => "Y",
        false => " ",
    }
}

pub fn sip_bool(value: bool) -> &'static str {
    match value {
        true => "Y",
        false => "N",
    }
}

pub fn num_bool(value: bool) -> &'static str {
    match value {
        true => "1",
        false => "0",
    }
}

/// Stringify a number left padded with zeros.
pub fn sip_count4(value: usize) -> String {
    format!("{value:0>4}")
}
