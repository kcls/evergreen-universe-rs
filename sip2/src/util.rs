//! SIP utility functions
use super::error;
use super::spec;
use chrono::{DateTime, Local};
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
    text.replace("|", "")
}

/// Current date + time in SIP format
pub fn sip_date_now() -> String {
    Local::now().format(spec::SIP_DATE_FORMAT).to_string()
}

/// Transltate an iso8601-ish to SIP format
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
            return Err(error::Error::DateFormatError);
        }
    }
}
