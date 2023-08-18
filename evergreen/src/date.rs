use crate::result::EgResult;
use chrono::{DateTime, Datelike, Duration, FixedOffset, Local, Months, NaiveDate, TimeZone};
use chrono_tz::Tz;

/// Turn an interval string into a number of seconds.
///
/// Supports a subset of the language, which is typically enough
/// for our use cases.  For better parsing, if needed, we could use
/// (e.g.) https://crates.io/crates/parse_duration
///
/// ```
/// use evergreen::date;
///
/// let seconds = date::interval_to_seconds("02:20:05").expect("Parse OK");
/// assert_eq!(seconds, 8405);
///
/// let seconds = date::interval_to_seconds("1 min 2 seconds").expect("Parse OK");
/// assert_eq!(seconds, 62);
/// ```
pub fn interval_to_seconds(interval: &str) -> Result<i64, String> {
    // Avoid generating the error string until we need it.
    let errstr = || format!("Invalid/unsupported interval string: {interval}");

    let interval = interval.to_lowercase();
    let parts = interval.split(" ").collect::<Vec<&str>>();
    let partcount = parts.len();

    let start = Local::now();
    let mut date = Local::now();
    let mut counter = 0;

    loop {
        if counter == partcount - 1 {
            // Final part of the interval string and it only contains
            // one piece (i.e. no count + value).  Assume it's a simple
            // "hh:mm:ss" string.
            date = add_hms(&parts[counter], date).or_else(|_| Err(errstr()))?;
            break;
        }

        let intvl_count = parts[counter].parse::<i64>().or_else(|_| Err(errstr()))?;

        counter += 1; // move counter to our "interval type" part (e.g. "hours")
        let intvl_type = parts[counter].replace(",", "");

        if intvl_type.starts_with("s") {
            date = date + Duration::seconds(intvl_count);
        } else if intvl_type.starts_with("min") {
            date = date + Duration::minutes(intvl_count);
        } else if intvl_type.starts_with("h") {
            date = date + Duration::hours(intvl_count);
        } else if intvl_type.starts_with("d") {
            date = date + Duration::days(intvl_count);
        } else if intvl_type.starts_with("mon") {
            date = date + Months::new(intvl_count as u32);
        } else if intvl_type.starts_with("y") {
            // No 'Years equivalent
            date = date + Months::new(intvl_count as u32 * 12);
        } else {
            Err(errstr())?;
        }

        counter += 1; // move counter to next chunk

        if counter == partcount {
            break;
        }
    }

    let duration = date - start;

    Ok(duration.num_seconds())
}

fn add_hms(part: &str, mut date: DateTime<Local>) -> Result<DateTime<Local>, String> {
    let errstr = || format!("Invalid/unsupported hh::mm::ss string: {part}");
    let time_parts = part.split(":").collect::<Vec<&str>>();

    let hours = time_parts.get(0).ok_or(errstr())?;
    let minutes = time_parts.get(1).ok_or(errstr())?;
    let seconds = time_parts.get(2).ok_or(errstr())?;

    // Turn the string values into numeric values.
    let hours = hours.parse::<i64>().or_else(|_| Err(errstr()))?;
    let minutes = minutes.parse::<i64>().or_else(|_| Err(errstr()))?;
    let seconds = seconds.parse::<i64>().or_else(|_| Err(errstr()))?;

    date = date + Duration::hours(hours);
    date = date + Duration::minutes(minutes);
    date = date + Duration::seconds(seconds);

    Ok(date)
}

/// Current date/time with a fixed offset matching the local time zone.
pub fn now_local() -> DateTime<FixedOffset> {
    Local::now().into()
}

/// Parse an ISO date string and return a date which retains its original
/// time zone.
///
/// If the datetime string is in the Local timezone, for example, the
/// DateTime value produced will also be in the local timezone.
///
/// ```
/// use evergreen::date;
/// use chrono::{DateTime, FixedOffset, Local};
///
/// let dt = date::parse_datetime("2023-07-11T12:00:00-0200");
/// assert!(dt.is_ok());
///
/// let dt2 = date::parse_datetime("2023-07-11T11:00:00-0300");
/// assert!(dt2.is_ok());
///
/// assert_eq!(dt.unwrap(), dt2.unwrap());
///
/// let dt = date::parse_datetime("2023-07-11");
/// assert!(dt.is_ok());
///
/// let dt = date::parse_datetime("2023-07-11 HOWDY");
/// assert!(dt.is_err());
///
/// ```
pub fn parse_datetime(dt: &str) -> EgResult<DateTime<FixedOffset>> {
    if dt.len() > 10 {
        // Assume its a full date + time
        return match dt.parse::<DateTime<FixedOffset>>() {
            Ok(d) => Ok(d),
            Err(e) => return Err(format!("Could not parse datetime string: {e} {dt}").into()),
        };
    }

    if dt.len() < 10 {
        return Err(format!("Invalid date string: {dt}").into());
    }

    // Assumes it's just a YYYY-MM-DD
    let date = match dt.parse::<NaiveDate>() {
        Ok(d) => d,
        Err(e) => return Err(format!("Could not parse date string: {e} {dt}").into()),
    };

    // If we only have a date, use the local timezone.
    let local_date = match Local
        .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .earliest()
    {
        Some(d) => d,
        None => return Err(format!("Could not parse date string: {dt}").into()),
    };

    Ok(local_date.into())
}

/// Turn a DateTime into the kind of date string we like in these parts.
/// ```
/// use evergreen::date;
/// use chrono::{DateTime, FixedOffset, Local};
/// let dt: DateTime<FixedOffset> = "2023-07-11T12:00:00-0700".parse().unwrap();
/// assert_eq!(date::to_iso(&dt), "2023-07-11T12:00:00-0700");
/// ```
pub fn to_iso(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%FT%T%z").to_string()
}

/// Translate a DateTime into the Local timezone while leaving the
/// DateTime as a FixedOffset DateTime.
/// ```
/// use evergreen::date;
/// use chrono::{DateTime, FixedOffset, Local};
/// let dt: DateTime<FixedOffset> = "2023-07-11T12:00:00-0200".parse().unwrap();
/// let dt2: DateTime<FixedOffset> = date::to_local_timezone_fixed(dt);
///
///
/// assert_eq!(dt2.offset(), Local::now().offset());
///
/// // String output will vary by locale, but the dates will be equivalent.
/// assert_eq!(dt, dt2);
/// ```
pub fn to_local_timezone_fixed(dt: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    let local: DateTime<Local> = dt.into();

    // Translate back to a fixed time zone using our newly
    // acquired local time zone as the offset.
    local.with_timezone(local.offset())
}

/// Apply a timezone to a DateTime value.
///
/// This does not change the date/time, only the lense through which
/// the datetime is interpreted (string representation, hour, day of week, etc.).
///
/// To apply a timezone to a Local or Utc value, just:
/// set_timezone(local_date.into(), "America/New_York");
///
/// ```
/// use evergreen::date;
/// use chrono::{DateTime, FixedOffset};
/// let dt: DateTime<FixedOffset> = "2023-07-11T12:00:00-0400".parse().unwrap();
/// let dt = date::set_timezone(dt, "GMT").unwrap();
/// assert_eq!(date::to_iso(&dt), "2023-07-11T16:00:00+0000");
/// ```
pub fn set_timezone(
    dt: DateTime<FixedOffset>,
    timezone: &str,
) -> Result<DateTime<FixedOffset>, String> {
    if timezone == "local" {
        return Ok(to_local_timezone_fixed(dt));
    }

    // Parse the time zone string.
    let tz: Tz = timezone
        .parse()
        .or_else(|e| Err(format!("Cannot parse timezone: {timezone} {e}")))?;

    let modified = dt.with_timezone(&tz);

    let fixed: DateTime<FixedOffset> = match modified.format("%FT%T%z").to_string().parse() {
        Ok(f) => f,
        Err(e) => Err(format!("Cannot reconstruct date: {modified:?} : {e}"))?,
    };

    Ok(fixed)
}

/// Set the hour/minute/seconds on a DateTime, retaining the original date and timezone.
///
/// (There's gotta be a better way...)
///
/// ```
/// use evergreen::date;
/// use chrono::{DateTime, FixedOffset};
/// let dt: DateTime<FixedOffset> = "2023-07-11T01:25:18-0400".parse().unwrap();
/// let dt = date::set_hms(&dt, 23, 59, 59).unwrap();
/// assert_eq!(date::to_iso(&dt), "2023-07-11T23:59:59-0400");
/// ```
pub fn set_hms(
    date: &DateTime<FixedOffset>,
    hours: u32,
    minutes: u32,
    seconds: u32,
) -> Result<DateTime<FixedOffset>, String> {
    let offset = FixedOffset::from_offset(date.offset());

    let datetime = match date.date_naive().and_hms_opt(hours, minutes, seconds) {
        Some(dt) => dt,
        None => Err(format!("Could not set time to {hours}:{minutes}:{seconds}"))?,
    };

    // and_local_timezone() can return multiples in cases where it's ambiguous.
    let new_date: DateTime<FixedOffset> = match datetime.and_local_timezone(offset).single() {
        Some(d) => d,
        None => Err(format!("Error setting timezone for datetime {datetime:?}"))?,
    };

    Ok(new_date)
}
