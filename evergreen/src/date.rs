use chrono::{DateTime, Duration, FixedOffset, Local, Months};
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

/// Parse an ISO date string and return a date which retains its original
/// time zone.
///
/// If the datetime string is in the Local timezone, for example, the
/// DateTime value produced will also be in the local timezone.
pub fn parse_datetime(dt: &str) -> Result<DateTime<FixedOffset>, String> {
    dt.parse::<DateTime<FixedOffset>>()
        .or_else(|e| Err(format!("Could not parse datetime string: {e} {dt}")))
}

/// Turn a DateTime into the kind of date string we like in these parts.
pub fn to_iso8601(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%FT%T%z").to_string()
}

/// Translate a DateTime into the Local timezone while leaving the
/// DateTime as a FixedOffset DateTime.
pub fn to_local_timezone_fixed(dt: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    let local: DateTime<Local> = dt.into();

    // Translate back to a fixed time zone using our newly
    // acquired local time zone as the offset.
    local.with_timezone(local.offset())
}

/// Apply a timezone to a DateTime value.
///
/// This does not change the date/time, only the lense through which
/// the datetime is interpreted (string represntation, hour, day of week, etc.).
///
/// To apply a timezone to a Local or Utc value, just:
/// set_timezone(local_date.into(), "America/New_York");
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

    // Apply the parsed timezone to the provided date.
    dt.with_timezone(&tz);

    Ok(dt)
}
