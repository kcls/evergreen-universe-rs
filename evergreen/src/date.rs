use chrono::{Duration, DateTime, Utc, Months};

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
    let errstr = || format!("Invalid/unsupported interval string: {interval}") ;

    let interval = interval.to_lowercase();
    let parts = interval.split(" ").collect::<Vec<&str>>();
    let partcount = parts.len();

    let start = Utc::now();
    let mut date = Utc::now();
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

fn add_hms(part: &str, mut date: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let errstr = || format!("Invalid/unsupported hh::mm::ss string: {part}") ;
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


