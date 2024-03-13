use json;
use crate::EgValue;
use rand::Rng;
use std::thread;
use std::time::{Instant, SystemTime};

pub const REDACTED_PARAMS_STR: &str = "**PARAMS REDACTED**";

/// Current thread ID as u64.
///
/// Eventually this will not be needed.
/// https://doc.rust-lang.org/stable/std/thread/struct.ThreadId.html#method.as_u64
/// https://github.com/rust-lang/rust/pull/110738
pub fn thread_id() -> u64 {
    // "Thread(123)"
    let id = format!("{:?}", thread::current().id());
    let mut parts = id.split(&['(', ')']);

    if let Some(id) = parts.nth(1) {
        if let Ok(idnum) = id.parse::<u64>() {
            return idnum;
        }
    }

    return 0;
}

/// Returns a string of random numbers of the requested length
///
/// ```
/// use eversrf::util;
/// let n = util::random_number(12);
/// assert_eq!(n.len(), 12);
/// let n = util::random_number(100);
/// assert_eq!(n.len(), 100);
/// ```
pub fn random_number(size: usize) -> String {
    let mut rng = rand::thread_rng();
    let num: u64 = rng.gen_range(100_000_000_000..1_000_000_000_000);
    format!("{:0width$}", num, width = size)[0..size].to_string()
}

/// Converts a JSON number or string to an isize if possible
///
/// ```
/// use eversrf::util;
/// use json;
/// let v = json::from(-123);
/// assert_eq!(util::json_isize(&v), Some(-123));
/// let v = json::from("hello");
/// assert_eq!(util::json_isize(&v), None);
/// ```
pub fn json_isize(value: &json::JsonValue) -> Option<isize> {
    if let Some(i) = value.as_isize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<isize>() {
            return Some(i2);
        }
    };

    None
}

/// Converts a JSON number or string to an usize if possible
/// ```
/// use eversrf::util;
/// use json;
/// let v = json::from(-123);
/// assert_eq!(util::json_usize(&v), None);
/// let v = json::from("hello");
/// assert_eq!(util::json_usize(&v), None);
/// let v = json::from(12321);
/// assert_eq!(util::json_usize(&v), Some(12321));
/// ```
pub fn json_usize(value: &json::JsonValue) -> Option<usize> {
    if let Some(i) = value.as_usize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<usize>() {
            return Some(i2);
        }
    };

    None
}

/// Simple seconds-based countdown timer.
/// ```
/// use eversrf::util;
///
/// let t = util::Timer::new(60);
/// assert!(!t.done());
/// assert!(t.remaining() > 0);
/// assert_eq!(t.duration(), 60);
///
/// let t = util::Timer::new(0);
/// assert!(t.done());
/// assert!(t.remaining() == 0);
/// assert_eq!(t.duration(), 0);
///
/// ```
pub struct Timer {
    /// Duration of this timer in seconds.
    /// Timer is "done" once this many seconds have passed
    /// since start_time.
    duration: i32,

    /// Moment this timer starts.
    start_time: Instant,
}

impl Timer {
    pub fn new(duration: i32) -> Timer {
        Timer {
            duration,
            start_time: Instant::now(),
        }
    }
    pub fn reset(&mut self) {
        self.start_time = Instant::now();
    }
    pub fn remaining(&self) -> i32 {
        self.duration - self.start_time.elapsed().as_secs() as i32
    }
    pub fn duration(&self) -> i32 {
        self.duration
    }
    pub fn done(&self) -> bool {
        self.remaining() <= 0
    }
}

pub fn epoch_secs() -> f64 {
    if let Ok(dur) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        let ms = dur.as_millis();
        ms as f64 / 1000.0
    } else {
        0.0
    }
}

pub fn epoch_secs_str() -> String {
    format!("{:0<3}", epoch_secs())
}

/// Creates a (JSON) String verion of a list of method parameters,
/// replacing params with a generic REDACTED message for log-protected
/// methods.
///
/// ```
/// use eversrf::util;
/// let method = "opensrf.system.private.stuff";
/// let log_protect = vec!["opensrf.system.private".to_string()];
/// let params = vec![];
///
/// let s = util::stringify_params(method, &params, &log_protect);
/// assert_eq!(s.as_str(), util::REDACTED_PARAMS_STR);
/// ```
pub fn stringify_params(
    method: &str,
    params: &Vec<EgValue>,
    log_protect: &Vec<String>,
) -> String {
    if log_protect
        .iter()
        .filter(|m| method.starts_with(&m[..]))
        .next()
        .is_none()
    {
        params
            .iter()
            // EgValue.dump() consumes the value, hence the clone.
            .map(|p| p.clone().dump())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        REDACTED_PARAMS_STR.to_string()
    }
}
