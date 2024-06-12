use crate::EgResult;
use crate::EgValue;
use json::JsonValue;
use rand::Rng;
use socket2::{Domain, Socket, Type};
use std::collections::HashSet;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub const REDACTED_PARAMS_STR: &str = "**PARAMS REDACTED**";

// Typical value for SOMAXCONN
const CONNECT_TCP_BACKLOG: i32 = 128;

/// Current thread ID as u64.
///
/// Eventually this will not be needed.
/// <https://doc.rust-lang.org/stable/std/thread/struct.ThreadId.html#method.as_u64>
/// <https://github.com/rust-lang/rust/pull/110738>
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
/// Any `size` value that exceeds about 20 will consist wholly of
/// zeroes along the first portion of the string.
///
/// ```
/// use evergreen::util;
/// let n = util::random_number(12);
/// assert_eq!(n.len(), 12);
/// ```
pub fn random_number(size: u8) -> String {
    let mut rng = rand::thread_rng();
    let num: u64 = rng.gen_range(0..std::u64::MAX);
    format!("{:0width$}", num, width = size as usize)[0..size as usize].to_string()
}

/// Converts a JSON number or string to an isize if possible
///
/// ```
/// use evergreen::util;
/// use json;
/// let v = json::from(-123);
/// assert_eq!(util::json_isize(&v), Some(-123));
/// let v = json::from("hello");
/// assert_eq!(util::json_isize(&v), None);
/// ```
pub fn json_isize(value: &JsonValue) -> Option<isize> {
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
/// use evergreen::util;
/// use json;
/// let v = json::from(-123);
/// assert_eq!(util::json_usize(&v), None);
/// let v = json::from("hello");
/// assert_eq!(util::json_usize(&v), None);
/// let v = json::from(12321);
/// assert_eq!(util::json_usize(&v), Some(12321));
/// ```
pub fn json_usize(value: &JsonValue) -> Option<usize> {
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
/// use evergreen::util;
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

/// Creates a (JSON) String verion of a list of method parameters,
/// replacing params with a generic REDACTED message for log-protected
/// methods.
///
/// ```
/// use evergreen::util;
/// let method = "opensrf.system.private.stuff";
/// let log_protect = vec!["opensrf.system.private".to_string()];
/// let params = vec![];
///
/// let s = util::stringify_params(method, &params, &log_protect);
/// assert_eq!(s.as_str(), util::REDACTED_PARAMS_STR);
/// ```
pub fn stringify_params(method: &str, params: &Vec<EgValue>, log_protect: &Vec<String>) -> String {
    // Check if the method should be protected
    let is_protected = log_protect.iter().any(|m| method.starts_with(m));

    if is_protected {
        REDACTED_PARAMS_STR.to_string()
    } else {
        params
            .iter()
            // EgValue.dump() consumes the value, hence the clone.
            .map(|p| p.clone().dump())
            .collect::<Vec<_>>()
            .join(", ")
    }
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

/// Add value b to value a while compensating for common floating point
/// math problems.
pub fn fpsum(a: f64, b: f64) -> f64 {
    ((a * 100.00) + (b * 100.00)) / 100.00
}

/// "check", "create", "delete" a lockfile
pub fn lockfile(path: &str, action: &str) -> EgResult<bool> {
    match action {
        "check" => match Path::new(path).try_exists() {
            Ok(b) => return Ok(b),
            Err(e) => return Err(e.to_string().into()),
        },
        "create" => {
            // create() truncates.  create_new() is still experimental.
            // So check manually first.

            if lockfile(path, "check")? {
                return Err(format!("Lockfile already exists: {path}").into());
            }

            match fs::File::create(path) {
                Ok(_) => return Ok(true),
                Err(e) => return Err(e.to_string().into()),
            }
        }
        "delete" => match fs::remove_file(path) {
            Ok(_) => return Ok(true),
            Err(e) => return Err(e.to_string().into()),
        },
        _ => return Err(format!("Invalid lockfile action: {action}").into()),
    }
}

/// Bind to the provided host:port while applying a read timeout to the
/// TcpListener.
///
/// Applying a timeout to the TcpListener allows TCP servers to
/// periodically stop listening for new connections and perform
/// housekeeping duties (check for signals, etc.)
///
/// If you don't need a read timeout, the standard TcpListener::bind()
/// approach should suffice.
///
/// * `address` - Bind and listen at this address
/// * `port` - Bind and listen at this port.
/// * `read_timeout` - Read timeout in seconds applied to the listening socket.
///
/// Example:
///
///
/// ```text
/// loop {
///    let mut tcp_listener = eg::util::tcp_listener("127.0.0.1", 9898, 5)?;
///
///    let client_stream = match self.tcp_listener.accept() {
///        Ok(stream, _addr) => stream,
///        Err(e) => match e.kind() {
///            std::io::ErrorKind::WouldBlock => {
///                // Read timed out.  This is OK.
///                self.check_for_signals_and_stuff();
///                continue;
///            },
///            _ => {
///                // Some other error occurred.
///                eprintln!("TCP accept error {e}");
///                break;
///            }
///        }
///    }
/// }
/// ```
pub fn tcp_listener(address: &str, port: u16, read_timeout: u64) -> EgResult<TcpListener> {
    let bind = format!("{address}:{port}");

    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
        .or_else(|e| Err(format!("Socket::new() failed with {e}")))?;

    // When we stop/start the service, the address may briefly linger
    // from open (idle) client connections.
    socket
        .set_reuse_address(true)
        .or_else(|e| Err(format!("Error setting reuse address: {e}")))?;

    let address: SocketAddr = bind
        .parse()
        .or_else(|e| Err(format!("Error parsing listen address: {bind}: {e}")))?;

    socket
        .bind(&address.into())
        .or_else(|e| Err(format!("Error binding to address: {bind}: {e}")))?;

    socket
        .listen(CONNECT_TCP_BACKLOG)
        .or_else(|e| Err(format!("Error listending on socket {bind}: {e}")))?;

    // We need a read timeout so we can wake periodically to check
    // for shutdown signals.
    let polltime = Duration::from_secs(read_timeout);

    socket
        .set_read_timeout(Some(polltime))
        .or_else(|e| Err(format!("Error setting socket read_timeout: {e}")))?;

    Ok(socket.into())
}
