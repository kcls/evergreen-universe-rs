use crate::result::EgResult;
use json::JsonValue;
use socket2::{Domain, Socket, Type};
use std::collections::HashSet;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;

// Typical value for SOMAXCONN
const CONNECT_TCP_BACKLOG: i32 = 128;

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
pub fn json_float(value: &JsonValue) -> EgResult<f64> {
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
pub fn json_int(value: &JsonValue) -> EgResult<i64> {
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
pub fn json_string(value: &JsonValue) -> EgResult<String> {
    if let Some(s) = value.as_str() {
        Ok(s.to_string())
    } else if value.is_number() {
        Ok(format!("{value}"))
    } else {
        Err(format!("Cannot extract value as a string: {value}").into())
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
///
///    // ...
/// }
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
