use super::util;
use std::fmt;
use serde_json as json;

const DEFAULT_LOCALE: &str = "en-US";
const DEFAULT_TIMEZONE: &str = "America/New_York";
const DEFAULT_API_LEVEL: u8 = 1;
const DEFAULT_INGRESS: &str = "opensrf";
const JSON_NULL: json::Value = json::Value::Null;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MessageType {
    Connect,
    Request,
    Result,
    Status,
    Disconnect,
    Unknown,
}

/// Create a MessageType from the string that would be found in a message.
///
/// ```
/// let mt: opensrf::message::MessageType = "REQUEST".into();
/// assert_eq!(mt, opensrf::message::MessageType::Request);
/// ```
impl From<&str> for MessageType {
    fn from(s: &str) -> Self {
        match s {
            "CONNECT" => MessageType::Connect,
            "REQUEST" => MessageType::Request,
            "RESULT" => MessageType::Result,
            "STATUS" => MessageType::Status,
            "DISCONNECT" => MessageType::Disconnect,
            _ => MessageType::Unknown,
        }
    }
}

/// Create the string that will be used within the serialized message
/// for a given MessageType
///
/// ```
/// let s: &str = opensrf::message::MessageType::Request.into();
/// assert_eq!(s, "REQUEST");
/// ```
impl Into<&'static str> for MessageType {
    fn into(self) -> &'static str {
        match self {
            MessageType::Connect => "CONNECT",
            MessageType::Request => "REQUEST",
            MessageType::Result => "RESULT",
            MessageType::Status => "STATUS",
            MessageType::Disconnect => "DISCONNECT",
            _ => "UNKNOWN",
        }
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: &str = (*self).into();
        write!(f, "{}", s)
    }
}

/// OpenSRF messages have HTTP-like status codes.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MessageStatus {
    Continue = 100,
    Ok = 200,
    Accepted = 202,
    PartialComplete = 204,
    Complete = 205,
    Partial = 206,
    Redirected = 307,
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    MethodNotFound = 404,
    NotAllowed = 405,
    ServiceNotFound = 406,
    Timeout = 408,
    Expfailed = 417,
    InternalServerError = 500,
    NotImplemented = 501,
    ServiceUnavailable = 503,
    VersionNotSupported = 505,
    Unknown,
}

/// Translate a code number into a MessageStatus
///
/// ```
/// let ms: opensrf::message::MessageStatus = 205.into();
/// assert_eq!(ms, opensrf::message::MessageStatus::Complete);
/// ```
impl From<isize> for MessageStatus {
    fn from(num: isize) -> Self {
        match num {
            100 => MessageStatus::Continue,
            200 => MessageStatus::Ok,
            202 => MessageStatus::Accepted,
            204 => MessageStatus::PartialComplete,
            205 => MessageStatus::Complete,
            206 => MessageStatus::Partial,
            307 => MessageStatus::Redirected,
            400 => MessageStatus::BadRequest,
            401 => MessageStatus::Unauthorized,
            403 => MessageStatus::Forbidden,
            404 => MessageStatus::MethodNotFound,
            405 => MessageStatus::NotAllowed,
            406 => MessageStatus::ServiceNotFound,
            408 => MessageStatus::Timeout,
            417 => MessageStatus::Expfailed,
            500 => MessageStatus::InternalServerError,
            501 => MessageStatus::NotImplemented,
            503 => MessageStatus::ServiceUnavailable,
            505 => MessageStatus::VersionNotSupported,
            _ => MessageStatus::Unknown,
        }
    }
}

/// Translate a MessageStatus into its display label
///
/// ```
/// let s: &str = opensrf::message::MessageStatus::Continue.into();
/// assert_eq!(s, "Continue");
/// ```
impl Into<&'static str> for MessageStatus {
    fn into(self) -> &'static str {
        match self {
            MessageStatus::Ok => "OK",
            MessageStatus::Continue => "Continue",
            MessageStatus::Complete => "Request Complete",
            MessageStatus::BadRequest => "Bad Request",
            MessageStatus::Timeout => "Timeout",
            MessageStatus::MethodNotFound => "Method Not Found",
            MessageStatus::NotAllowed => "Not Allowed",
            MessageStatus::ServiceNotFound => "Service Not Found",
            MessageStatus::InternalServerError => "Internal Server Error",
            _ => "See Status Code",
        }
    }
}

impl fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}) {:?}", *self as isize, self)
    }
}

/// The message payload is the core of the message.
#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    Method(Method),
    Result(Result),
    Status(Status),
    NoPayload,
}

impl Payload {
    pub fn to_json_value(&self) -> json::Value {
        match self {
            Payload::Method(pl) => pl.to_json_value(),
            Payload::Result(pl) => pl.to_json_value(),
            Payload::Status(pl) => pl.to_json_value(),
            Payload::NoPayload => json::Value::Null,
        }
    }
}

/// Message envelope containing one or more Messages, routing
/// details, and other metadata.
#[derive(Debug, PartialEq)]
pub struct TransportMessage {
    to: String,
    from: String,
    thread: String,
    osrf_xid: String,
    router_command: Option<String>,
    router_class: Option<String>,
    router_reply: Option<String>,
    body: Vec<Message>,
}

impl TransportMessage {
    pub fn new(to: &str, from: &str, thread: &str) -> Self {
        TransportMessage {
            to: to.to_string(),
            from: from.to_string(),
            thread: thread.to_string(),
            osrf_xid: String::from(""),
            router_command: None,
            router_class: None,
            router_reply: None,
            body: Vec::new(),
        }
    }

    pub fn with_body(to: &str, from: &str, thread: &str, msg: Message) -> Self {
        let mut tm = TransportMessage::new(to, from, thread);
        tm.body.push(msg);
        tm
    }

    pub fn with_body_vec(to: &str, from: &str, thread: &str, msgs: Vec<Message>) -> Self {
        let mut tm = TransportMessage::new(to, from, thread);
        tm.body = msgs;
        tm
    }

    pub fn to(&self) -> &str {
        &self.to
    }

    pub fn set_to(&mut self, to: &str) {
        self.to = to.to_string();
    }

    pub fn from(&self) -> &str {
        &self.from
    }

    pub fn set_from(&mut self, from: &str) {
        self.from = from.to_string();
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }

    pub fn body(&self) -> &Vec<Message> {
        &self.body
    }

    pub fn body_as_mut(&mut self) -> &mut Vec<Message> {
        &mut self.body
    }

    pub fn osrf_xid(&self) -> &str {
        &self.osrf_xid
    }

    pub fn set_osrf_xid(&mut self, xid: &str) {
        self.osrf_xid = xid.to_string()
    }

    pub fn router_command(&self) -> Option<&str> {
        self.router_command.as_deref()
    }

    pub fn set_router_command(&mut self, command: &str) {
        self.router_command = Some(command.to_string());
    }

    pub fn router_class(&self) -> Option<&str> {
        self.router_class.as_deref()
    }

    pub fn set_router_class(&mut self, class: &str) {
        self.router_class = Some(class.to_string());
    }

    pub fn router_reply(&self) -> Option<&str> {
        self.router_reply.as_deref()
    }

    pub fn set_router_reply(&mut self, reply: &str) {
        self.router_reply = Some(reply.to_string());
    }

    /// Create a TransportMessage from a JSON object, consuming the JSON value.
    ///
    /// Returns None if the JSON value cannot be coerced into a TransportMessage.
    pub fn from_json_value(mut json_obj: json::Value) -> Option<Self> {
        let to = match json_obj["to"].as_str() {
            Some(i) => i,
            None => return None,
        };

        let from = match json_obj["from"].as_str() {
            Some(i) => i,
            None => return None,
        };

        let thread = match json_obj["thread"].as_str() {
            Some(i) => i,
            None => return None,
        };

        let mut tmsg = TransportMessage::new(&to, &from, &thread);

        if let Some(xid) = json_obj["osrf_xid"].as_str() {
            tmsg.set_osrf_xid(xid);
        };

        if let Some(rc) = json_obj["router_command"].as_str() {
            tmsg.set_router_command(rc);
        }

        if let Some(rc) = json_obj["router_class"].as_str() {
            tmsg.set_router_class(rc);
        }

        if let Some(rc) = json_obj["router_reply"].as_str() {
            tmsg.set_router_reply(rc);
        }

        let body = json_obj["body"].take();

        if let json::Value::Array(arr) = body {
            for body in arr {
                if let Some(b) = Message::from_json_value(body) {
                    tmsg.body_as_mut().push(b);
                }
            }
        } else {
            // Message body is typically an array, but may be a single
            // body entry.

            if let Some(b) = Message::from_json_value(body) {
                tmsg.body_as_mut().push(b);
            }
        }

        Some(tmsg)
    }

    pub fn to_json_value(&self) -> json::Value {
        let body: Vec<json::Value> = self.body().iter().map(|b| b.to_json_value()).collect();

        let mut obj = json::json!({
            "to": self.to(),
            "from": self.from(),
            "thread": self.thread(),
            "osrf_xid": self.osrf_xid(),
            "body": body,
        });

        if let Some(rc) = self.router_command() {
            obj["router_command"] = rc.into();
        }

        if let Some(rc) = self.router_class() {
            obj["router_class"] = rc.into();
        }

        if let Some(rc) = self.router_reply() {
            obj["router_reply"] = rc.into();
        }

        obj
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    mtype: MessageType,
    thread_trace: usize,
    locale: String,
    timezone: String,
    api_level: u8,
    ingress: String,
    payload: Payload,
    msg_class: String,
}

impl Message {
    pub fn new(mtype: MessageType, thread_trace: usize, payload: Payload) -> Self {
        Message {
            mtype,
            thread_trace,
            payload,
            api_level: DEFAULT_API_LEVEL,
            locale: DEFAULT_LOCALE.to_string(),
            timezone: DEFAULT_TIMEZONE.to_string(),
            ingress: DEFAULT_INGRESS.to_string(),
            msg_class: String::from("osrfMessage"), // Only supported value
        }
    }

    pub fn mtype(&self) -> &MessageType {
        &self.mtype
    }

    pub fn thread_trace(&self) -> usize {
        self.thread_trace
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    pub fn api_level(&self) -> u8 {
        self.api_level
    }

    pub fn set_api_level(&mut self, level: u8) {
        self.api_level = level;
    }

    pub fn locale(&self) -> &str {
        &self.locale
    }

    pub fn set_locale(&mut self, locale: &str) {
        self.locale = locale.to_string()
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn set_timezone(&mut self, timezone: &str) {
        self.timezone = timezone.to_string()
    }

    pub fn ingress(&self) -> &str {
        &self.ingress
    }

    pub fn set_ingress(&mut self, ingress: &str) {
        self.ingress = ingress.to_string()
    }

    /// Creates a Message from a JSON value, consuming the JSON value.
    ///
    /// Returns None if the JSON value cannot be coerced into a Message.
    pub fn from_json_value(json_obj: json::Value) -> Option<Self> {
        let mut msg_wrapper: super::classified::ClassifiedJson =
            match super::classified::ClassifiedJson::declassify(json_obj) {
                Some(sm) => sm,
                None => return None,
            };

        let msg_class = msg_wrapper.class();

        if msg_class != "osrfMessage" {
            log::warn!("Message::from_json_value() unkonown class {}", msg_class);
            return None;
        }

        let mut msg_hash = msg_wrapper.take_json();

        let thread_trace = match util::json_usize(&msg_hash["threadTrace"]) {
            Some(tt) => tt,
            None => {
                log::warn!("Message contains invalid threadTrace: {}", msg_hash.to_string());
                return None;
            }
        };

        let mtype_str = match msg_hash["type"].as_str() {
            Some(s) => s,
            None => return None,
        };

        let mtype: MessageType = mtype_str.into();
        let payload = msg_hash["payload"].take();

        let payload = match Message::payload_from_json_value(mtype, payload) {
            Some(p) => p,
            None => return None,
        };

        let mut msg = Message::new(mtype, thread_trace, payload);

        if let Some(tz) = msg_hash["tz"].as_str() {
            msg.set_timezone(tz);
        }

        if let Some(lc) = msg_hash["locale"].as_str() {
            msg.set_locale(lc);
        }

        if let Some(ing) = msg_hash["ingress"].as_str() {
            msg.set_ingress(ing);
        }

        if let Some(al) = msg_hash["api_level"].as_u64() {
            msg.set_api_level(al as u8);
        }

        Some(msg)
    }

    fn payload_from_json_value(
        mtype: MessageType,
        payload_obj: json::Value,
    ) -> Option<Payload> {
        match mtype {
            MessageType::Request => match Method::from_json_value(payload_obj) {
                Some(method) => Some(Payload::Method(method)),
                _ => None,
            },

            MessageType::Result => match Result::from_json_value(payload_obj) {
                Some(res) => Some(Payload::Result(res)),
                _ => None,
            },

            MessageType::Status => match Status::from_json_value(payload_obj) {
                Some(stat) => Some(Payload::Status(stat)),
                _ => None,
            },

            _ => Some(Payload::NoPayload),
        }
    }

    /// Create a JSON value from a Message.
    pub fn to_json_value(&self) -> json::Value {
        let mtype: &str = self.mtype.into();

        let mut obj = json::json!({
            "threadTrace": self.thread_trace,
            "type": mtype,
            "locale": self.locale(),
            "timezone": self.timezone(),
            "api_level": self.api_level(),
            "ingress": self.ingress(),
        });

        match self.payload {
            // Avoid adding the "payload" key for non-payload messages.
            Payload::NoPayload => {}
            _ => obj["payload"] = self.payload.to_json_value(),
        }

        super::classified::ClassifiedJson::classify(obj, &self.msg_class)
    }
}

/// Delivers a single API response.
///
/// Each Request will have zero or more associated Response messages.
#[derive(Debug, Clone, PartialEq)]
pub struct Result {
    status: MessageStatus,

    status_label: String,

    msg_class: String,

    /// API response value.
    content: json::Value,
}

impl Result {
    pub fn new(
        status: MessageStatus,
        status_label: &str,
        msg_class: &str,
        content: json::Value,
    ) -> Self {
        Result {
            status,
            content,
            msg_class: msg_class.to_string(),
            status_label: status_label.to_string(),
        }
    }

    pub fn content(&self) -> &json::Value {
        &self.content
    }

    pub fn status(&self) -> &MessageStatus {
        &self.status
    }

    pub fn status_label(&self) -> &str {
        &self.status_label
    }

    pub fn from_json_value(json_obj: json::Value) -> Option<Self> {
        let msg_wrapper: super::classified::ClassifiedJson =
            match super::classified::ClassifiedJson::declassify(json_obj) {
                Some(sm) => sm,
                None => return None,
            };

        let msg_class = msg_wrapper.class();
        let msg_hash = msg_wrapper.json();

        let code = match util::json_isize(&msg_hash["statusCode"]) {
            Some(tt) => tt,
            None => {
                log::warn!("Result has invalid status code {}", msg_hash.to_string());
                return None;
            }
        };

        let stat: MessageStatus = code.into();

        // If the message contains a status label, use it, otherwise
        // use the label associated locally with the status code
        let stat_str: &str = msg_hash["status"].as_str().unwrap_or(stat.into());

        Some(Result::new(
            stat,
            stat_str,
            msg_class,
            msg_hash["content"].to_owned(),
        ))
    }

    pub fn to_json_value(&self) -> json::Value {
        let obj = json::json!({
            "status": self.status_label(),
            "statusCode": self.status as isize,
            "content": self.content.clone(),
        });

        super::classified::ClassifiedJson::classify(obj, &self.msg_class)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Status {
    status: MessageStatus,
    status_label: String,
    msg_class: String,
}

impl Status {
    pub fn new(status: MessageStatus, status_label: &str, msg_class: &str) -> Self {
        Status {
            status,
            status_label: status_label.to_string(),
            msg_class: msg_class.to_string(),
        }
    }

    pub fn status(&self) -> &MessageStatus {
        &self.status
    }

    pub fn status_label(&self) -> &str {
        &self.status_label
    }

    pub fn from_json_value(json_obj: json::Value) -> Option<Self> {
        let msg_wrapper: super::classified::ClassifiedJson =
            match super::classified::ClassifiedJson::declassify(json_obj) {
                Some(sm) => sm,
                None => return None,
            };

        let msg_class = msg_wrapper.class();
        let msg_hash = msg_wrapper.json();

        let code = match util::json_isize(&msg_hash["statusCode"]) {
            Some(tt) => tt,
            None => {
                log::warn!("Status has invalid status code {}", msg_hash.to_string());
                return None;
            }
        };

        let stat: MessageStatus = code.into();

        // If the message contains a status label, use it, otherwise
        // use the label associated locally with the status code
        let stat_str: &str = msg_hash["status"].as_str().unwrap_or(stat.into());

        Some(Status::new(stat, stat_str, msg_class))
    }

    pub fn to_json_value(&self) -> json::Value {
        let obj = json::json!({
            "status": self.status_label(),
            "statusCode": self.status as isize,
        });

        super::classified::ClassifiedJson::classify(obj, &self.msg_class)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "stat={} class={} label={}",
            self.status, self.msg_class, self.status_label
        )
    }
}

/// A single API request with method name and parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Method {
    method: String,
    params: Vec<json::Value>,
    msg_class: String,
}

impl Method {
    pub fn new(method: &str, params: Vec<json::Value>) -> Self {
        Method {
            params: params,
            method: String::from(method),
            msg_class: String::from("osrfMethod"), // only supported value
        }
    }

    /// Create a Method from a json::Value.
    pub fn from_json_value(json_obj: json::Value) -> Option<Self> {
        let mut msg_wrapper: super::classified::ClassifiedJson =
            match super::classified::ClassifiedJson::declassify(json_obj) {
                Some(mw) => mw,
                None => return None,
            };

        let mut msg_hash = msg_wrapper.take_json();

        let method = match msg_hash["method"].as_str() {
            Some(m) => m.to_string(),
            None => return None,
        };

        let mut params = Vec::new();
        if let json::Value::Array(vec) = msg_hash["params"].take() {
            params = vec;
        }

        Some(Method {
            method,
            params,
            msg_class: msg_wrapper.class().to_string(),
        })
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn params(&self) -> &Vec<json::Value> {
        &self.params
    }

    /// Return a ref to the param at the specififed index.
    ///
    /// Returns NULL if the param is not set.
    pub fn param(&self, index: usize) -> &json::Value {
        self.params.get(index).unwrap_or(&JSON_NULL)
    }

    /// Create a json::Value from a Method
    pub fn to_json_value(&self) -> json::Value {
        let obj = json::json!({
            "method": self.method(),
            "params": self.params().to_vec(),
        });

        super::classified::ClassifiedJson::classify(obj, &self.msg_class)
    }
}
