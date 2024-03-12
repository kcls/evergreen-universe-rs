use crate::util;
use crate::{EgResult, EgValue};
use crate::classified::ClassifiedJson;
use std::cell::RefCell;
use std::fmt;

const DEFAULT_TIMEZONE: &str = "America/New_York";
const DEFAULT_API_LEVEL: u8 = 1;
const DEFAULT_INGRESS: &str = "opensrf";
const OSRF_MESSAGE_CLASS: &str = "osrfMessage";
const JSON_NULL: json::JsonValue = json::JsonValue::Null;
const DEFAULT_LOCALE: &str = "en-US";
/// The C code maxes this at 16 chars.
const MAX_LOCALE_LEN: usize = 16;

// Locale is tied to the current thread.
// Initially the locale is set to the default value.
// When parsing an opensrf message that contains a locale value,
// adopt that value as our new thread-scoped locale.
thread_local! {
    static THREAD_LOCALE: RefCell<String> = RefCell::new(DEFAULT_LOCALE.to_string());
}

/// Set the locale for the current thread.
pub fn set_thread_locale(locale: &str) {
    THREAD_LOCALE.with(|lc| {
        // Only verify and allocate if necessary.
        if lc.borrow().as_str() == locale {
            return;
        }

        // Make sure the requested locale is reasonable.

        if locale.len() > MAX_LOCALE_LEN {
            log::error!("Invalid locale: '{locale}'");
            return;
        }

        if locale
            .chars()
            .any(|b| !b.is_ascii_alphabetic() && b != '-' && b != '.')
        {
            log::error!("Invalid locale: '{locale}'");
            return;
        }

        *lc.borrow_mut() = locale.to_string();
    });
}

/// Reset the locale to our default.
pub fn reset_thread_locale() {
    set_thread_locale(DEFAULT_LOCALE);
}

/// Returns the locale for the current thread.
///
/// String clone is required here to escape the temp borrow.
pub fn thread_locale() -> String {
    let mut locale = None;
    THREAD_LOCALE.with(|lc| locale = Some((*lc.borrow()).to_string()));
    locale.unwrap()
}

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
/// let mt: eversrf::message::MessageType = "REQUEST".into();
/// assert_eq!(mt, eversrf::message::MessageType::Request);
/// ```
#[rustfmt::skip]
impl From<&str> for MessageType {
    fn from(s: &str) -> Self {
        match s {
            "CONNECT"    => MessageType::Connect,
            "REQUEST"    => MessageType::Request,
            "RESULT"     => MessageType::Result,
            "STATUS"     => MessageType::Status,
            "DISCONNECT" => MessageType::Disconnect,
            _ => MessageType::Unknown,
        }
    }
}

/// Create the string that will be used within the serialized message
/// for a given MessageType
///
/// ```
/// let s: &str = eversrf::message::MessageType::Request.into();
/// assert_eq!(s, "REQUEST");
/// ```
#[rustfmt::skip]
impl Into<&'static str> for MessageType {
    fn into(self) -> &'static str {
        match self {
            MessageType::Connect    => "CONNECT",
            MessageType::Request    => "REQUEST",
            MessageType::Result     => "RESULT",
            MessageType::Status     => "STATUS",
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
#[rustfmt::skip]
pub enum MessageStatus {
    Continue            = 100,
    Ok                  = 200,
    Accepted            = 202,
    PartialComplete     = 204,
    Complete            = 205,
    Partial             = 206,
    Redirected          = 307,
    BadRequest          = 400,
    Unauthorized        = 401,
    Forbidden           = 403,
    MethodNotFound      = 404,
    NotAllowed          = 405,
    ServiceNotFound     = 406,
    Timeout             = 408,
    Expfailed           = 417,
    InternalServerError = 500,
    NotImplemented      = 501,
    ServiceUnavailable  = 503,
    VersionNotSupported = 505,
    Unknown,
}

/// Translate a code number into a MessageStatus
///
/// ```
/// let ms: eversrf::message::MessageStatus = 205.into();
/// assert_eq!(ms, eversrf::message::MessageStatus::Complete);
/// ```
#[rustfmt::skip]
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
            _   => MessageStatus::Unknown,
        }
    }
}

/// Translate a MessageStatus into its display label
///
/// ```
/// let s: &str = eversrf::message::MessageStatus::Continue.into();
/// assert_eq!(s, "Continue");
/// ```
#[rustfmt::skip]
impl Into<&'static str> for MessageStatus {
    fn into(self) -> &'static str {
        match self {
            MessageStatus::Ok                  => "OK",
            MessageStatus::Continue            => "Continue",
            MessageStatus::Complete            => "Request Complete",
            MessageStatus::BadRequest          => "Bad Request",
            MessageStatus::Timeout             => "Timeout",
            MessageStatus::MethodNotFound      => "Method Not Found",
            MessageStatus::NotAllowed          => "Not Allowed",
            MessageStatus::ServiceNotFound     => "Service Not Found",
            MessageStatus::InternalServerError => "Internal Server Error",
            _                                  => "See Status Code",
        }
    }
}

impl fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}) {:?}", *self as isize, self)
    }
}

impl MessageStatus {
    pub fn is_4xx(&self) -> bool {
        let num = *self as isize;
        num >= 400 && num < 500
    }
    pub fn is_5xx(&self) -> bool {
        let num = *self as isize;
        num >= 500
    }
}

/// The message payload is the core of the message.
#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    Method(MethodCall),
    Result(Result),
    Status(Status),
    NoPayload,
}

impl Payload {
    pub fn to_json_value(&self) -> json::JsonValue {
        match self {
            Payload::Method(pl) => pl.to_json_value(),
            Payload::Result(pl) => pl.to_json_value(),
            Payload::Status(pl) => pl.to_json_value(),
            Payload::NoPayload => json::JsonValue::Null,
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

    pub fn body_mut(&mut self) -> &mut Vec<Message> {
        &mut self.body
    }

    pub fn take_body(&mut self) -> Vec<Message> {
        std::mem::replace(&mut self.body, Vec::new())
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
    pub fn from_json_value(mut json_obj: json::JsonValue) -> EgResult<Self> {
        let err = || format!("Invalid TransportMessage");

        let to = json_obj["to"].as_str().ok_or_else(err)?;
        let from = json_obj["from"].as_str().ok_or_else(err)?;
        let thread = json_obj["thread"].as_str().ok_or_else(err)?;

        let mut tmsg = TransportMessage::new(to, from, thread);

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

        if let json::JsonValue::Array(arr) = body {
            for body in arr {
                tmsg.body_mut().push(Message::from_json_value(body)?);
            }
        } else {
            // Sometimes a transport message body is a single message.
            tmsg.body_mut().push(Message::from_json_value(body)?);
        }

        Ok(tmsg)
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let body: Vec<json::JsonValue> = self.body().iter().map(|b| b.to_json_value()).collect();

        let mut obj = json::object! {
            to: self.to(),
            from: self.from(),
            thread: self.thread(),
            osrf_xid: self.osrf_xid(),
            body: body,
        };

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
    timezone: Option<String>,
    api_level: u8,
    ingress: Option<String>,
    payload: Payload,
}

impl Message {
    pub fn new(mtype: MessageType, thread_trace: usize, payload: Payload) -> Self {
        Message {
            mtype,
            thread_trace,
            payload,
            api_level: DEFAULT_API_LEVEL,
            timezone: None,
            ingress: None,
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
    pub fn payload_mut(&mut self) -> &mut Payload {
        &mut self.payload
    }

    pub fn api_level(&self) -> u8 {
        self.api_level
    }

    pub fn set_api_level(&mut self, level: u8) {
        self.api_level = level;
    }

    pub fn timezone(&self) -> &str {
        self.timezone.as_deref().unwrap_or(DEFAULT_TIMEZONE)
    }

    pub fn set_timezone(&mut self, timezone: &str) {
        self.timezone = Some(timezone.to_string())
    }

    pub fn ingress(&self) -> &str {
        self.ingress.as_deref().unwrap_or(DEFAULT_INGRESS)
    }

    pub fn set_ingress(&mut self, ingress: &str) {
        self.ingress = Some(ingress.to_string())
    }

    /// Creates a Message from a JSON value, consuming the JSON value.
    ///
    /// Returns Err if the JSON value cannot be coerced into a Message.
    pub fn from_json_value(json_obj: json::JsonValue) -> EgResult<Self> {
        let err = || format!("Invalid JSON Message");

        let mut msg_wrapper = ClassifiedJson::declassify(json_obj).ok_or_else(err)?;

        let msg_class = msg_wrapper.class();

        if msg_class != "osrfMessage" {
            return Err(format!("Unknown message class {msg_class}").into());
        }

        let mut msg_hash = msg_wrapper.take_json();

        let thread_trace = util::json_usize(&msg_hash["threadTrace"]).ok_or_else(err)?;

        let mtype_str = msg_hash["type"].as_str().ok_or_else(err)?;

        let mtype: MessageType = mtype_str.into();
        let payload = msg_hash["payload"].take();

        let payload = Message::payload_from_json_value(mtype, payload)?;

        let mut msg = Message::new(mtype, thread_trace, payload);

        if let Some(tz) = msg_hash["tz"].as_str() {
            msg.set_timezone(tz);
        }

        // Any time we receive (parse) a message the contains a locale
        // value, adopt that value as our new thread-scoped locale.
        if let Some(lc) = msg_hash["locale"].as_str() {
            set_thread_locale(lc);
        }

        if let Some(ing) = msg_hash["ingress"].as_str() {
            msg.set_ingress(ing);
        }

        if let Some(al) = msg_hash["api_level"].as_u8() {
            msg.set_api_level(al);
        }

        Ok(msg)
    }

    fn payload_from_json_value(
        mtype: MessageType,
        payload_obj: json::JsonValue,
    ) -> EgResult<Payload> {
        match mtype {
            MessageType::Request => {
                let method = MethodCall::from_json_value(payload_obj)?;
                Ok(Payload::Method(method))
            },

            MessageType::Result => {
                let result = Result::from_json_value(payload_obj)?;
                Ok(Payload::Result(result))
            },

            MessageType::Status => {
                let stat = Status::from_json_value(payload_obj)?;
                Ok(Payload::Status(stat))
            },

            _ => Ok(Payload::NoPayload),
        }
    }

    /// Create a JSON value from a Message.
    pub fn to_json_value(&self) -> json::JsonValue {
        let mtype: &str = self.mtype.into();

        let mut obj = json::object! {
            threadTrace: self.thread_trace,
            type: mtype,
            locale: thread_locale(),
            timezone: self.timezone(),
            api_level: self.api_level(),
            ingress: self.ingress(),
        };

        match self.payload {
            // Avoid adding the "payload" key for non-payload messages.
            Payload::NoPayload => {}
            _ => obj["payload"] = self.payload.to_json_value(),
        }

        super::classified::ClassifiedJson::classify(obj, OSRF_MESSAGE_CLASS)
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
    content: json::JsonValue,
}

impl Result {
    pub fn new(
        status: MessageStatus,
        status_label: &str,
        msg_class: &str,
        content: json::JsonValue,
    ) -> Self {
        Result {
            status,
            content,
            msg_class: msg_class.to_string(),
            status_label: status_label.to_string(),
        }
    }

    pub fn content(&self) -> &json::JsonValue {
        &self.content
    }

    pub fn take_content(&mut self) -> json::JsonValue {
        self.content.take()
    }

    pub fn set_content(&mut self, v: json::JsonValue) {
        self.content = v
    }

    pub fn status(&self) -> &MessageStatus {
        &self.status
    }

    pub fn status_label(&self) -> &str {
        &self.status_label
    }

    pub fn from_json_value(json_obj: json::JsonValue) -> EgResult<Self> {
        let err = || format!("Invalid Result message");

        let mut msg_wrapper = ClassifiedJson::declassify(json_obj).ok_or_else(err)?;

        let mut msg_hash = msg_wrapper.take_json();
        let content = msg_hash["content"].take();

        let code = util::json_isize(&msg_hash["statusCode"]).ok_or_else(err)?;
        let stat: MessageStatus = code.into();

        // If the message contains a status label, use it, otherwise
        // use the label associated locally with the status code
        let stat_str: &str = msg_hash["status"].as_str().unwrap_or(stat.into());

        Ok(Result::new(stat, stat_str, msg_wrapper.class(), content))
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let obj = json::object! {
            status: self.status_label(),
            statusCode: self.status as isize,
            content: self.content.clone(),
        };

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

    pub fn from_json_value(json_obj: json::JsonValue) -> EgResult<Self> {
        let err = || format!("Invalid Status message");

        let msg_wrapper = ClassifiedJson::declassify(json_obj).ok_or_else(err)?;

        let msg_class = msg_wrapper.class();
        let msg_hash = msg_wrapper.json();

        let code = util::json_isize(&msg_hash["statusCode"]).ok_or_else(err)?;
        let stat: MessageStatus = code.into();

        // If the message contains a status label, use it, otherwise
        // use the label associated locally with the status code
        let stat_str: &str = msg_hash["status"].as_str().unwrap_or(stat.into());

        Ok(Status::new(stat, stat_str, msg_class))
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let obj = json::object! {
            status: self.status_label(),
            statusCode: self.status as isize,
        };

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
pub struct MethodCall {
    method: String,
    params: Vec<json::JsonValue>,
    msg_class: String,
}

impl MethodCall {
    pub fn new(method: &str, params: Vec<json::JsonValue>) -> Self {
        MethodCall {
            params: params,
            method: String::from(method),
            msg_class: String::from("osrfMethod"), // only supported value
        }
    }

    /// Create a Method from a JsonValue.
    pub fn from_json_value(json_obj: json::JsonValue) -> EgResult<Self> {
        let err = || format!("Invalid MethodCall message");

        let mut msg_wrapper = ClassifiedJson::declassify(json_obj).ok_or_else(err)?;
        let mut msg_hash = msg_wrapper.take_json();

        let method = msg_hash["method"].as_str().ok_or_else(err)?.to_string();

        let mut params = Vec::new();
        if let json::JsonValue::Array(vec) = msg_hash["params"].take() {
            params = vec;
        }

        Ok(MethodCall {
            method,
            params,
            msg_class: msg_wrapper.class().to_string(),
        })
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn params(&self) -> &Vec<json::JsonValue> {
        &self.params
    }

    pub fn take_params(&mut self) -> Vec<json::JsonValue> {
        std::mem::replace(&mut self.params, Vec::new())
    }

    pub fn set_params(&mut self, params: Vec<json::JsonValue>) {
        self.params = params
    }

    /// Return a ref to the param at the specififed index.
    ///
    /// Returns NULL if the param is not set.
    pub fn param(&self, index: usize) -> &json::JsonValue {
        self.params.get(index).unwrap_or(&JSON_NULL)
    }

    /// Create a JsonValue from a MethodCall
    pub fn to_json_value(&self) -> json::JsonValue {
        let obj = json::object! {
            method: self.method(),
            params: self.params().to_vec(),
        };

        super::classified::ClassifiedJson::classify(obj, &self.msg_class)
    }
}
