//! SIP2 Specification as a collection of static values.
use std::fmt;

pub const SIP_PROTOCOL_VERSION: &str = "2.00";
pub const LINE_TERMINATOR: &str = "\r";
pub const SIP_DATE_FORMAT: &str = "%Y%m%d    %H%M%S";

/// Fixed field definition with label and field length
#[derive(PartialEq, Debug)]
pub struct FixedField {
    /// For documentation and debugging purposes.
    ///
    /// This value does not appear in any messages.
    pub label: &'static str,

    /// Length of the fixed field.
    ///
    /// Fixed field values are always ASCII, this is essentially
    /// the number of characters in the fixed field.
    pub length: usize,
}

impl fmt::Display for FixedField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ({})", self.label, self.length)
    }
}

/// Field definition with label and 2-character code.
#[derive(PartialEq, Debug)]
pub struct Field {
    /// For documentation and debugging purposes.
    ///
    /// This value does not appear in any messages.
    pub label: &'static str,

    /// 2-Character SIP Field Code
    pub code: &'static str,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.code, self.label)
    }
}

impl Field {
    /// Get a Field from its 2-character code.
    ///
    /// ```
    /// use sip2::spec;
    /// let f = &spec::F_LOGIN_UID;
    /// let f2 = spec::Field::from_code(f.code).unwrap();
    /// assert_eq!(f2.code, f.code);
    /// ```
    pub fn from_code(code: &str) -> Option<&'static Field> {
        match code {
            f if f == F_LOGIN_UID.code => Some(&F_LOGIN_UID),
            f if f == F_LOGIN_PWD.code => Some(&F_LOGIN_PWD),
            f if f == F_PATRON_ID.code => Some(&F_PATRON_ID),
            f if f == F_PATRON_IDENT.code => Some(&F_PATRON_IDENT),
            f if f == F_ITEM_IDENT.code => Some(&F_ITEM_IDENT),
            f if f == F_TERMINAL_PWD.code => Some(&F_TERMINAL_PWD),
            f if f == F_PATRON_PWD.code => Some(&F_PATRON_PWD),
            f if f == F_PERSONAL_NAME.code => Some(&F_PERSONAL_NAME),
            f if f == F_SCREEN_MSG.code => Some(&F_SCREEN_MSG),
            f if f == F_PRINT_LINE.code => Some(&F_PRINT_LINE),
            f if f == F_DUE_DATE.code => Some(&F_DUE_DATE),
            f if f == F_TITLE_IDENT.code => Some(&F_TITLE_IDENT),
            f if f == F_BLOCKED_CARD_MSG.code => Some(&F_BLOCKED_CARD_MSG),
            f if f == F_LIBRARY_NAME.code => Some(&F_LIBRARY_NAME),
            f if f == F_TERMINAL_LOCATION.code => Some(&F_TERMINAL_LOCATION),
            f if f == F_INSTITUTION_ID.code => Some(&F_INSTITUTION_ID),
            f if f == F_CURRENT_LOCATION.code => Some(&F_CURRENT_LOCATION),
            f if f == F_PERMANENT_LOCATION.code => Some(&F_PERMANENT_LOCATION),
            f if f == F_HOLD_ITEMS.code => Some(&F_HOLD_ITEMS),
            f if f == F_OVERDUE_ITEMS.code => Some(&F_OVERDUE_ITEMS),
            f if f == F_CHARGED_ITEMS.code => Some(&F_CHARGED_ITEMS),
            f if f == F_FINE_ITEMS.code => Some(&F_FINE_ITEMS),
            f if f == F_SEQUENCE_NUMBER.code => Some(&F_SEQUENCE_NUMBER),
            f if f == F_CHECKSUM.code => Some(&F_CHECKSUM),
            f if f == F_HOME_ADDRESS.code => Some(&F_HOME_ADDRESS),
            f if f == F_EMAIL_ADDRESS.code => Some(&F_EMAIL_ADDRESS),
            f if f == F_HOME_PHONE.code => Some(&F_HOME_PHONE),
            f if f == F_OWNER.code => Some(&F_OWNER),
            f if f == F_CURRENCY.code => Some(&F_CURRENCY),
            f if f == F_CANCEL.code => Some(&F_CANCEL),
            f if f == F_TRANSACTION_ID.code => Some(&F_TRANSACTION_ID),
            f if f == F_VALID_PATRON.code => Some(&F_VALID_PATRON),
            f if f == F_RENEWED_ITEMS.code => Some(&F_RENEWED_ITEMS),
            f if f == F_UNRENEWED_ITEMS.code => Some(&F_UNRENEWED_ITEMS),
            f if f == F_FEE_ACKNOWLEGED.code => Some(&F_FEE_ACKNOWLEGED),
            f if f == F_START_ITEM.code => Some(&F_START_ITEM),
            f if f == F_END_ITEM.code => Some(&F_END_ITEM),
            f if f == F_QUEUE_POSITION.code => Some(&F_QUEUE_POSITION),
            f if f == F_PICKUP_LOCATION.code => Some(&F_PICKUP_LOCATION),
            f if f == F_RECALL_ITEMS.code => Some(&F_RECALL_ITEMS),
            f if f == F_FEE_TYPE.code => Some(&F_FEE_TYPE),
            f if f == F_FEE_LIMIT.code => Some(&F_FEE_LIMIT),
            f if f == F_FEE_AMOUNT.code => Some(&F_FEE_AMOUNT),
            f if f == F_EXPIRE_DATE.code => Some(&F_EXPIRE_DATE),
            f if f == F_SUPPORTED_MESSAGES.code => Some(&F_SUPPORTED_MESSAGES),
            f if f == F_HOLD_TYPE.code => Some(&F_HOLD_TYPE),
            f if f == F_HOLD_ITEMS_LIMIT.code => Some(&F_HOLD_ITEMS_LIMIT),
            f if f == F_OVERDUE_ITEMS_LIST.code => Some(&F_OVERDUE_ITEMS_LIST),
            f if f == F_CHARGED_ITEMS_LIMIT.code => Some(&F_CHARGED_ITEMS_LIMIT),
            f if f == F_UNAVAIL_HOLD_ITEMS.code => Some(&F_UNAVAIL_HOLD_ITEMS),
            f if f == F_HOLD_QUEUE_LENGTH.code => Some(&F_HOLD_QUEUE_LENGTH),
            f if f == F_FEE_IDENTIFIER.code => Some(&F_FEE_IDENTIFIER),
            f if f == F_ITEM_PROPERTIES.code => Some(&F_ITEM_PROPERTIES),
            f if f == F_SECURITY_INHIBIT.code => Some(&F_SECURITY_INHIBIT),
            f if f == F_RECALL_DATE.code => Some(&F_RECALL_DATE),
            f if f == F_MEDIA_TYPE.code => Some(&F_MEDIA_TYPE),
            f if f == F_SORT_BIN.code => Some(&F_SORT_BIN),
            f if f == F_HOLD_PICKUP_DATE.code => Some(&F_HOLD_PICKUP_DATE),
            f if f == F_LOGIN_USER_ID.code => Some(&F_LOGIN_USER_ID),
            f if f == F_LOCATION_CODE.code => Some(&F_LOCATION_CODE),
            f if f == F_VALID_PATRON_PWD.code => Some(&F_VALID_PATRON_PWD),
            f if f == F_INET_PROFILE.code => Some(&F_INET_PROFILE),
            f if f == F_CALL_NUMBER.code => Some(&F_CALL_NUMBER),
            f if f == F_COLLECTION_CODE.code => Some(&F_COLLECTION_CODE),
            f if f == F_ALERT_TYPE.code => Some(&F_ALERT_TYPE),
            f if f == F_HOLD_PATRON_ID.code => Some(&F_HOLD_PATRON_ID),
            f if f == F_HOLD_PATRON_NAME.code => Some(&F_HOLD_PATRON_NAME),
            f if f == F_DEST_LOCATION.code => Some(&F_DEST_LOCATION),
            f if f == F_PATRON_EXPIRE_DATE.code => Some(&F_PATRON_EXPIRE_DATE),
            f if f == F_PATRON_DOB.code => Some(&F_PATRON_DOB),
            f if f == F_PATRON_CLASS.code => Some(&F_PATRON_CLASS),
            f if f == F_REGISTER_LOGIN.code => Some(&F_REGISTER_LOGIN),
            f if f == F_CHECK_NUMBER.code => Some(&F_CHECK_NUMBER),
            _ => None,
        }
    }
}

/// SIP message definition with 2-character code, label, and
/// fixed fields.
///
/// No attempt is made to specify which spec::Field's are used for
/// each Message since use in the wild varies wildly.
#[derive(PartialEq, Debug)]
pub struct Message {
    /// Two-Character SIP Message Code
    pub code: &'static str,

    /// For documentation and debugging purposes.
    ///
    /// This value does not appear in any messages.
    pub label: &'static str,

    /// Fixed fields used by this message, defined in the order they
    /// appear in the compiled message.
    pub fixed_fields: &'static [&'static FixedField],
}

impl Message {
    /// Maps a message code to a message spec.
    ///
    /// ```
    /// use sip2::spec;
    /// let msg = &spec::M_LOGIN;
    /// let msg2 = spec::Message::from_code(&spec::M_LOGIN.code).unwrap();
    /// assert_eq!(msg2.code, msg.code);
    /// ```
    pub fn from_code(code: &str) -> Option<&'static Message> {
        match code {
            m if m == M_SC_STATUS.code => Some(&M_SC_STATUS),
            m if m == M_ACS_STATUS.code => Some(&M_ACS_STATUS),
            m if m == M_LOGIN.code => Some(&M_LOGIN),
            m if m == M_LOGIN_RESP.code => Some(&M_LOGIN_RESP),
            m if m == M_ITEM_INFO.code => Some(&M_ITEM_INFO),
            m if m == M_ITEM_INFO_RESP.code => Some(&M_ITEM_INFO_RESP),
            m if m == M_PATRON_STATUS.code => Some(&M_PATRON_STATUS),
            m if m == M_PATRON_STATUS_RESP.code => Some(&M_PATRON_STATUS_RESP),
            m if m == M_PATRON_INFO.code => Some(&M_PATRON_INFO),
            m if m == M_PATRON_INFO_RESP.code => Some(&M_PATRON_INFO_RESP),
            m if m == M_CHECKOUT.code => Some(&M_CHECKOUT),
            m if m == M_CHECKOUT_RESP.code => Some(&M_CHECKOUT_RESP),
            m if m == M_RENEW.code => Some(&M_RENEW),
            m if m == M_RENEW_RESP.code => Some(&M_RENEW_RESP),
            m if m == M_RENEW_ALL.code => Some(&M_RENEW_ALL),
            m if m == M_RENEW_ALL_RESP.code => Some(&M_RENEW_ALL_RESP),
            m if m == M_CHECKIN.code => Some(&M_CHECKIN),
            m if m == M_CHECKIN_RESP.code => Some(&M_CHECKIN_RESP),
            m if m == M_HOLD.code => Some(&M_HOLD),
            m if m == M_HOLD_RESP.code => Some(&M_HOLD_RESP),
            m if m == M_FEE_PAID.code => Some(&M_FEE_PAID),
            m if m == M_FEE_PAID_RESP.code => Some(&M_FEE_PAID_RESP),
            m if m == M_END_PATRON_SESSION.code => Some(&M_END_PATRON_SESSION),
            m if m == M_END_PATRON_SESSION_RESP.code => Some(&M_END_PATRON_SESSION_RESP),
            m if m == M_END_SESSION.code => Some(&M_END_SESSION),
            m if m == M_END_SESSION_RESP.code => Some(&M_END_SESSION_RESP),
            m if m == M_BLOCK_PATRON.code => Some(&M_BLOCK_PATRON),
            m if m == M_REQUEST_ACS_RESEND.code => Some(&M_REQUEST_ACS_RESEND),
            _ => None,
        }
    }
}

// -------------------------------------------------------------------------
// Fixed Fields
// -------------------------------------------------------------------------

type FF = FixedField; // local shorthand

pub const FF_DATE: FF = FF {
    length: 18,
    label: "transaction date",
};
pub const FF_OK: FF = FF {
    length: 1,
    label: "ok",
};
pub const FF_UID_ALGO: FF = FF {
    length: 1,
    label: "uid algorithm",
};
pub const FF_PWD_ALGO: FF = FF {
    length: 1,
    label: "pwd algorithm",
};
pub const FF_FEE_TYPE: FF = FF {
    length: 2,
    label: "fee type",
};
pub const FF_PAYMENT_TYPE: FF = FF {
    length: 2,
    label: "payment type",
};
pub const FF_CURRENCY: FF = FF {
    length: 3,
    label: "currency type",
};
pub const FF_PAYMENT_ACCEPTED: FF = FF {
    length: 1,
    label: "payment accepted",
};
pub const FF_CIRCULATION_STATUS: FF = FF {
    length: 2,
    label: "circulation status",
};
pub const FF_SECURITY_MARKER: FF = FF {
    length: 2,
    label: "security marker",
};
pub const FF_LANGUAGE: FF = FF {
    length: 3,
    label: "language",
};
pub const FF_PATRON_STATUS: FF = FF {
    length: 14,
    label: "patron status",
};
pub const FF_SUMMARY: FF = FF {
    length: 10,
    label: "summary",
};
pub const FF_HOLD_ITEMS_COUNT: FF = FF {
    length: 4,
    label: "hold items count",
};
pub const FF_OD_ITEMS_COUNT: FF = FF {
    length: 4,
    label: "overdue items count",
};
pub const FF_CH_ITEMS_COUNT: FF = FF {
    length: 4,
    label: "charged items count",
};
pub const FF_FINE_ITEMS_COUNT: FF = FF {
    length: 4,
    label: "fine items count",
};
pub const FF_RECALL_ITEMS_COUNT: FF = FF {
    length: 4,
    label: "recall items count",
};
pub const FF_UNAVAIL_HOLDS_COUNT: FF = FF {
    length: 4,
    label: "unavail holds count",
};
pub const FF_SC_RENEWAL_POLICY: FF = FF {
    length: 1,
    label: "sc renewal policy",
};
pub const FF_NO_BLOCK: FF = FF {
    length: 1,
    label: "no block",
};
pub const FF_NB_DUE_DATE: FF = FF {
    length: 18,
    label: "nb due date",
};
pub const FF_STATUS_CODE: FF = FF {
    length: 1,
    label: "status code",
};
pub const FF_MAX_PRINT_WIDTH: FF = FF {
    length: 3,
    label: "max print width",
};
pub const FF_PROTOCOL_VERSION: FF = FF {
    length: 4,
    label: "protocol version",
};
pub const FF_RENEW_OK: FF = FF {
    length: 1,
    label: "renewal ok",
};
pub const FF_MAGNETIC_MEDIA: FF = FF {
    length: 1,
    label: "magnetic media",
};
pub const FF_DESENSITIZE: FF = FF {
    length: 1,
    label: "desensitize",
};
pub const FF_RESENSITIZE: FF = FF {
    length: 1,
    label: "resensitize",
};
pub const FF_RETURN_DATE: FF = FF {
    length: 18,
    label: "return date",
};
pub const FF_ALERT: FF = FF {
    length: 1,
    label: "alert",
};
pub const FF_ONLINE_STATUS: FF = FF {
    length: 1,
    label: "on-line status",
};
pub const FF_CHECKIN_OK: FF = FF {
    length: 1,
    label: "checkin ok",
};
pub const FF_CHECKOUT_OK: FF = FF {
    length: 1,
    label: "checkout ok",
};
pub const FF_ACS_RENEWAL_POLICY: FF = FF {
    length: 1,
    label: "acs renewal policy",
};
pub const FF_STATUS_UPDATE_OK: FF = FF {
    length: 1,
    label: "status update ok",
};
pub const FF_OFFLINE_OK: FF = FF {
    length: 1,
    label: "offline ok",
};
pub const FF_TIMEOUT_PERIOD: FF = FF {
    length: 3,
    label: "timeout period",
};
pub const FF_RETRIES_ALLOWED: FF = FF {
    length: 3,
    label: "retries allowed",
};
pub const FF_DATETIME_SYNC: FF = FF {
    length: 18,
    label: "date/time sync",
};
pub const FF_THIRD_PARTY_ALLOWED: FF = FF {
    length: 1,
    label: "third party allowed",
};
pub const FF_RENEWED_COUNT: FF = FF {
    length: 4,
    label: "renewed count",
};
pub const FF_UNRENEWED_COUNT: FF = FF {
    length: 4,
    label: "unrenewed count",
};
pub const FF_HOLD_MODE: FF = FF {
    length: 1,
    label: "hold mode",
};
pub const FF_HOLD_AVAILABLE: FF = FF {
    length: 1,
    label: "hold available",
};
pub const FF_CARD_RETAINED: FF = FF {
    length: 1,
    label: "card retained",
};
pub const FF_END_PATRON_SESSION: FF = FF {
    length: 1,
    label: "end session",
};

// -------------------------------------------------------------------------
// Fields
// -------------------------------------------------------------------------

type F = Field; // local shorthand

pub const F_LOGIN_UID: F = F {
    code: "CN",
    label: "login user id",
};
pub const F_LOGIN_PWD: F = F {
    code: "CO",
    label: "login password",
};
pub const F_PATRON_ID: F = F {
    code: "AA",
    label: "patron identifier",
};
pub const F_PATRON_IDENT: F = F {
    code: "AA",
    label: "patron identifier",
};
pub const F_ITEM_IDENT: F = F {
    code: "AB",
    label: "item identifier",
};
pub const F_TERMINAL_PWD: F = F {
    code: "AC",
    label: "terminal password",
};
pub const F_PATRON_PWD: F = F {
    code: "AD",
    label: "patron password",
};
pub const F_PERSONAL_NAME: F = F {
    code: "AE",
    label: "personal name",
};
pub const F_SCREEN_MSG: F = F {
    code: "AF",
    label: "screen message",
};
pub const F_PRINT_LINE: F = F {
    code: "AG",
    label: "print line",
};
pub const F_DUE_DATE: F = F {
    code: "AH",
    label: "due date",
};
pub const F_TITLE_IDENT: F = F {
    code: "AJ",
    label: "title identifier",
};
pub const F_BLOCKED_CARD_MSG: F = F {
    code: "AL",
    label: "blocked card msg",
};
pub const F_LIBRARY_NAME: F = F {
    code: "AM",
    label: "library name",
};
pub const F_TERMINAL_LOCATION: F = F {
    code: "AN",
    label: "terminal location",
};
pub const F_INSTITUTION_ID: F = F {
    code: "AO",
    label: "institution id",
};
pub const F_CURRENT_LOCATION: F = F {
    code: "AP",
    label: "current location",
};
pub const F_PERMANENT_LOCATION: F = F {
    code: "AQ",
    label: "permanent location",
};
pub const F_HOLD_ITEMS: F = F {
    code: "AS",
    label: "hold items",
};
pub const F_OVERDUE_ITEMS: F = F {
    code: "AT",
    label: "overdue items",
};
pub const F_CHARGED_ITEMS: F = F {
    code: "AU",
    label: "charged items",
};
pub const F_FINE_ITEMS: F = F {
    code: "AV",
    label: "fine items",
};
pub const F_SEQUENCE_NUMBER: F = F {
    code: "AY",
    label: "sequence number",
};
pub const F_CHECKSUM: F = F {
    code: "AZ",
    label: "checksum",
};
pub const F_HOME_ADDRESS: F = F {
    code: "BD",
    label: "home address",
};
pub const F_EMAIL_ADDRESS: F = F {
    code: "BE",
    label: "e-mail address",
};
pub const F_HOME_PHONE: F = F {
    code: "BF",
    label: "home phone number",
};
pub const F_OWNER: F = F {
    code: "BG",
    label: "owner",
};
pub const F_CURRENCY: F = F {
    code: "BH",
    label: "currency type",
};
pub const F_CANCEL: F = F {
    code: "BI",
    label: "cancel",
};
pub const F_TRANSACTION_ID: F = F {
    code: "BK",
    label: "transaction id",
};
pub const F_VALID_PATRON: F = F {
    code: "BL",
    label: "valid patron",
};
pub const F_RENEWED_ITEMS: F = F {
    code: "BM",
    label: "renewed items",
};
pub const F_UNRENEWED_ITEMS: F = F {
    code: "BN",
    label: "unrenewed items",
};
pub const F_FEE_ACKNOWLEGED: F = F {
    code: "BO",
    label: "fee acknowledged",
};
pub const F_START_ITEM: F = F {
    code: "BP",
    label: "start item",
};
pub const F_END_ITEM: F = F {
    code: "BQ",
    label: "end item",
};
pub const F_QUEUE_POSITION: F = F {
    code: "BR",
    label: "queue position",
};
pub const F_PICKUP_LOCATION: F = F {
    code: "BS",
    label: "pickup location",
};
pub const F_RECALL_ITEMS: F = F {
    code: "BU",
    label: "recall items",
};
pub const F_FEE_TYPE: F = F {
    code: "BT",
    label: "fee type",
};
pub const F_FEE_LIMIT: F = F {
    code: "CC",
    label: "fee limit",
};
pub const F_FEE_AMOUNT: F = F {
    code: "BV",
    label: "fee amount",
};
pub const F_EXPIRE_DATE: F = F {
    code: "BW",
    label: "expiration date",
};
pub const F_SUPPORTED_MESSAGES: F = F {
    code: "BX",
    label: "supported messages",
};
pub const F_HOLD_TYPE: F = F {
    code: "BY",
    label: "hold type",
};
pub const F_HOLD_ITEMS_LIMIT: F = F {
    code: "BZ",
    label: "hold items limit",
};
pub const F_OVERDUE_ITEMS_LIST: F = F {
    code: "CA",
    label: "overdue items limit",
};
pub const F_CHARGED_ITEMS_LIMIT: F = F {
    code: "CB",
    label: "charged items limit",
};
pub const F_UNAVAIL_HOLD_ITEMS: F = F {
    code: "CD",
    label: "unavailable hold items",
};
pub const F_HOLD_QUEUE_LENGTH: F = F {
    code: "CF",
    label: "hold queue length",
};
pub const F_FEE_IDENTIFIER: F = F {
    code: "CG",
    label: "fee identifier",
};
pub const F_ITEM_PROPERTIES: F = F {
    code: "CH",
    label: "item properties",
};
pub const F_SECURITY_INHIBIT: F = F {
    code: "CI",
    label: "security inhibit",
};
pub const F_RECALL_DATE: F = F {
    code: "CJ",
    label: "recall date",
};
pub const F_MEDIA_TYPE: F = F {
    code: "CK",
    label: "media type",
};
pub const F_SORT_BIN: F = F {
    code: "CL",
    label: "sort bin",
};
pub const F_HOLD_PICKUP_DATE: F = F {
    code: "CM",
    label: "hold pickup date",
};
pub const F_LOGIN_USER_ID: F = F {
    code: "CN",
    label: "login user id",
};
pub const F_LOCATION_CODE: F = F {
    code: "CP",
    label: "location code",
};
pub const F_VALID_PATRON_PWD: F = F {
    code: "CQ",
    label: "valid patron password",
};
pub const F_INET_PROFILE: F = F {
    code: "PI",
    label: "patron internet profile",
};
pub const F_CALL_NUMBER: F = F {
    code: "CS",
    label: "call number",
};
pub const F_COLLECTION_CODE: F = F {
    code: "CR",
    label: "collection code",
};
pub const F_ALERT_TYPE: F = F {
    code: "CV",
    label: "alert type",
};
pub const F_HOLD_PATRON_ID: F = F {
    code: "CY",
    label: "hold patron id",
};
pub const F_HOLD_PATRON_NAME: F = F {
    code: "DA",
    label: "hold patron name",
};
pub const F_DEST_LOCATION: F = F {
    code: "CT",
    label: "destination location",
};

//  Envisionware Terminal Extensions
pub const F_PATRON_EXPIRE_DATE: F = F {
    code: "PA",
    label: "patron expire date",
};
pub const F_PATRON_DOB: F = F {
    code: "PB",
    label: "patron birth date",
};
pub const F_PATRON_CLASS: F = F {
    code: "PC",
    label: "patron class",
};
pub const F_REGISTER_LOGIN: F = F {
    code: "OR",
    label: "register login",
};
pub const F_CHECK_NUMBER: F = F {
    code: "RN",
    label: "check number",
};

// NOTE: when adding new fields, be sure to also add the new
// to Field::from_code()

// -------------------------------------------------------------------------
// Messages
// -------------------------------------------------------------------------

pub const EMPTY: &[&FixedField; 0] = &[];

/// Message 99
pub const M_SC_STATUS: Message = Message {
    code: "99",
    label: "SC Status",
    fixed_fields: &[&FF_STATUS_CODE, &FF_MAX_PRINT_WIDTH, &FF_PROTOCOL_VERSION],
};

/// Message 98
pub const M_ACS_STATUS: Message = Message {
    code: "98",
    label: "ACS Status",
    fixed_fields: &[
        &FF_ONLINE_STATUS,
        &FF_CHECKIN_OK,
        &FF_CHECKOUT_OK,
        &FF_ACS_RENEWAL_POLICY,
        &FF_STATUS_UPDATE_OK,
        &FF_OFFLINE_OK,
        &FF_TIMEOUT_PERIOD,
        &FF_RETRIES_ALLOWED,
        &FF_DATETIME_SYNC,
        &FF_PROTOCOL_VERSION,
    ],
};

/// Message 93
pub const M_LOGIN: Message = Message {
    code: "93",
    label: "Login Request",
    fixed_fields: &[&FF_UID_ALGO, &FF_PWD_ALGO],
};

/// Message 94
pub const M_LOGIN_RESP: Message = Message {
    code: "94",
    label: "Login Response",
    fixed_fields: &[&FF_OK],
};

/// Message 17
pub const M_ITEM_INFO: Message = Message {
    code: "17",
    label: "Item Information Request",
    fixed_fields: &[&FF_DATE],
};

/// Message 18
pub const M_ITEM_INFO_RESP: Message = Message {
    code: "18",
    label: "Item Information Response",
    fixed_fields: &[
        &FF_CIRCULATION_STATUS,
        &FF_SECURITY_MARKER,
        &FF_FEE_TYPE,
        &FF_DATE,
    ],
};

/// Message 23
pub const M_PATRON_STATUS: Message = Message {
    code: "23",
    label: "Patron Status Request",
    fixed_fields: &[&FF_LANGUAGE, &FF_DATE],
};

/// Message 24
pub const M_PATRON_STATUS_RESP: Message = Message {
    code: "24",
    label: "Patron Status Response",
    fixed_fields: &[&FF_PATRON_STATUS, &FF_LANGUAGE, &FF_DATE],
};

/// Message 63
pub const M_PATRON_INFO: Message = Message {
    code: "63",
    label: "Patron Information",
    fixed_fields: &[&FF_LANGUAGE, &FF_DATE, &FF_SUMMARY],
};

/// Message 64
pub const M_PATRON_INFO_RESP: Message = Message {
    code: "64",
    label: "Patron Information Response",
    fixed_fields: &[
        &FF_PATRON_STATUS,
        &FF_LANGUAGE,
        &FF_DATE,
        &FF_HOLD_ITEMS_COUNT,
        &FF_OD_ITEMS_COUNT,
        &FF_CH_ITEMS_COUNT,
        &FF_FINE_ITEMS_COUNT,
        &FF_RECALL_ITEMS_COUNT,
        &FF_UNAVAIL_HOLDS_COUNT,
    ],
};

/// Message 11
pub const M_CHECKOUT: Message = Message {
    code: "11",
    label: "Checkout Request",
    fixed_fields: &[
        &FF_SC_RENEWAL_POLICY,
        &FF_NO_BLOCK,
        &FF_DATE,
        &FF_NB_DUE_DATE,
    ],
};

/// Message 12
pub const M_CHECKOUT_RESP: Message = Message {
    code: "12",
    label: "Checkout Response",
    fixed_fields: &[
        &FF_OK,
        &FF_RENEW_OK,
        &FF_MAGNETIC_MEDIA,
        &FF_DESENSITIZE,
        &FF_DATE,
    ],
};

/// Message 29
pub const M_RENEW: Message = Message {
    code: "29",
    label: "Renew Request",
    fixed_fields: &[
        &FF_THIRD_PARTY_ALLOWED,
        &FF_NO_BLOCK,
        &FF_DATE,
        &FF_NB_DUE_DATE,
    ],
};

/// Message 30
pub const M_RENEW_RESP: Message = Message {
    code: "30",
    label: "Renew Response",
    fixed_fields: &[
        &FF_OK,
        &FF_RENEW_OK,
        &FF_MAGNETIC_MEDIA,
        &FF_DESENSITIZE,
        &FF_DATE,
    ],
};

/// Message 65
pub const M_RENEW_ALL: Message = Message {
    code: "65",
    label: "Renew All Request",
    fixed_fields: &[&FF_DATE],
};

/// Message 66
pub const M_RENEW_ALL_RESP: Message = Message {
    code: "66",
    label: "Renew All Response",
    fixed_fields: &[&FF_OK, &FF_RENEWED_COUNT, &FF_UNRENEWED_COUNT, &FF_DATE],
};

/// Message 09
pub const M_CHECKIN: Message = Message {
    code: "09",
    label: "Checkin Request",
    fixed_fields: &[&FF_NO_BLOCK, &FF_DATE, &FF_RETURN_DATE],
};

/// Message 10
pub const M_CHECKIN_RESP: Message = Message {
    code: "10",
    label: "Checkin Response",
    fixed_fields: &[
        &FF_OK,
        &FF_RESENSITIZE,
        &FF_MAGNETIC_MEDIA,
        &FF_ALERT,
        &FF_DATE,
    ],
};

/// Message 15
pub const M_HOLD: Message = Message {
    code: "15",
    label: "Hold Request",
    fixed_fields: &[&FF_HOLD_MODE, &FF_DATE],
};

/// Message 16
pub const M_HOLD_RESP: Message = Message {
    code: "16",
    label: "Hold Response",
    fixed_fields: &[&FF_OK, &FF_HOLD_AVAILABLE, &FF_DATE],
};

/// Message 35
pub const M_END_PATRON_SESSION: Message = Message {
    code: "35",
    label: "End Patron Session",
    fixed_fields: &[&FF_DATE],
};

/// Message 36
pub const M_END_PATRON_SESSION_RESP: Message = Message {
    code: "36",
    label: "End Session Response",
    fixed_fields: &[&FF_END_PATRON_SESSION, &FF_DATE],
};

/// Message 37
pub const M_FEE_PAID: Message = Message {
    code: "37",
    label: "Fee Paid",
    fixed_fields: &[&FF_DATE, &FF_FEE_TYPE, &FF_PAYMENT_TYPE, &FF_CURRENCY],
};

/// Message 38
pub const M_FEE_PAID_RESP: Message = Message {
    code: "38",
    label: "Fee Paid Response",
    fixed_fields: &[&FF_PAYMENT_ACCEPTED, &FF_DATE],
};

/// Message 97
pub const M_REQUEST_ACS_RESEND: Message = Message {
    code: "97",
    label: "Request ACS Resend",
    fixed_fields: &[],
};

/// Message 01
pub const M_BLOCK_PATRON: Message = Message {
    code: "01",
    label: "Block Patron",
    fixed_fields: &[&FF_CARD_RETAINED, &FF_DATE],
};

// Custom "end session" messages for SIP2Mediator.
// This differs from "End Patron Session" (35) message in that it's not
// about a patron but about a SIP client session, which can involve
// many patrons (or none).

/// SIP2Mediator XS (End Session) Message
pub const M_END_SESSION: Message = Message {
    code: "XS",
    label: "End SIP Session",
    fixed_fields: &[],
};

/// SIP2Mediator XT (End Session Response) Message
pub const M_END_SESSION_RESP: Message = Message {
    code: "XT",
    label: "End SIP Session Response",
    fixed_fields: &[],
};

// NOTE: when adding new message types, be sure to also add the new
// message to Message::from_code()
