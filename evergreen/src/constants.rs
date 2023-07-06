/// Evergreen Constants

// ---------------------------------------------------------------------
// Copy Statuses
// ---------------------------------------------------------------------
pub const EG_COPY_STATUS_AVAILABLE: i64 = 0;
pub const EG_COPY_STATUS_CHECKED_OUT: i64 = 1;
pub const EG_COPY_STATUS_BINDERY: i64 = 2;
pub const EG_COPY_STATUS_LOST: i64 = 3;
pub const EG_COPY_STATUS_MISSING: i64 = 4;
pub const EG_COPY_STATUS_IN_PROCESS: i64 = 5;
pub const EG_COPY_STATUS_IN_TRANSIT: i64 = 6;
pub const EG_COPY_STATUS_RESHELVING: i64 = 7;
pub const EG_COPY_STATUS_ON_HOLDS_SHELF: i64 = 8;
pub const EG_COPY_STATUS_ON_ORDER: i64 = 9;
pub const EG_COPY_STATUS_ILL: i64 = 10;
pub const EG_COPY_STATUS_CATALOGING: i64 = 11;
pub const EG_COPY_STATUS_RESERVES: i64 = 12;
pub const EG_COPY_STATUS_DISCARD: i64 = 13;
pub const EG_COPY_STATUS_DAMAGED: i64 = 14;
pub const EG_COPY_STATUS_ON_RESV_SHELF: i64 = 15;
pub const EG_COPY_STATUS_LONG_OVERDUE: i64 = 16;
pub const EG_COPY_STATUS_LOST_AND_PAID: i64 = 17;
pub const EG_COPY_STATUS_CANCELED_TRANSIT: i64 = 18;

pub const OILS_CIRC_DURATION_SHORT: i64 = 1;
pub const OILS_CIRC_DURATION_NORMAL: i64 = 2;
pub const OILS_CIRC_DURATION_EXTENDED: i64 = 3;
pub const OILS_CIRC_FINE_LEVEL_LOW: i64 = 1;
pub const OILS_CIRC_FINE_LEVEL_MEDIUM: i64 = 2;
pub const OILS_CIRC_FINE_LEVEL_HIGH: i64 = 3;

// ---------------------------------------------------------------------
// Hold Types
// ---------------------------------------------------------------------
pub const EG_HOLD_TYPE_COPY: &str = "C";
pub const EG_HOLD_TYPE_FORCE: &str = "F";
pub const EG_HOLD_TYPE_RECALL: &str = "R";
pub const EG_HOLD_TYPE_ISSUANCE: &str = "I";
pub const EG_HOLD_TYPE_VOLUME: &str = "V";
pub const EG_HOLD_TYPE_TITLE: &str = "T";
pub const EG_HOLD_TYPE_METARECORD: &str = "M";
pub const EG_HOLD_TYPE_MONOPART: &str = "P";
