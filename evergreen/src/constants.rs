/// Evergreen Constants

// ---------------------------------------------------------------------
// Copy Statuses
// ---------------------------------------------------------------------
pub const EG_COPY_STATUS_AVAILABLE: i16 = 0;
pub const EG_COPY_STATUS_CHECKED_OUT: i16 = 1;
pub const EG_COPY_STATUS_BINDERY: i16 = 2;
pub const EG_COPY_STATUS_LOST: i16 = 3;
pub const EG_COPY_STATUS_MISSING: i16 = 4;
pub const EG_COPY_STATUS_IN_PROCESS: i16 = 5;
pub const EG_COPY_STATUS_IN_TRANSIT: i16 = 6;
pub const EG_COPY_STATUS_RESHELVING: i16 = 7;
pub const EG_COPY_STATUS_ON_HOLDS_SHELF: i16 = 8;
pub const EG_COPY_STATUS_ON_ORDER: i16 = 9;
pub const EG_COPY_STATUS_ILL: i16 = 10;
pub const EG_COPY_STATUS_CATALOGING: i16 = 11;
pub const EG_COPY_STATUS_RESERVES: i16 = 12;
pub const EG_COPY_STATUS_DISCARD: i16 = 13;
pub const EG_COPY_STATUS_DAMAGED: i16 = 14;
pub const EG_COPY_STATUS_ON_RESV_SHELF: i16 = 15;
pub const EG_COPY_STATUS_LONG_OVERDUE: i16 = 16;
pub const EG_COPY_STATUS_LOST_AND_PAID: i16 = 17;
pub const EG_COPY_STATUS_CANCELED_TRANSIT: i16 = 18;

pub const OILS_CIRC_DURATION_SHORT: i16 = 1;
pub const OILS_CIRC_DURATION_NORMAL: i16 = 2;
pub const OILS_CIRC_DURATION_EXTENDED: i16 = 3;
pub const OILS_CIRC_FINE_LEVEL_LOW: i16 = 1;
pub const OILS_CIRC_FINE_LEVEL_MEDIUM: i16 = 2;
pub const OILS_CIRC_FINE_LEVEL_HIGH: i16 = 3;

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
