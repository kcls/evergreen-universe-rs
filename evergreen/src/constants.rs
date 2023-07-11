/// Evergreen Constants

// ---------------------------------------------------------------------
// Copy Statuses
// ---------------------------------------------------------------------
pub const COPY_STATUS_AVAILABLE: i64 = 0;
pub const COPY_STATUS_CHECKED_OUT: i64 = 1;
pub const COPY_STATUS_BINDERY: i64 = 2;
pub const COPY_STATUS_LOST: i64 = 3;
pub const COPY_STATUS_MISSING: i64 = 4;
pub const COPY_STATUS_IN_PROCESS: i64 = 5;
pub const COPY_STATUS_IN_TRANSIT: i64 = 6;
pub const COPY_STATUS_RESHELVING: i64 = 7;
pub const COPY_STATUS_ON_HOLDS_SHELF: i64 = 8;
pub const COPY_STATUS_ON_ORDER: i64 = 9;
pub const COPY_STATUS_ILL: i64 = 10;
pub const COPY_STATUS_CATALOGING: i64 = 11;
pub const COPY_STATUS_RESERVES: i64 = 12;
pub const COPY_STATUS_DISCARD: i64 = 13;
pub const COPY_STATUS_DAMAGED: i64 = 14;
pub const COPY_STATUS_ON_RESV_SHELF: i64 = 15;
pub const COPY_STATUS_LONG_OVERDUE: i64 = 16;
pub const COPY_STATUS_LOST_AND_PAID: i64 = 17;
pub const COPY_STATUS_CANCELED_TRANSIT: i64 = 18;

// ---------------------------------------------------------------------
// Loans
// ---------------------------------------------------------------------
pub const CIRC_DURATION_SHORT: i64 = 1;
pub const CIRC_DURATION_NORMAL: i64 = 2;
pub const CIRC_DURATION_EXTENDED: i64 = 3;
pub const CIRC_FINE_LEVEL_LOW: i64 = 1;
pub const CIRC_FINE_LEVEL_MEDIUM: i64 = 2;
pub const CIRC_FINE_LEVEL_HIGH: i64 = 3;

// ---------------------------------------------------------------------
// Billing Types
// ---------------------------------------------------------------------
pub const BTYPE_OVERDUE_MATERIALS: i64 = 1;
pub const BTYPE_LONG_OVERDUE_COLLECTION_FEE: i64 = 2;
pub const BTYPE_LOST_MATERIALS: i64 = 3;
pub const BTYPE_LOST_MATERIALS_PROCESSING_FEE: i64 = 4;
pub const BTYPE_DEPOSIT: i64 = 5;
pub const BTYPE_RENTAL: i64 = 6;
pub const BTYPE_DAMAGED_ITEM: i64 = 7;
pub const BTYPE_DAMAGED_ITEM_PROCESSING_FEE: i64 = 8;
pub const BTYPE_NOTIFICATION_FEE: i64 = 9;
pub const BTYPE_LONG_OVERDUE_MATERIALS: i64 = 10;
pub const BTYPE_LONG_OVERDUE_MATERIALS_PROCESSING_FEE: i64 = 11;

// ---------------------------------------------------------------------
// Hold Types
// ---------------------------------------------------------------------
pub const HOLD_TYPE_COPY: &str = "C";
pub const HOLD_TYPE_FORCE: &str = "F";
pub const HOLD_TYPE_RECALL: &str = "R";
pub const HOLD_TYPE_ISSUANCE: &str = "I";
pub const HOLD_TYPE_VOLUME: &str = "V";
pub const HOLD_TYPE_TITLE: &str = "T";
pub const HOLD_TYPE_METARECORD: &str = "M";
pub const HOLD_TYPE_MONOPART: &str = "P";
