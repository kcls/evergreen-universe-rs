//! Evergreen sample data and tools
use crate as eg;
use eg::constants as C;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;

// Sample data based on the Evergreen Concerto sample data set.

pub const ACN_CREATOR: i64 = 1;
pub const ACN_RECORD: i64 = 1;
pub const ACN_LABEL: &str = "_EG_TEST_";
pub const ACN_LABEL_CLASS: i64 = 1; // Generic
pub const ACP_STATUS: i64 = C::COPY_STATUS_AVAILABLE;
pub const ACP_BARCODE: &str = "_EG_TEST_";
pub const ACP_LOAN_DURATION: i64 = C::CIRC_DURATION_NORMAL;
pub const ACP_FINE_LEVEL: i64 = C::CIRC_FINE_LEVEL_MEDIUM;
pub const AOU_BR1_ID: i64 = 4;
pub const AOU_BR1_SHORTNAME: &str = "BR1";
pub const AOU_BR2_ID: i64 = 5;
pub const AOU_BR2_SHORTNAME: &str = "BR2";

pub const AU_BARCODE: &str = "_EG_TEST_";
pub const AU_PROFILE: i64 = 2; // Patrons
pub const AU_IDENT_TYPE: i64 = 3; // Other

pub const AU_STAFF_ID: i64 = 195; // br1mclark

pub const PAYMENT_TYPES: [&str; 8] = [
    "credit_card_payment",
    "credit_payment",
    "check_payment",
    "work_payment",
    "forgive_payment",
    "goods_payment",
    "account_adjustment",
    "debit_card_payment",
];

pub struct SampleData {
    pub acn_creator: i64,
    pub acn_record: i64,
    pub aou_id: i64,
    pub aou_shortname: String,
    pub acn_label: String,
    pub acn_label_class: i64,
    pub acp_barcode: String,
    pub au_barcode: String,
    pub au_profile: i64,
    pub au_ident_type: i64,

    /// If we create any transactions, this tracks the ID of the most
    /// recently created transaction.
    pub last_xact_id: Option<i64>,
}

impl SampleData {
    pub fn new() -> SampleData {
        SampleData {
            acn_creator: ACN_CREATOR,
            acn_record: ACN_RECORD,
            aou_id: AOU_BR1_ID,
            acn_label: ACN_LABEL.to_string(),
            acn_label_class: ACN_LABEL_CLASS,
            acp_barcode: ACP_BARCODE.to_string(),
            aou_shortname: AOU_BR1_SHORTNAME.to_string(),
            au_barcode: AU_BARCODE.to_string(),
            au_profile: AU_PROFILE,
            au_ident_type: AU_IDENT_TYPE,
            last_xact_id: None,
        }
    }

    pub fn create_default_acn(&self, e: &mut Editor) -> EgResult<EgValue> {
        let mut acn = eg::hash! {
            creator: self.acn_creator,
            editor: self.acn_creator,
            record: self.acn_record,
            owning_lib: self.aou_id,
            label: self.acn_label.to_string(),
            label_class: self.acn_label_class,
        };

        acn.bless("acn")?;

        e.create(acn)
    }

    pub fn create_default_acp(&self, e: &mut Editor, acn_id: i64) -> EgResult<EgValue> {
        let mut acp = eg::hash! {
            call_number: acn_id,
            creator: self.acn_creator,
            editor: self.acn_creator,
            status: ACP_STATUS,
            circ_lib: self.aou_id,
            loan_duration: ACP_LOAN_DURATION,
            fine_level: ACP_FINE_LEVEL,
            barcode: self.acp_barcode.to_string(),
        };

        acp.bless("acp")?;

        e.create(acp)
    }

    pub fn delete_default_acn(&self, e: &mut Editor) -> EgResult<()> {
        let mut acns = e.search(
            "acn",
            eg::hash! {label: self.acn_label.to_string(), deleted: "f"},
        )?;

        if let Some(acn) = acns.pop() {
            e.delete(acn)?;
        }

        Ok(())
    }

    pub fn get_default_acp(&self, e: &mut Editor) -> EgResult<EgValue> {
        e.search(
            "acp",
            eg::hash! {barcode: self.acp_barcode.to_string(), deleted: "f"},
        )?
        .pop()
        .ok_or_else(|| format!("Cannot find default copy").into())
    }

    pub fn delete_default_acp(&self, e: &mut Editor) -> EgResult<()> {
        if let Ok(acp) = self.get_default_acp(e) {
            e.delete(acp)?;
        }
        Ok(())
    }

    pub fn modify_default_acp(&self, e: &mut Editor, mut values: EgValue) -> EgResult<()> {
        let mut acp = self.get_default_acp(e)?;
        for (k, v) in values.entries_mut() {
            acp[k] = v.take();
        }
        e.update(acp)
    }

    /// Create default user with a default card.
    pub fn create_default_au(&self, e: &mut Editor) -> EgResult<EgValue> {
        let mut au = eg::hash! {
            profile: self.au_profile,
            usrname: self.au_barcode.to_string(),
            passwd: self.au_barcode.to_string(),
            ident_type: self.au_ident_type,
            first_given_name: "_EG_TEST_",
            family_name: "_EG_TEST_",
            home_ou: self.aou_id,
        };

        au.bless("au")?;

        let mut au = e.create(au)?;

        let mut ac = eg::hash! {
            barcode: self.au_barcode.to_string(),
            usr: au["id"].clone(),
        };

        ac.bless("ac")?;

        let ac = e.create(ac)?;

        // Link the user back to the card
        au["card"] = ac["id"].clone();
        e.update(au.clone())?;

        Ok(au)
    }

    /// Create a new "grocery" billable transaction with a single billing
    ///
    /// Returns the billing value with its "xact" field fleshed.
    pub fn add_billing(&mut self, e: &mut Editor, btype: i64, amount: f64) -> EgResult<EgValue> {
        let user_id = self.get_default_user_id(e)?;

        let xact = eg::blessed! {
            "_classname": "mg",
            "usr": user_id,
            "billing_location": self.aou_id,
        }?;

        let xact = e.create(xact)?;

        self.last_xact_id = Some(xact.id()?);

        let billing = eg::blessed! {
            "_classname": "mb",
            "xact": xact.id()?,
            "btype": btype,
            "billing_type": "Testing",
            "amount": amount,
        }?;

        let mut billing = e.create(billing)?;

        billing["xact"] = xact;

        Ok(billing)
    }

    fn get_default_user_id(&self, e: &mut Editor) -> EgResult<Option<i64>> {
        let query = eg::hash! {
            "barcode": self.au_barcode.as_str(),
        };

        if let Some(user) = e.search("ac", query)?.pop() {
            Ok(Some(user.id()?))
        } else {
            Ok(None)
        }
    }

    /// Deletes the transaction and all linked billings and payments.
    pub fn delete_user_xacts(&self, e: &mut Editor) -> EgResult<()> {
        let user_id = self.get_default_user_id(e)?;

        let query = eg::hash! {"usr": user_id};
        let flesh = eg::hash! {
            "flesh": 2,
            "flesh_fields": {
                "mbt": ["billings", "payments"],
                "mp": PAYMENT_TYPES.as_slice(),
            }
        };

        let mut xacts = e.search_with_ops("mbt", query, flesh)?;

        for mut xact in xacts.drain(..) {
            // These are both arrays
            let mut payments = xact["payments"].take();
            let mut billings = xact["billings"].take();

            for mut payment_view in payments.take_vec().unwrap().drain(..) {
                for paytype in PAYMENT_TYPES {
                    let payment = payment_view[paytype].take();
                    // maybe null
                    if payment.is_blessed() {
                        e.delete(payment)?;
                    }
                }
            }

            for billing in billings.take_vec().unwrap().drain(..) {
                e.delete(billing)?;
            }

            e.delete(xact)?;
        }

        Ok(())
    }

    /// Purge the default user, including its linked card, transactions, etc.
    pub fn delete_default_au(&self, e: &mut Editor) -> EgResult<()> {
        let cards = e.search("ac", eg::hash! {barcode: self.au_barcode.to_string()})?;

        if let Some(ac) = cards.get(0) {
            // Purge the user, attached card, and any other data
            // linked to the user.
            let query = eg::hash! {
                from: ["actor.usr_delete", ac["usr"].clone(), EgValue::Null]
            };

            e.json_query(query)?;
        }

        Ok(())
    }
}
