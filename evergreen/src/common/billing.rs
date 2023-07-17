use crate::common::org;
use crate::common::penalty;
use crate::date;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;
use crate::util::{json_bool, json_float, json_int};
use chrono::{Duration, Local};
use json::JsonValue;
use std::cmp::Ordering;
use std::collections::HashSet;

/// Void a list of billings.
pub fn void_bills(
    editor: &mut Editor,
    billing_ids: &[i64], // money.billing.id
    maybe_note: Option<&str>,
) -> Result<(), String> {
    editor.has_requestor()?;

    let mut bills = editor.search("mb", json::object! {"id": billing_ids})?;
    let mut penalty_users: HashSet<(i64, i64)> = HashSet::new();

    if bills.len() == 0 {
        return Err(format!("No such billings: {billing_ids:?}"));
    }

    for mut bill in bills.drain(0..) {
        if json_bool(&bill["voided"]) {
            log::debug!("Billing {} already voided.  Skipping", bill["id"]);
            continue;
        }

        let xact = editor.retrieve("mbt", bill["xact"].clone())?;
        let xact = match xact {
            Some(x) => x,
            None => return editor.die_event(),
        };

        let xact_org = xact_org(editor, json_int(&xact["id"])?)?;
        let xact_user = json_int(&xact["usr"])?;
        let xact_id = json_int(&xact["id"])?;

        penalty_users.insert((xact_user, xact_org));

        bill["voided"] = json::from("t");
        bill["voider"] = json::from(editor.requestor_id());
        bill["void_time"] = json::from("now");

        if let Some(orig_note) = bill["note"].as_str() {
            if let Some(new_note) = maybe_note {
                bill["note"] = json::from(format!("{}\n{}", orig_note, new_note).as_str());
            }
        } else if let Some(new_note) = maybe_note {
            bill["note"] = json::from(new_note);
        }

        editor.update(&bill)?;
        check_open_xact(editor, xact_id)?;
    }

    for (user_id, org_id) in penalty_users.iter() {
        penalty::calculate_penalties(editor, *user_id, *org_id, None)?;
    }

    Ok(())
}

/// Sets or clears xact_finish on a transaction as needed.
pub fn check_open_xact(editor: &mut Editor, xact_id: i64) -> Result<(), String> {
    let mut xact = match editor.retrieve("mbt", xact_id)? {
        Some(x) => x,
        None => return editor.die_event(),
    };

    let mbts = match editor.retrieve("mbts", xact_id)? {
        Some(m) => m,
        None => return editor.die_event(),
    };

    // See if we have a completed circ.
    let no_circ_or_complete = match editor.retrieve("circ", xact_id)? {
        Some(c) => c["stop_fines"].is_string(), // otherwise is_null()
        None => true,
    };

    let zero_owed = json_float(&mbts["balance_owed"])? == 0.0;
    let xact_open = xact["xact_finish"].is_null();

    if zero_owed {
        if xact_open && no_circ_or_complete {
            // If nothing is owed on the transaction, but it is still open,
            // and this transaction is not an open circulation, close it.

            log::info!("Closing completed transaction {xact_id} on zero balance");
            xact["xact_finish"] = json::from("now");
            return editor.update(&xact);
        }
    } else if !xact_open {
        // Transaction closed but money or refund still owed.

        if !zero_owed && !xact_open {
            log::info!("Re-opening transaction {xact_id} on non-zero balance");
            xact["xact_finish"] = json::JsonValue::Null;
            return editor.update(&xact);
        }
    }

    Ok(())
}

/// Returns the context org unit ID for a transaction (by ID).
pub fn xact_org(editor: &mut Editor, xact_id: i64) -> Result<i64, String> {
    // There's a view for that!
    // money.billable_xact_summary_location_view
    if let Some(sum) = editor.retrieve("mbtslv", xact_id)? {
        json_int(&sum["billing_location"])
    } else {
        Err(format!("No Such Transaction: {xact_id}"))
    }
}

/// Creates and returns a newly created money.billing.
pub fn create_bill(
    editor: &mut Editor,
    amount: f64,
    btype_id: i64,
    btype_label: &str,
    xact_id: i64,
    maybe_note: Option<&str>,
    period_start: Option<&str>,
    period_end: Option<&str>,
) -> Result<JsonValue, String> {
    log::info!("System is charging ${amount} [btype={btype_id}:{btype_label}] on xact {xact_id}");

    let note = maybe_note.unwrap_or("SYSTEM GENERATED");

    let bill = json::object! {
        "xact": xact_id,
        "amount": amount,
        "period_start": period_start,
        "period_end": period_end,
        "billing_type": btype_label,
        "btype": btype_id,
        "note": note,
    };

    let bill = editor.idl().create_from("mb", bill)?;
    editor.create(&bill)
}

/// Void a set of bills (by type) for a transaction or apply
/// adjustments to zero the bills, depending on settings, etc.
pub fn void_or_zero_bills_of_type(
    editor: &mut Editor,
    xact_id: i64,
    context_org: i64,
    btype_id: i64,
    for_note: &str,
) -> Result<(), String> {
    log::info!("Void/Zero Bills for xact={xact_id} and btype={btype_id}");

    let mut settings = Settings::new(&editor);
    let query = json::object! {"xact": xact_id, "btype": btype_id};
    let bills = editor.search("mb", query)?;

    if bills.len() == 0 {
        return Ok(());
    }

    let bill_ids: Vec<i64> = bills
        .iter()
        .map(|b| json_int(&b["id"]).expect("Billing has invalid id?"))
        .collect();

    // "lost" settings are checked first for backwards compat /
    // consistency with Perl.
    let prohibit_neg_balance = json_bool(
        settings.get_value_at_org("bill.prohibit_negative_balance_on_lost", context_org)?,
    ) || json_bool(
        settings.get_value_at_org("bill.prohibit_negative_balance_default", context_org)?,
    );

    let mut neg_balance_interval =
        settings.get_value_at_org("bill.negative_balance_interval_on_lost", context_org)?;

    if neg_balance_interval.is_null() {
        neg_balance_interval =
            settings.get_value_at_org("bill.negative_balance_interval_default", context_org)?;
    }

    let mut has_refundable = false;
    if let Some(interval) = neg_balance_interval.as_str() {
        has_refundable = xact_has_payment_within(editor, xact_id, interval)?;
    }

    if prohibit_neg_balance && !has_refundable {
        let note = format!("System: ADJUSTED {for_note}");
        adjust_bills_to_zero(editor, bill_ids.as_slice(), &note)
    } else {
        let note = format!("System: VOIDED {for_note}");
        void_bills(editor, bill_ids.as_slice(), Some(&note))
    }
}

/// Assumes all bills are linked to the same transaction.
pub fn adjust_bills_to_zero(
    editor: &mut Editor,
    bill_ids: &[i64],
    note: &str,
) -> Result<(), String> {
    let bills = editor.search("mb", json::object! {"id": bill_ids})?;
    if bills.len() == 0 {
        return Ok(());
    }

    let xact_id = json_int(&bills[0]["xact"])?;

    let flesh = json::object! {
        "flesh": 2,
        "flesh_fields": {
            "mbt": ["grocery", "circulation"],
            "circ": ["target_copy"]
        }
    };

    let mbt = editor
        .retrieve_with_ops("mbt", xact_id, flesh)?
        .expect("Billing has no transaction?");

    let grocery = &mbt["grocery"];
    let circulation = &mbt["circulation"];

    // TODO
    // bill_payment_map_for_xact()
    // ...

    todo!();
}

pub struct BillPaymentMap {
    /// The adjusted bill object
    pub bill: JsonValue,
    /// List of account adjustments that apply directly to the bill.
    pub adjustments: Vec<JsonValue>,
    /// List of payment objects applied to the bill
    pub payments: Vec<JsonValue>,
    /// original amount from the billing object
    pub bill_amount: f64,
    /// Total of account adjustments that apply to the bill.
    pub adjustment_amount: f64,
}

pub fn bill_payment_map_for_xact(
    editor: &mut Editor,
    xact_id: i64,
) -> Result<Vec<BillPaymentMap>, String> {
    let query = json::object! {
        "xact": xact_id,
        "voided": "f",
    };
    let ops = json::object! {
        "order_by": {
            "mb": {
                "billing_ts": {
                    "direction": "asc"
                }
            }
        }
    };

    let mut bills = editor.search_with_ops("mb", query, ops)?;

    let mut maps = Vec::new();

    if bills.len() == 0 {
        return Ok(maps);
    }

    for bill in bills.drain(0..) {
        let amount = json_float(&bill["amount"])?;

        let map = BillPaymentMap {
            bill: bill,
            adjustments: Vec::new(),
            payments: Vec::new(),
            bill_amount: amount,
            adjustment_amount: 0.00,
        };

        maps.push(map);
    }

    let query = json::object! {"xact": xact_id, "voided": "f"};

    let ops = json::object! {
        "flesh": 1,
        "flesh_fields": {"mp": ["account_adjustment"]},
        "order_by": {"mp": {"payment_ts": {"direction": "asc"}}},
    };

    let mut payments = editor.search_with_ops("mp", query, ops)?;

    if payments.len() == 0 {
        // If we have no payments, return the unmodified maps.
        return Ok(maps);
    }

    // Sort payments largest to lowest amount.
    // This will come in handy later.
    payments.sort_by(|a, b| {
        if json_int(&b["amount"]).unwrap() < json_int(&a["amount"]).unwrap() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });

    let mut used_adjustments: HashSet<i64> = HashSet::new();

    for map in maps.iter_mut() {
        let bill = &mut map.bill;

        // Find adjustments that apply to this individual billing and
        // has not already been accounted for.
        let mut my_adjustments: Vec<&mut JsonValue> = payments
            .iter_mut()
            .filter(|p| p["payment_type"].as_str().unwrap() == "account_adjustment")
            .filter(|p| {
                used_adjustments.contains(&json_int(&p["account_adjustment"]["id"]).unwrap())
            })
            .filter(|p| p["account_adjustment"]["billing"] == bill["id"])
            .map(|p| &mut p["account_adjustment"])
            .collect();

        if my_adjustments.len() == 0 {
            continue;
        }

        for adjustment in my_adjustments.drain(0..) {
            let adjust_amount = json_float(&adjustment["amount"])?;
            let adjust_id = json_int(&adjustment["id"])?;

            let new_amount = util::fpdiff(json_float(&bill["amount"])?, adjust_amount);

            if new_amount >= 0.0 {
                map.adjustments.push(adjustment.clone());
                map.adjustment_amount += adjust_amount;
                bill["amount"] = json::from(new_amount);
                used_adjustments.insert(adjust_id);
            } else {
                // It should never happen that we have more adjustment
                // payments on a single bill than the amount of the bill.

                // Clone the adjustment to say how much of it actually
                // applied to this bill.
                let mut new_adjustment = adjustment.clone();
                new_adjustment["amount"] = bill["amount"].clone();
                new_adjustment["amount_collected"] = bill["amount"].clone();
                map.adjustments.push(new_adjustment.clone());
                map.adjustment_amount += json_float(&new_adjustment["amount"])?;
                bill["amount"] = json::from(0.0);
                adjustment["amount"] = json::from(-new_amount);
            }

            if json_float(&bill["amount"])? == 0.0 {
                break;
            }
        }
    }

    // Try to map payments to bills by amounts starting with the
    // largest payments.
    let mut used_payments: HashSet<i64> = HashSet::new();
    for payment in payments.iter() {
        let mut map = match maps
            .iter_mut()
            .filter(|m| {
                m.bill["amount"] == payment["amount"]
                    && !used_payments.contains(&json_int(&payment["id"]).unwrap())
            })
            .next()
        {
            Some(m) => m,
            None => continue,
        };

        map.bill["amount"] = json::from(0.0);
        map.payments.push(payment.clone());
        used_payments.insert(json_int(&payment["id"])?);
    }

    // Remove the used payments from our working list.
    let mut new_payments = Vec::new();
    for pay in payments.drain(0..) {
        if !used_payments.contains(&json_int(&pay["id"])?) {
            new_payments.push(pay);
        }
    }
    payments = new_payments;
    let mut used_payments = HashSet::new();

    // Map remaining bills to payments in whatever order.
    for map in maps
        .iter_mut()
        .filter(|m| json_float(&m.bill["amount"]).unwrap() > 0.0)
    {
        let bill = &mut map.bill;
        // Loop over remaining unused / unmapped payments.
        for pay in payments
            .iter_mut()
            .filter(|p| !used_payments.contains(&json_int(&p["id"]).unwrap()))
        {
            loop {
                let bill_amount = json_float(&bill["amount"])?;
                if bill_amount > 0.0 {
                    let new_amount = util::fpdiff(bill_amount, json_float(&pay["amount"])?);
                    if new_amount < 0.0 {
                        let mut new_payment = pay.clone();
                        new_payment["amount"] = json::from(bill_amount);
                        bill["amount"] = json::from(0.0);
                        map.payments.push(new_payment);
                        pay["amount"] = json::from(-new_amount);
                    } else {
                        bill["amount"] = json::from(new_amount);
                        map.payments.push(pay.clone());
                        used_payments.insert(json_int(&pay["id"])?);
                    }
                }
            }
        }
    }

    Ok(maps)
}

/// Returns true if the most recent payment toward a transaction
/// occurred within now minus the specified interval.
pub fn xact_has_payment_within(
    editor: &mut Editor,
    xact_id: i64,
    interval: &str,
) -> Result<bool, String> {
    let query = json::object! {
        "xact": xact_id,
        "payment_type": {"!=": "account_adjustment"}
    };

    let ops = json::object! {
        "limit": 1,
        "order_by": {"mp": "payment_ts DESC"}
    };

    let last_payment = editor.search_with_ops("mp", query, ops)?;

    if last_payment.len() == 0 {
        return Ok(false);
    }

    let payment = &last_payment[0];
    let intvl_secs = date::interval_to_seconds(interval)?;

    // Every payment has a payment_ts value
    let payment_ts = &payment["payment_ts"].as_str().unwrap();
    let payment_dt = date::parse_datetime(payment_ts)?;

    // Payments made before this time don't count.
    let window_start = Local::now() - Duration::seconds(intvl_secs);

    Ok(payment_dt > window_start)
}

pub fn generate_fines(editor: &mut Editor, xact_ids: &[i64]) -> Result<(), String> {
    todo!()
}
