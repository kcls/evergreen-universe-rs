use crate::common::org;
use crate::common::penalty;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;
use crate::date;
use std::collections::HashSet;
use json::JsonValue;
use chrono::{Duration, Local};

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
        if util::json_bool(&bill["voided"]) {
            log::debug!("Billing {} already voided.  Skipping", bill["id"]);
            continue;
        }

        let xact = editor.retrieve("mbt", bill["xact"].clone())?;
        let xact = match xact {
            Some(x) => x,
            None => return editor.die_event(),
        };

        let xact_org = xact_org(editor, util::json_int(&xact["id"])?)?;
        let xact_user = util::json_int(&xact["usr"])?;
        let xact_id = util::json_int(&xact["id"])?;

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

    let zero_owed = util::json_float(&mbts["balance_owed"])? == 0.0;
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
        util::json_int(&sum["billing_location"])
    } else {
        Err(format!("No Such Transaction: {xact_id}"))
    }
}

/// Creates and returns the newly created money.billing.
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

pub fn void_or_zero_bills_of_type(
    editor: &mut Editor,
    xact_id: i64,
    context_org: i64,
    btype_id: i64,
    for_note: &str
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
        .map(|b| util::json_int(&b["id"]).expect("Billing has invalid id?"))
        .collect();

    let prohibit_neg_balance =
        util::json_bool(
            settings.get_value_at_org("bill.prohibit_negative_balance_on_lost", context_org)?
        ) || util::json_bool(
            settings.get_value_at_org("bill.prohibit_negative_balance_default", context_org)?
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
        adjust_bills_to_zero(editor, bill_ids.as_slice(), &note)?;

    } else {
        // TODO
            /*
            $result = $class->void_bills($e, $billids, "System: VOIDED $for_note");
        }
        */
    }

    Ok(())
}

pub fn adjust_bills_to_zero(editor: &mut Editor, bill_ids: &[i64], note: &str) -> Result<(), String> {
    Ok(())
}

pub fn xact_has_payment_within(editor: &mut Editor, xact_id: i64, interval: &str) -> Result<bool, String> {
    let query = json::object! {"xact": xact_id, "payment_type": json::object! {"!=": "account_adjustment"}};
    let ops = json::object! {"limit": 1, "order_by": json::object! {"mp": "payment_ts DESC"}};

    let last_payment = editor.search_with_ops("mp", query, ops)?;

    if last_payment.len() == 0 {
        return Ok(false);
    }

    let payment = &last_payment[0];
    let intvl_secs = date::interval_to_seconds(interval)?;
    // Every payment has a payment_ts value
    let payment_ts = date::parse_datetime(&payment["payment_ts"].as_str().unwrap())?;
    let max_time = payment_ts + Duration::seconds(intvl_secs);

    Ok(max_time > Local::now())
}


pub fn generate_fines(editor: &mut Editor, xact_ids: &[i64]) -> Result<(), String> {
    todo!()
}

