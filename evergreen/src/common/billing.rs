use crate::common::org;
use crate::common::penalty;
use crate::common::settings::Settings;
use crate::constants as C;
use crate::date;
use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use crate::util::{json_bool, json_float, json_int, json_string};
use chrono::{DateTime, Duration, FixedOffset, Local};
use json::JsonValue;
use std::cmp::Ordering;
use std::collections::HashSet;

const DAY_OF_SECONDS: i64 = 86400;

/// Void a list of billings.
pub fn void_bills(
    editor: &mut Editor,
    billing_ids: &[i64], // money.billing.id
    maybe_note: Option<&str>,
) -> EgResult<()> {
    editor.has_requestor()?;

    let mut bills = editor.search("mb", json::object! {"id": billing_ids})?;
    let mut penalty_users: HashSet<(i64, i64)> = HashSet::new();

    if bills.len() == 0 {
        Err(format!("No such billings: {billing_ids:?}"))?;
    }

    for mut bill in bills.drain(0..) {
        if json_bool(&bill["voided"]) {
            log::debug!("Billing {} already voided.  Skipping", bill["id"]);
            continue;
        }

        let xact = editor.retrieve("mbt", bill["xact"].clone())?;
        let xact = match xact {
            Some(x) => x,
            None => Err(editor.die_event())?,
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
pub fn check_open_xact(editor: &mut Editor, xact_id: i64) -> EgResult<()> {
    let mut xact = match editor.retrieve("mbt", xact_id)? {
        Some(x) => x,
        None => Err(editor.die_event())?,
    };

    let mbts = match editor.retrieve("mbts", xact_id)? {
        Some(m) => m,
        None => Err(editor.die_event())?,
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
pub fn xact_org(editor: &mut Editor, xact_id: i64) -> EgResult<i64> {
    // There's a view for that!
    // money.billable_xact_summary_location_view
    if let Some(sum) = editor.retrieve("mbtslv", xact_id)? {
        json_int(&sum["billing_location"])
    } else {
        Err(format!("No Such Transaction: {xact_id}").into())
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
) -> EgResult<JsonValue> {
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
) -> EgResult<()> {
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
pub fn adjust_bills_to_zero(editor: &mut Editor, bill_ids: &[i64], note: &str) -> EgResult<()> {
    let mut bills = editor.search("mb", json::object! {"id": bill_ids})?;
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

    let user_id = json_int(&mbt["usr"])?;
    let mut bill_maps = bill_payment_map_for_xact(editor, xact_id)?;

    let xact_total = match bill_maps
        .iter()
        .map(|m| json_float(&m.bill["amount"]).unwrap())
        .reduce(|a, b| a + b)
    {
        Some(t) => t,
        None => return Ok(()), // should never happen
    };

    for bill in bills.iter_mut() {
        let map = match bill_maps
            .iter_mut()
            .filter(|m| m.bill["id"] == bill["id"])
            .next()
        {
            Some(m) => m,
            None => continue, // should never happen
        };

        // The amount to adjust is the non-adjusted balance on the
        // bill. It should never be less than zero.
        let mut amount_to_adjust = util::fpdiff(map.bill_amount, map.adjustment_amount);

        // Check if this bill is already adjusted.  We don't allow
        // "double" adjustments regardless of settings.
        if amount_to_adjust <= 0.0 {
            continue;
        }

        if amount_to_adjust > xact_total {
            amount_to_adjust = xact_total;
        }

        // Create the account adjustment
        let payment = json::object! {
            "amount": amount_to_adjust,
            "amount_collected": amount_to_adjust,
            "xact": xact_id,
            "accepting_usr": editor.requestor_id(),
            "payment_ts": "now",
            "billing": bill["id"].clone(),
            "note": note,
        };

        let payment = editor.idl().create_from("maa", payment)?;
        editor.create(&payment)?;

        // Adjust our bill_payment_map
        map.adjustment_amount += amount_to_adjust;
        map.adjustments.push(payment);

        // Should come to zero:
        let new_bill_amount = util::fpdiff(json_float(&bill["amount"])?, amount_to_adjust);
        bill["amount"] = json::from(new_bill_amount);
    }

    check_open_xact(editor, xact_id)?;

    let org_id = xact_org(editor, xact_id)?;
    penalty::calculate_penalties(editor, user_id, org_id, None)?;

    Ok(())
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
) -> EgResult<Vec<BillPaymentMap>> {
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
        let map = match maps
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
) -> EgResult<bool> {
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
    // "Local" could be replaced with any timezone for the
    // purposes of finding the window size.
    let window_start = Local::now() - Duration::seconds(intvl_secs);

    Ok(payment_dt > window_start)
}

#[derive(Clone, PartialEq)]
pub enum BillableTransactionType {
    Circ,
    Reservation,
}

pub fn generate_fines_for_resv(editor: &mut Editor, resv_id: i64) -> EgResult<()> {
    let resv = editor
        .retrieve("bresv", resv_id)?
        .ok_or(editor.last_event_unchecked())?;

    let fine_interval = match resv["fine_interval"].as_str() {
        Some(f) => f,
        None => return Ok(()),
    };

    generate_fines_for_xact(
        editor,
        resv_id,
        resv["end_time"].as_str().unwrap(),
        json_int(&resv["pickup_lib"])?,
        json_float(&resv["fine_amount"])?,
        fine_interval,
        json_float(&resv["max_fine"])?,
        None, // grace period
        BillableTransactionType::Reservation,
    )
}

pub fn generate_fines_for_circ(editor: &mut Editor, circ_id: i64) -> EgResult<()> {
    let circ = editor
        .retrieve("circ", circ_id)?
        .ok_or(editor.last_event_unchecked())?;

    generate_fines_for_xact(
        editor,
        circ_id,
        circ["due_date"].as_str().unwrap(),
        json_int(&circ["circ_lib"])?,
        json_float(&circ["recurring_fine"])?,
        circ["fine_interval"].as_str().unwrap(),
        json_float(&circ["max_fine"])?,
        circ["grace_period"].as_str(),
        BillableTransactionType::Circ,
    )
}

pub fn generate_fines_for_xact(
    editor: &mut Editor,
    xact_id: i64,
    due_date: &str,
    circ_lib: i64,
    mut recurring_fine: f64,
    fine_interval: &str,
    mut max_fine: f64,
    grace_period: Option<&str>,
    xact_type: BillableTransactionType,
) -> EgResult<()> {
    let mut settings = Settings::new(&editor);

    let fine_interval = date::interval_to_seconds(fine_interval)?;
    let mut grace_period = date::interval_to_seconds(grace_period.unwrap_or("0s"))?;
    let now = Local::now();

    if fine_interval == 0 || recurring_fine * 100.0 == 0.0 || max_fine * 100.0 == 0.0 {
        log::info!(
            "Fine generator skipping transaction {xact_id}
            due to 0 fine interval, 0 fine rate, or 0 max fine."
        );
        return Ok(());
    }

    // TODO add the bit about reservation time zone offsets

    let query = json::object! {
        "xact": xact_id,
        "btype": C::BTYPE_OVERDUE_MATERIALS,
    };

    let ops = json::object! {
        "flesh": 1,
        "flesh_fields": {"mb": ["adjustments"]},
        "order_by": {"mb": "billing_ts DESC"},
    };

    let fines = editor.search_with_ops("mb", query, ops)?;
    let mut current_fine_total = 0.0;
    for fine in fines.iter() {
        if !json_bool(&fine["voided"]) {
            current_fine_total += json_float(&fine["amount"])? * 100.0;
        }
        for adj in fine["adjustments"].members() {
            if !json_bool(&adj["voided"]) {
                current_fine_total -= json_float(&adj["amount"])? * 100.0;
            }
        }
    }

    log::info!(
        "Fine total for transaction {xact_id} is {:.2}",
        current_fine_total / 100.0
    );

    // Determine the billing period of the next fine to generate
    // based on the billing time of the most recent fine *which
    // occurred after the current due date*.  Otherwise, when a
    // due date changes, the fine generator will back-fill billings
    // for a period of time where the item was not technically overdue.
    let fines: Vec<JsonValue> = fines
        .iter()
        .filter(|f| f["billing_ts"].as_str().unwrap() > due_date)
        .map(|f| f.to_owned())
        .collect();

    let due_date_dt = date::parse_datetime(due_date)?;

    // First fine in the list (if we have one) will be the most recent.
    let last_fine_dt = match fines.get(0) {
        Some(f) => date::parse_datetime(&f["billing_ts"].as_str().unwrap())?,
        None => {
            grace_period = extend_grace_period(
                editor,
                circ_lib,
                grace_period,
                due_date_dt,
                Some(&mut settings),
            )?;

            // If we have no fines, due date is the last fine time.
            due_date_dt
        }
    };

    if last_fine_dt > now {
        log::warn!("Transaction {xact_id} has futuer last fine date?");
        return Ok(());
    }

    if last_fine_dt == due_date_dt
        && grace_period > 0
        && now.timestamp() < due_date_dt.timestamp() - grace_period
    {
        // We have no fines yet and we have a grace period and we
        // are still within the grace period.  New fines not yet needed.

        log::info!("Stil within grace period for circ {xact_id}");
        return Ok(());
    }

    // Generate fines for each past interval, including the one we are inside.
    let range = now.timestamp() - last_fine_dt.timestamp();
    let pending_fine_count = (range as f64 / fine_interval as f64).ceil() as i64;

    if pending_fine_count == 0 {
        // No fines to generate.
        return Ok(());
    }

    recurring_fine *= 100.0;
    max_fine *= 100.0;

    let skip_closed_check =
        json_bool(settings.get_value_at_org("circ.fines.charge_when_closed", circ_lib)?);

    let truncate_to_max_fine =
        json_bool(settings.get_value_at_org("circ.fines.truncate_to_max_fine", circ_lib)?);

    let timezone = match settings
        .get_value_at_org("lib.timezone", circ_lib)?
        .as_str()
    {
        Some(tz) => tz,
        None => "local",
    };

    for slot in 0..pending_fine_count {
        if current_fine_total >= max_fine {
            if xact_type == BillableTransactionType::Circ {
                log::info!("Max fines reached for circulation {xact_id}");

                if let Some(mut circ) = editor.retrieve("circ", xact_id)? {
                    circ["stop_fines"] = json::from("MAXFINES");
                    circ["stop_fines_time"] = json::from("now");
                    editor.update(&circ)?;
                    break;
                }
            }
        }

        // Translate the last fine time to the timezone of the affected
        // org unit so the org::next_open_date() calculation below
        // can use the correct day / day of week information, which can
        // vary across timezones.
        let mut period_end = date::set_timezone(last_fine_dt, timezone)?;

        let mut current_bill_count = slot;
        while current_bill_count > 0 {
            period_end = period_end + Duration::seconds(fine_interval);
            current_bill_count -= 1;
        }

        let period_start = period_end - Duration::seconds(fine_interval - 1);

        if !skip_closed_check {
            let org_open_data = org::next_open_date(editor, circ_lib, &period_end)?;
            if org_open_data != org::OrgOpenState::Open {
                // Avoid adding a fine if the org unit is closed
                // on the day of the period_end date.
                continue;
            }
        }

        // The billing amount for this billing normally ought to be
        // the recurring fine amount.  However, if the recurring fine
        // amount would cause total fines to exceed the max fine amount,
        // we may wish to reduce the amount for this billing (if
        // circ.fines.truncate_to_max_fine is true).
        let mut this_billing_amount = recurring_fine;
        if truncate_to_max_fine && (current_fine_total + this_billing_amount) > max_fine {
            this_billing_amount = max_fine - current_fine_total;
        }

        current_fine_total += this_billing_amount;

        let bill = json::object! {
            xact: xact_id,
            note: "System Generated Overdue Fine",
            billing_type: "Overdue materials",
            btype: C::BTYPE_OVERDUE_MATERIALS,
            amount: this_billing_amount / 100.0,
            period_start: date::to_iso(&period_start),
            period_end: date::to_iso(&period_end),
        };

        let bill = editor.idl().create_from("mb", bill)?;
        editor.create(&bill)?;
    }

    let xact = editor.retrieve("mbt", xact_id)?.unwrap(); // required
    let user_id = json_int(&xact["usr"])?;

    penalty::calculate_penalties(editor, user_id, circ_lib, None)?;

    Ok(())
}

pub fn extend_grace_period(
    editor: &mut Editor,
    context_org: i64,
    grace_period: i64,
    mut due_date: DateTime<FixedOffset>,
    settings: Option<&mut Settings>,
) -> EgResult<i64> {
    if grace_period < DAY_OF_SECONDS {
        // Only extended for >1day intervals.
        return Ok(grace_period);
    }

    let mut local_settings;
    let settings = match settings {
        Some(s) => s,
        None => {
            local_settings = Some(Settings::new(&editor));
            local_settings.as_mut().unwrap()
        }
    };

    let extend = json_bool(settings.get_value_at_org("circ.grace.extend", context_org)?);

    if !extend {
        // No extension configured.
        return Ok(grace_period);
    }

    let extend_into_closed =
        json_bool(settings.get_value_at_org("circ.grace.extend.into_closed", context_org)?);

    if extend_into_closed {
        // Merge closed dates trailing the grace period into the grace period.
        // Note to self: why add exactly one day?
        due_date = due_date + Duration::seconds(DAY_OF_SECONDS);
    }

    let extend_all = json_bool(settings.get_value_at_org("circ.grace.extend.all", context_org)?);

    if extend_all {
        // Start checking the day after the item was due.
        due_date = due_date + Duration::seconds(DAY_OF_SECONDS);
    } else {
        // Jump to the end of the grace period.
        due_date = due_date + Duration::seconds(grace_period);
    }

    let org_open_data = org::next_open_date(editor, context_org, &due_date.into())?;

    let closed_until = match org_open_data {
        org::OrgOpenState::Never | org::OrgOpenState::Open => {
            // No need to extend the grace period if the org unit
            // is never open or it's open on the calculated due date;
            return Ok(grace_period);
        }
        org::OrgOpenState::OpensOnDate(d) => d,
    };

    // Extend the due date out (using seconds instead of whole days),
    // until the due date occurs on the next open day.
    let mut new_grace_period = grace_period;
    while due_date.date_naive() < closed_until.date_naive() {
        new_grace_period += DAY_OF_SECONDS;
        due_date = due_date + Duration::seconds(DAY_OF_SECONDS);
    }

    Ok(new_grace_period)
}

pub fn void_or_zero_overdues(
    editor: &mut Editor,
    circ_id: i64,
    backdate: Option<&str>,
    mut note: Option<&str>,
    force_zero: bool,
    force_void: bool,
) -> EgResult<()> {
    log::info!("Voiding overdues for circ={circ_id}");

    let circ = editor
        .retrieve("circ", circ_id)?
        .ok_or(editor.last_event_unchecked())?;

    let mut query = json::object! {
        "xact": circ_id,
        "btype": C::BTYPE_OVERDUE_MATERIALS,
    };

    if let Some(bd) = backdate {
        if note.is_none() {
            note = Some("System: OVERDUE REVERSED FOR BACKDATE");
        }
        if let Some(min_date) = calc_min_void_date(editor, &circ, bd)? {
            query["billing_ts"] = json::object! {">=": date::to_iso(&min_date) };
        }
    }

    let circ_lib = json_int(&circ["circ_lib"])?;
    let bills = editor.search("mb", query)?;

    if bills.len() == 0 {
        // Nothing to void/zero.
        return Ok(());
    }

    let bill_ids: Vec<i64> = bills.iter().map(|b| json_int(&b["id"]).unwrap()).collect();

    let mut settings = Settings::new(&editor);
    let prohibit_neg_balance = json_bool(
        settings.get_value_at_org("bill.prohibit_negative_balance_on_overdue", circ_lib)?,
    ) || json_bool(
        settings.get_value_at_org("bill.prohibit_negative_balance_default", circ_lib)?,
    );

    let mut neg_balance_interval =
        settings.get_value_at_org("bill.negative_balance_interval_on_overdue", circ_lib)?;

    if neg_balance_interval.is_null() {
        neg_balance_interval =
            settings.get_value_at_org("bill.negative_balance_interval_default", circ_lib)?;
    }

    let mut has_refundable = false;
    if let Some(interval) = neg_balance_interval.as_str() {
        has_refundable = xact_has_payment_within(editor, circ_id, interval)?;
    }

    if force_zero || (!force_void && prohibit_neg_balance && !has_refundable) {
        adjust_bills_to_zero(editor, bill_ids.as_slice(), note.unwrap_or(""))
    } else {
        void_bills(editor, bill_ids.as_slice(), note)
    }
}

/// Determine the minimum overdue billing date that can be voided,
/// based on the provided backdate.
///
/// Fines for overdue materials are assessed up to, but not including,
/// one fine interval after the fines are applicable.  Here, we add
/// one fine interval to the backdate to ensure that we are not
/// voiding fines that were applicable before the backdate.
fn calc_min_void_date(
    editor: &mut Editor,
    circ: &JsonValue,
    backdate: &str,
) -> EgResult<Option<DateTime<FixedOffset>>> {
    let fine_interval = json_string(&circ["fine_interval"])?;
    let fine_interval = date::interval_to_seconds(&fine_interval)?;
    let backdate = date::parse_datetime(backdate)?;
    let due_date = date::parse_datetime(&json_string(&circ["due_date"])?)?;

    let grace_period = circ["grace_period"].as_str().unwrap_or("0s");
    let grace_period = date::interval_to_seconds(&grace_period)?;

    let grace_period = extend_grace_period(
        editor,
        json_int(&circ["circ_lib"])?,
        grace_period,
        due_date,
        None,
    )?;

    if backdate < due_date + Duration::seconds(grace_period) {
        log::info!("Backdate {backdate} is within grace period, voiding all");
        Ok(None)
    } else {
        let backdate = backdate + Duration::seconds(fine_interval);
        log::info!("Applying backdate {backdate} in overdue voiding");
        Ok(Some(backdate))
    }
}
