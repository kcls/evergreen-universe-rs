use crate as eg;
use chrono::Duration;
use eg::common::org;
use eg::common::penalty;
use eg::common::settings::Settings;
use eg::constants as C;
use eg::date;
use eg::editor::Editor;
use eg::result::EgResult;
use eg::util;
use eg::EgValue;
use std::cmp::Ordering;
use std::collections::HashSet;

const DAY_OF_SECONDS: i64 = 86400;

/// Void a list of billings.
pub fn void_bills(
    editor: &mut Editor,
    billing_ids: &[i64], // money.billing.id
    maybe_note: Option<&str>,
) -> EgResult<()> {
    let mut bills = editor.search("mb", eg::hash! {"id": billing_ids})?;
    let mut penalty_users: HashSet<(i64, i64)> = HashSet::new();

    if bills.len() == 0 {
        Err(format!("No such billings: {billing_ids:?}"))?;
    }

    for mut bill in bills.drain(0..) {
        if bill["voided"].boolish() {
            log::debug!("Billing {} already voided.  Skipping", bill["id"]);
            continue;
        }

        let xact = editor.retrieve("mbt", bill["xact"].clone())?;
        let xact = match xact {
            Some(x) => x,
            None => Err(editor.die_event())?,
        };

        let xact_org = xact_org(editor, xact.id()?)?;
        let xact_user = xact["usr"].int()?;
        let xact_id = xact.id()?;

        penalty_users.insert((xact_user, xact_org));

        bill["voided"] = "t".into();
        bill["voider"] = editor.requestor_id()?.into();
        bill["void_time"] = "now".into();

        if let Some(orig_note) = bill["note"].as_str() {
            if let Some(new_note) = maybe_note {
                bill["note"] = format!("{}\n{}", orig_note, new_note).into();
            }
        } else if let Some(new_note) = maybe_note {
            bill["note"] = new_note.into();
        }

        editor.update(bill)?;
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

    let zero_owed = mbts["balance_owed"].float()? == 0.0;
    let xact_open = xact["xact_finish"].is_null();

    if zero_owed {
        if xact_open && no_circ_or_complete {
            // If nothing is owed on the transaction, but it is still open,
            // and this transaction is not an open circulation, close it.

            log::info!("Closing completed transaction {xact_id} on zero balance");
            xact["xact_finish"] = "now".into();
            return editor.update(xact);
        }
    } else if !xact_open {
        // Transaction closed but money or refund still owed.

        if !zero_owed && !xact_open {
            log::info!("Re-opening transaction {xact_id} on non-zero balance");
            xact["xact_finish"] = EgValue::Null;
            return editor.update(xact);
        }
    }

    Ok(())
}

/// Returns the context org unit ID for a transaction (by ID).
pub fn xact_org(editor: &mut Editor, xact_id: i64) -> EgResult<i64> {
    // There's a view for that!
    // money.billable_xact_summary_location_view
    if let Some(sum) = editor.retrieve("mbtslv", xact_id)? {
        sum["billing_location"].int()
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
) -> EgResult<EgValue> {
    log::info!("System is charging ${amount} [btype={btype_id}:{btype_label}] on xact {xact_id}");

    let note = maybe_note.unwrap_or("SYSTEM GENERATED");

    let bill = eg::hash! {
        "xact": xact_id,
        "amount": amount,
        "period_start": period_start,
        "period_end": period_end,
        "billing_type": btype_label,
        "btype": btype_id,
        "note": note,
    };

    let bill = EgValue::create("mb", bill)?;
    editor.create(bill)
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
    let query = eg::hash! {"xact": xact_id, "btype": btype_id};
    let bills = editor.search("mb", query)?;

    if bills.len() == 0 {
        return Ok(());
    }

    let bill_ids: Vec<i64> = bills
        .iter()
        .map(|b| b.id().expect("Billing has ID"))
        .collect();

    // "lost" settings are checked first for backwards compat /
    // consistency with Perl.
    let prohibit_neg_balance = settings
        .get_value_at_org("bill.prohibit_negative_balance_on_lost", context_org)?
        .boolish()
        || settings
            .get_value_at_org("bill.prohibit_negative_balance_default", context_org)?
            .boolish();

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
    let mut bills = editor.search("mb", eg::hash! {"id": bill_ids})?;
    if bills.len() == 0 {
        return Ok(());
    }

    let xact_id = bills[0]["xact"].int()?;

    let flesh = eg::hash! {
        "flesh": 2,
        "flesh_fields": {
            "mbt": ["grocery", "circulation"],
            "circ": ["target_copy"]
        }
    };

    let mbt = editor
        .retrieve_with_ops("mbt", xact_id, flesh)?
        .expect("Billing has no transaction?");

    let user_id = mbt["usr"].int()?;
    let mut bill_maps = bill_payment_map_for_xact(editor, xact_id)?;

    let xact_total = match bill_maps
        .iter()
        .map(|m| m.bill["amount"].float().unwrap())
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
        let payment = eg::hash! {
            "amount": amount_to_adjust,
            "amount_collected": amount_to_adjust,
            "xact": xact_id,
            "accepting_usr": editor.requestor_id()?,
            "payment_ts": "now",
            "billing": bill["id"].clone(),
            "note": note,
        };

        let payment = EgValue::create("maa", payment)?;

        let payment = editor.create(payment)?;

        // Adjust our bill_payment_map
        map.adjustment_amount += amount_to_adjust;
        map.adjustments.push(payment);

        // Should come to zero:
        let new_bill_amount = util::fpdiff(bill["amount"].float()?, amount_to_adjust);
        bill["amount"] = new_bill_amount.into();
    }

    check_open_xact(editor, xact_id)?;

    let org_id = xact_org(editor, xact_id)?;
    penalty::calculate_penalties(editor, user_id, org_id, None)?;

    Ok(())
}

pub struct BillPaymentMap {
    /// The adjusted bill object
    pub bill: EgValue,
    /// List of account adjustments that apply directly to the bill.
    pub adjustments: Vec<EgValue>,
    /// List of payment objects applied to the bill
    pub payments: Vec<EgValue>,
    /// original amount from the billing object
    pub bill_amount: f64,
    /// Total of account adjustments that apply to the bill.
    pub adjustment_amount: f64,
}

pub fn bill_payment_map_for_xact(
    editor: &mut Editor,
    xact_id: i64,
) -> EgResult<Vec<BillPaymentMap>> {
    let query = eg::hash! {
        "xact": xact_id,
        "voided": "f",
    };
    let ops = eg::hash! {
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
        let amount = bill["amount"].float()?;

        let map = BillPaymentMap {
            bill,
            adjustments: Vec::new(),
            payments: Vec::new(),
            bill_amount: amount,
            adjustment_amount: 0.00,
        };

        maps.push(map);
    }

    let query = eg::hash! {"xact": xact_id, "voided": "f"};

    let ops = eg::hash! {
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
        if b["amount"].float().unwrap() < a["amount"].float().unwrap() {
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
        let mut my_adjustments: Vec<&mut EgValue> = payments
            .iter_mut()
            .filter(|p| p["payment_type"].as_str().unwrap() == "account_adjustment")
            .filter(|p| used_adjustments.contains(&p["account_adjustment"].id().unwrap()))
            .filter(|p| p["account_adjustment"]["billing"] == bill["id"])
            .map(|p| &mut p["account_adjustment"])
            .collect();

        if my_adjustments.len() == 0 {
            continue;
        }

        for adjustment in my_adjustments.drain(0..) {
            let adjust_amount = adjustment["amount"].float()?;
            let adjust_id = adjustment["id"].int()?;

            let new_amount = util::fpdiff(bill["amount"].float()?, adjust_amount);

            if new_amount >= 0.0 {
                map.adjustments.push(adjustment.clone());
                map.adjustment_amount += adjust_amount;
                bill["amount"] = new_amount.into();
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
                map.adjustment_amount += new_adjustment["amount"].float()?;
                bill["amount"] = 0.0.into();
                adjustment["amount"] = EgValue::from(-new_amount);
            }

            if bill["amount"].float()? == 0.0 {
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
                    && !used_payments.contains(&payment.id().unwrap())
            })
            .next()
        {
            Some(m) => m,
            None => continue,
        };

        map.bill["amount"] = EgValue::from(0.0);
        map.payments.push(payment.clone());
        used_payments.insert(payment.id()?);
    }

    // Remove the used payments from our working list.
    let mut new_payments = Vec::new();
    for pay in payments.drain(0..) {
        if !used_payments.contains(&pay.id()?) {
            new_payments.push(pay);
        }
    }
    payments = new_payments;
    let mut used_payments = HashSet::new();

    // Map remaining bills to payments in whatever order.
    for map in maps
        .iter_mut()
        .filter(|m| m.bill["amount"].float().unwrap() > 0.0)
    {
        let bill = &mut map.bill;
        // Loop over remaining unused / unmapped payments.
        for pay in payments
            .iter_mut()
            .filter(|p| !used_payments.contains(&p.id().unwrap()))
        {
            loop {
                let bill_amount = bill["amount"].float()?;
                if bill_amount > 0.0 {
                    let new_amount = util::fpdiff(bill_amount, pay["amount"].float()?);
                    if new_amount < 0.0 {
                        let mut new_payment = pay.clone();
                        new_payment["amount"] = EgValue::from(bill_amount);
                        bill["amount"] = EgValue::from(0.0);
                        map.payments.push(new_payment);
                        pay["amount"] = EgValue::from(-new_amount);
                    } else {
                        bill["amount"] = EgValue::from(new_amount);
                        map.payments.push(pay.clone());
                        used_payments.insert(pay.id()?);
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
    let query = eg::hash! {
        "xact": xact_id,
        "payment_type": {"!=": "account_adjustment"}
    };

    let ops = eg::hash! {
        "limit": 1,
        "order_by": {"mp": "payment_ts DESC"}
    };

    let last_payment = editor.search_with_ops("mp", query, ops)?;

    if last_payment.len() == 0 {
        return Ok(false);
    }

    let payment = &last_payment[0];

    // Every payment has a payment_ts value
    let payment_ts = &payment["payment_ts"].as_str().unwrap();
    let payment_dt = date::parse_datetime(payment_ts)?;

    let window_start = date::subtract_interval(date::now(), interval)?;

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
        .ok_or_else(|| editor.die_event())?;

    let fine_interval = match resv["fine_interval"].as_str() {
        Some(f) => f,
        None => return Ok(()),
    };

    generate_fines_for_xact(
        editor,
        resv_id,
        resv["end_time"].as_str().unwrap(),
        resv["pickup_lib"].int()?,
        resv["fine_amount"].float()?,
        fine_interval,
        resv["max_fine"].float()?,
        None, // grace period
        BillableTransactionType::Reservation,
    )
}

pub fn generate_fines_for_circ(editor: &mut Editor, circ_id: i64) -> EgResult<()> {
    log::info!("Generating fines for circulation {circ_id}");

    let circ = editor
        .retrieve("circ", circ_id)?
        .ok_or_else(|| editor.die_event())?;

    generate_fines_for_xact(
        editor,
        circ_id,
        circ["due_date"].as_str().unwrap(),
        circ["circ_lib"].int()?,
        circ["recurring_fine"].float()?,
        circ["fine_interval"].str()?,
        circ["max_fine"].float()?,
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

    let fine_interval_secs = date::interval_to_seconds(fine_interval)?;
    let mut grace_period = date::interval_to_seconds(grace_period.unwrap_or("0s"))?;
    let now = date::now();

    if fine_interval_secs == 0 || recurring_fine * 100.0 == 0.0 || max_fine * 100.0 == 0.0 {
        log::info!(
            "Fine generator skipping transaction {xact_id}
            due to 0 fine interval, 0 fine rate, or 0 max fine."
        );
        return Ok(());
    }

    // TODO add the bit about reservation time zone offsets

    let query = eg::hash! {
        "xact": xact_id,
        "btype": C::BTYPE_OVERDUE_MATERIALS,
    };

    let ops = eg::hash! {
        "flesh": 1,
        "flesh_fields": {"mb": ["adjustments"]},
        "order_by": {"mb": "billing_ts DESC"},
    };

    let mut fines = editor.search_with_ops("mb", query, ops)?;
    let mut current_fine_total = 0.0;
    for fine in fines.iter() {
        if !fine["voided"].boolish() {
            current_fine_total += fine["amount"].float()? * 100.0;
        }
        for adj in fine["adjustments"].members() {
            if !adj["voided"].boolish() {
                current_fine_total -= adj["amount"].float()? * 100.0;
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
    let fines: Vec<EgValue> = fines
        .drain(..)
        .filter(|f| f["billing_ts"].as_str().unwrap() > due_date)
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
    let pending_fine_count = (range as f64 / fine_interval_secs as f64).ceil() as i64;

    if pending_fine_count == 0 {
        // No fines to generate.
        return Ok(());
    }

    recurring_fine *= 100.0;
    max_fine *= 100.0;

    let skip_closed_check = settings
        .get_value_at_org("circ.fines.charge_when_closed", circ_lib)?
        .boolish();

    let truncate_to_max_fine = settings
        .get_value_at_org("circ.fines.truncate_to_max_fine", circ_lib)?
        .boolish();

    let timezone = match settings
        .get_value_at_org("lib.timezone", circ_lib)?
        .as_str()
    {
        Some(tz) => tz,
        None => "local",
    };

    for slot in 0..pending_fine_count {
        if current_fine_total >= max_fine && xact_type == BillableTransactionType::Circ {
            log::info!("Max fines reached for circulation {xact_id}");

            if let Some(mut circ) = editor.retrieve("circ", xact_id)? {
                circ["stop_fines"] = EgValue::from("MAXFINES");
                circ["stop_fines_time"] = EgValue::from("now");
                editor.update(circ)?;
                break;
            }
        }

        // Translate the last fine time to the timezone of the affected
        // org unit so the org::next_open_date() calculation below
        // can use the correct day / day of week information, which can
        // vary across timezones.
        let mut period_end = date::set_timezone(last_fine_dt, timezone)?;

        let mut current_bill_count = slot;
        while current_bill_count > 0 {
            period_end = date::add_interval(period_end, fine_interval)?;
            current_bill_count -= 1;
        }

        let duration = Duration::try_seconds(fine_interval_secs - 1)
            .ok_or_else(|| format!("Invalid interval {fine_interval}"))?;

        let period_start = period_end - duration;

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

        let bill = eg::hash! {
            xact: xact_id,
            note: "System Generated Overdue Fine",
            billing_type: "Overdue materials",
            btype: C::BTYPE_OVERDUE_MATERIALS,
            amount: this_billing_amount / 100.0,
            period_start: date::to_iso(&period_start),
            period_end: date::to_iso(&period_end),
        };

        let bill = EgValue::create("mb", bill)?;
        editor.create(bill)?;
    }

    let xact = editor.retrieve("mbt", xact_id)?.unwrap(); // required
    let user_id = xact["usr"].int()?;

    penalty::calculate_penalties(editor, user_id, circ_lib, None)?;

    Ok(())
}

pub fn extend_grace_period(
    editor: &mut Editor,
    context_org: i64,
    grace_period: i64,
    mut due_date: date::EgDate,
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

    let extend = settings
        .get_value_at_org("circ.grace.extend", context_org)?
        .boolish();

    if !extend {
        // No extension configured.
        return Ok(grace_period);
    }

    let extend_into_closed = settings
        .get_value_at_org("circ.grace.extend.into_closed", context_org)?
        .boolish();

    if extend_into_closed {
        // Merge closed dates trailing the grace period into the grace period.
        // Note to self: why add exactly one day?
        due_date = date::add_interval(due_date, "1 day")?;
    }

    let extend_all = settings
        .get_value_at_org("circ.grace.extend.all", context_org)?
        .boolish();

    if extend_all {
        // Start checking the day after the item was due.
        due_date = date::add_interval(due_date, "1 day")?;
    } else {
        // Jump to the end of the grace period.
        due_date = due_date
            + Duration::try_seconds(grace_period)
                .ok_or_else(|| format!("Invalid duration seconds: {grace_period}"))?;
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
        due_date = date::add_interval(due_date, "1 day")?;
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
        .ok_or_else(|| editor.die_event())?;

    let mut query = eg::hash! {
        "xact": circ_id,
        "btype": C::BTYPE_OVERDUE_MATERIALS,
    };

    if let Some(bd) = backdate {
        if note.is_none() {
            note = Some("System: OVERDUE REVERSED FOR BACKDATE");
        }
        if let Some(min_date) = calc_min_void_date(editor, &circ, bd)? {
            query["billing_ts"] = eg::hash! {">=": date::to_iso(&min_date) };
        }
    }

    let circ_lib = circ["circ_lib"].int()?;
    let bills = editor.search("mb", query)?;

    if bills.len() == 0 {
        // Nothing to void/zero.
        return Ok(());
    }

    let bill_ids: Vec<i64> = bills.iter().map(|b| b.id().expect("Has ID")).collect();

    let mut settings = Settings::new(&editor);
    let prohibit_neg_balance = settings
        .get_value_at_org("bill.prohibit_negative_balance_on_overdue", circ_lib)?
        .boolish()
        || settings
            .get_value_at_org("bill.prohibit_negative_balance_default", circ_lib)?
            .boolish();

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
    circ: &EgValue,
    backdate: &str,
) -> EgResult<Option<date::EgDate>> {
    let fine_interval = circ["fine_interval"].str()?;
    let backdate = date::parse_datetime(backdate)?;
    let due_date = date::parse_datetime(circ["due_date"].str()?)?;

    let grace_period = circ["grace_period"].as_str().unwrap_or("0s");
    let grace_period = date::interval_to_seconds(&grace_period)?;

    let grace_period = extend_grace_period(
        editor,
        circ["circ_lib"].int()?,
        grace_period,
        due_date,
        None,
    )?;

    let grace_duration = Duration::try_seconds(grace_period)
        .ok_or_else(|| format!("Invalid duration seconds: {grace_period}"))?;

    if backdate < due_date + grace_duration {
        log::info!("Backdate {backdate} is within grace period, voiding all");
        Ok(None)
    } else {
        let backdate = date::add_interval(backdate, fine_interval)?;
        log::info!("Applying backdate {backdate} in overdue voiding");
        Ok(Some(backdate))
    }
}

/// Get the numeric cost of a copy, honoring various org settings
/// for which field to pull the cost from and how to handle zero/unset
/// cost values.
pub fn get_copy_price(editor: &mut Editor, copy_id: i64) -> EgResult<f64> {
    let flesh = eg::hash! {"flesh": 1, "flesh_fields": {"acp": ["call_number"]}};

    let copy = editor
        .retrieve_with_ops("acp", copy_id, flesh)?
        .ok_or_else(|| editor.die_event())?;

    let owner = if copy["call_number"].id()? == C::PRECAT_CALL_NUMBER {
        copy["circ_lib"].int()?
    } else {
        copy["call_number"]["owning_lib"].int()?
    };

    let mut settings = Settings::new(&editor);
    settings.set_org_id(owner);

    settings.fetch_values(&[
        "circ.min_item_price",
        "circ.max_item_price",
        "circ.charge_lost_on_zero",
        "circ.primary_item_value_field",
        "circ.secondary_item_value_field",
        "cat.default_item_price",
    ])?;

    let primary_field = match settings
        .get_value("circ.primary_item_value_field")?
        .as_str()
    {
        Some("cost") => "cost",
        Some("price") => "price",
        _ => "",
    };

    let secondary_field = match settings
        .get_value("circ.secondary_item_value_field")?
        .as_str()
    {
        Some("cost") => "cost",
        Some("price") => "price",
        _ => "",
    };

    let charge_on_zero_op = settings.get_value("circ.charge_lost_on_zero")?.as_bool();
    let charge_on_zero = if let Some(b) = charge_on_zero_op {
        b
    } else {
        false
    };

    // Retain the price as a json value for now because null is important.
    let mut price = if primary_field == "cost" {
        &copy["cost"]
    } else {
        &copy["price"]
    };

    if (price.is_null() || (price.float()? == 0.0 && charge_on_zero)) && secondary_field != "" {
        price = &copy[secondary_field];
    }

    // Fall back to legacy item cost calculation
    let price_binding;
    if price.is_null() || (price.float()? == 0.0 && charge_on_zero) {
        let def_price = match settings.get_value("cat.default_item_price")?.as_f64() {
            Some(p) => p,
            _ => 0.0,
        };
        price_binding = Some(EgValue::from(def_price));
        price = price_binding.as_ref().unwrap();
    }

    // Now we want numbers
    let mut price = if let Ok(p) = price.float() { p } else { 0.00 };

    if let Some(max_price) = settings.get_value("circ.max_item_price")?.as_f64() {
        if price > max_price {
            price = max_price;
        }
    } else if let Some(min_price) = settings.get_value("circ.min_item_price")?.as_f64() {
        // Only let $0 fall through if charge_on_zero is explicitly false.
        if price < min_price && (price != 0.0 || charge_on_zero || charge_on_zero_op.is_none()) {
            price = min_price;
        }
    }

    Ok(price)
}
