use eg::editor::Editor;
use eg::util::json_int;
use evergreen as eg;
use json::JsonValue;

/// Returns counts of items out, overdue, etc. for a given user.
pub fn user_open_checkout_counts(e: &mut Editor, user_id: i64) -> Result<JsonValue, String> {
    match e.retrieve("ocirccount", user_id)? {
        Some(mut c) => {
            c["total_out"] = json::from(json_int(&c["out"])? + json_int(&c["overdue"])?);
            eg::idl::unbless(&mut c);
            Ok(c)
        }
        None => {
            // There will be no response if the user has no open circs.
            Ok(json::object! {
                out: 0,
                overdue: 0,
                lost: 0,
                claims_returned: 0,
                long_overdue: 0,
                total_count: 0,
            })
        }
    }
}

/// Returns a summary of fines owed by the patron.
pub fn user_fines_summary(e: &mut Editor, user_id: i64) -> Result<JsonValue, String> {
    let mut fines_list = e.search("mous", json::object! {usr: user_id})?;

    if let Some(mut fines) = fines_list.pop() {
        eg::idl::unbless(&mut fines);
        Ok(fines)
    } else {
        // Not all users have a fines summary row in the database.
        Ok(json::object! {
            balance_owed: 0,
            total_owed: 0,
            total_paid: 0,
            usr: user_id
        })
    }
}

/// Returns a total/ready hold counts for a given user.
pub fn user_active_hold_counts(e: &mut Editor, user_id: i64) -> Result<JsonValue, String> {
    let query = json::object! {
        select: {ahr: ["pickup_lib", "current_shelf_lib", "behind_desk"]},
        from: "ahr",
        where: {
            usr: user_id,
            fulfillment_time: JsonValue::Null,
            cancel_time: JsonValue::Null,
        }
    };

    let holds = e.json_query(query)?;
    let total = holds.len();
    let mut ready = 0;

    for hold in holds.iter().filter(|h| !h["current_shelf_lib"].is_null()) {
        let pickup_lib = json_int(&hold["pickup_lib"])?;
        let shelf_lib = json_int(&hold["current_shelf_lib"])?;

        // A hold is ready for pickup if its current shelf location is
        // the pickup location.
        if pickup_lib == shelf_lib {
            ready += 1;
        }
    }

    Ok(json::object! {total: total, ready: ready})
}
