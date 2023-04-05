use eg::editor::Editor;
use evergreen as eg;
use json::JsonValue;

pub fn user_open_checkout_counts(e: &mut Editor, user_id: i64) -> Result<JsonValue, String> {
    match e.retrieve("ocirccount", user_id)? {
        Some(mut c) => {
            c["total_out"] =
                json::from(eg::util::json_int(&c["out"])? + eg::util::json_int(&c["overdue"])?);
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

pub fn user_fines_summary(e: &mut Editor, user_id: i64) -> Result<JsonValue, String> {
    let mut fines_list = e.search("mous", json::object! {usr: user_id})?;

    if let Some(mut fines) = fines_list.pop() {
        eg::idl::unbless(&mut fines);
        Ok(fines)
    } else {
        // Not all users have a fines summary row in the database.
        // When not, create a dummy version.
        let mut f = e.idl().create("mous")?;

        f["balance_owed"] = json::from(0.00);
        f["total_owed"] = json::from(0.00);
        f["total_paid"] = json::from(0.00);
        f["usr"] = json::from(user_id);

        Ok(f)
    }
}
