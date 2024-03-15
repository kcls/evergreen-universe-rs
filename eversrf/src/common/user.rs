//! Shared, user-focused utility functions
use crate::editor::Editor;
use crate::idl;
use crate::result::EgResult;
use crate::util;
use crate::util::json_int;
use EgValue;
use md5;

pub const PW_TYPE_MAIN: &str = "main";

/// Returns result of True if the password provides matches the user's password.
///
/// # Arguments
///
/// * 'is_hashed' - Set to true if the password has already been md5-hashed.
pub fn verify_migrated_password(
    e: &mut Editor,
    user_id: i64,
    password: &str,
    is_hashed: bool,
) -> EgResult<bool> {
    let mut computed: Option<String> = None;

    if !is_hashed {
        // Only compute / allocate a new String if required.
        computed = Some(format!("{:x}", md5::compute(password)));
    }

    let pass_hash = computed.as_deref().unwrap_or(password);

    let query = eg::hash! {
        from: [
            "actor.get_salt",
            user_id,
            PW_TYPE_MAIN,
        ]
    };

    let salt_list = e.json_query(query)?;

    if let Some(hash) = salt_list.get(0) {
        if let Some(salt) = hash["actor.get_salt"].as_str() {
            let combined = format!("{}{}", salt, pass_hash);
            let digested = format!("{:x}", md5::compute(combined));

            return verify_password(e, user_id, &digested, PW_TYPE_MAIN);
        }
    }

    Ok(false)
}

/// Returns result of True if the password provided matches the user's password.
///
/// Passwords are tested as-is without any additional hashing.
pub fn verify_password(
    e: &mut Editor,
    user_id: i64,
    password: &str,
    pw_type: &str,
) -> EgResult<bool> {
    let query = eg::hash! {
        from: [
            "actor.verify_passwd",
            user_id,
            pw_type,
            password
        ]
    };

    let verify = e.json_query(query)?;

    if let Some(resp) = verify.get(0) {
        Ok(util::json_bool(&resp["actor.verify_passwd"]))
    } else {
        Err(format!("actor.verify_passwd failed to return a response").into())
    }
}

/// Returns a list of all org unit IDs where the provided user has
/// the provided work permission.
pub fn has_work_perm_at(e: &mut Editor, user_id: i64, perm: &str) -> EgResult<Vec<i64>> {
    let dbfunc = "permission.usr_has_perm_at_all";

    let query = eg::hash! { from: [dbfunc, user_id, perm] };

    let values = e.json_query(query)?;

    let mut orgs: Vec<i64> = Vec::new();
    for value in values.iter() {
        let org = util::json_int(&value[dbfunc])?;
        orgs.push(org);
    }

    Ok(orgs)
}

/// Returns counts of items out, overdue, etc. for a user.
pub fn open_checkout_counts(e: &mut Editor, user_id: i64) -> EgResult<EgValue> {
    match e.retrieve("ocirccount", user_id)? {
        Some(mut c) => {
            c["total_out"] = EgValue::from(json_int(&c["out"])? + json_int(&c["overdue"])?);
            idl::unbless(&mut c);
            Ok(c)
        }
        None => {
            // There will be no response if the user has no open circs.
            Ok(eg::hash! {
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

/// Returns a summary of fines owed by a user
pub fn fines_summary(e: &mut Editor, user_id: i64) -> EgResult<EgValue> {
    let mut fines_list = e.search("mous", eg::hash! {usr: user_id})?;

    if let Some(mut fines) = fines_list.pop() {
        idl::unbless(&mut fines);
        Ok(fines)
    } else {
        // Not all users have a fines summary row in the database.
        Ok(eg::hash! {
            balance_owed: 0,
            total_owed: 0,
            total_paid: 0,
            usr: user_id
        })
    }
}

/// Returns a total/ready hold counts for a user.
pub fn active_hold_counts(e: &mut Editor, user_id: i64) -> EgResult<EgValue> {
    let query = eg::hash! {
        select: {ahr: ["pickup_lib", "current_shelf_lib", "behind_desk"]},
        from: "ahr",
        where: {
            usr: user_id,
            fulfillment_time: EgValue::Null,
            cancel_time: EgValue::Null,
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

    Ok(eg::hash! {total: total, ready: ready})
}
