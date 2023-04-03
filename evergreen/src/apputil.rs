use super::editor::Editor;
use super::util;
use md5;

pub const PW_TYPE_MAIN: &str = "main";

/// Returns result of True if the password provides matches the user's password.
///
/// # Arguments
///
/// * 'is_hashed' - Set to true if the password has already been md5-hashed.
pub fn verify_migrated_user_password(
    e: &mut Editor,
    user_id: i64,
    password: &str,
    is_hashed: bool,
) -> Result<bool, String> {
    let mut computed: Option<String> = None;

    if !is_hashed {
        // Only compute / allocate a new String if required.
        computed = Some(format!("{:x}", md5::compute(password)));
    }

    let pass_hash = computed.as_deref().unwrap_or(password);

    let query = json::object! {
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

            return verify_user_password(e, user_id, &digested, PW_TYPE_MAIN);
        }
    }

    Ok(false)
}

/// Returns result of True if the password provided matches the user's password.
///
/// Passwords are tested as-is without any additional hashing.
pub fn verify_user_password(
    e: &mut Editor,
    user_id: i64,
    password: &str,
    pw_type: &str,
) -> Result<bool, String> {
    let query = json::object! {
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
        Err(format!("actor.verify_passwd failed to return a response"))
    }
}

/// Returns a list of all org unit IDs where the provided user has
/// the provided work permission.
pub fn user_has_work_perm_at(e: &mut Editor, user_id: i64, perm: &str) -> Result<Vec<i64>, String> {
    let dbfunc = "permission.usr_has_perm_at_all";

    let query = json::object! { from: [dbfunc, user_id, perm] };

    let values = e.json_query(query)?;

    let mut orgs: Vec<i64> = Vec::new();
    for value in values.iter() {
        let org = util::json_int(&value[dbfunc])?;
        orgs.push(org);
    }

    Ok(orgs)
}
