//! CAPTCHA-minted session tokens.
//!
//! A token is minted after a successful Turnstile verification and is then
//! required on every sensitive/tracked address API call (and, eventually,
//! on patron registration).  Tokens are stored in the shared "global"
//! memcache so any drone -- and the Perl register service -- can validate
//! them.
//!
//! Each token carries a hard expiry (TTL) and a cap on the number of
//! sensitive calls it may authorize; exceeding either invalidates it and
//! forces the client to solve a fresh CAPTCHA.

use eg::EgResult;
use eg::osrf::cache::Cache;
use evergreen as eg;

/// Token lifetime in seconds (10 minutes).
const SESSION_TTL: u32 = 600;

/// Maximum number of sensitive/tracked calls a single token may authorize.
const MAX_USES: i64 = 100;

/// Cache key prefix for session token records.
const CACHE_PREFIX: &str = "kcls.captcha.session.";

/// Env var that, when truthy, makes verify() reject invalid tokens.  While
/// false (the default) verify() logs but allows the call, so the service
/// can be deployed before the UI mints real tokens.
const ENFORCE_VAR: &str = "KCLS_CAPTCHA_ENFORCE";

/// Current unix epoch seconds.
fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn token_key(token: &str) -> String {
    format!("{CACHE_PREFIX}{token}")
}

/// Whether token verification is enforced (vs. log-only).
fn enforced() -> bool {
    matches!(
        std::env::var(ENFORCE_VAR).ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// Generate a random, hard-to-guess token.
fn new_token() -> String {
    use rand::Rng;
    use rand::distributions::Alphanumeric;

    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

/// Mint a new session token and return it along with its lifetime (seconds).
pub fn create() -> EgResult<(String, u32)> {
    let token = new_token();
    let now = now_epoch();

    let record = eg::hash! {
        "uses": 0,
        "issued_at": now,
        "expires_at": now + SESSION_TTL as i64,
    };

    Cache::set_global_for(&token_key(&token), record, SESSION_TTL)?;

    Ok((token, SESSION_TTL))
}

/// Validate a session token for one sensitive/tracked call.
///
/// When enforcement is disabled, a rejected token is logged but allowed so
/// the service can run before the UI is wired to mint tokens.
pub fn verify(token: &str) -> EgResult<()> {
    match check_and_consume(token) {
        Ok(()) => Ok(()),
        Err(e) => {
            if enforced() {
                Err(e)
            } else {
                log::warn!("Session token rejected (enforcement disabled): {e}");
                Ok(())
            }
        }
    }
}

/// Validate the token, count the use against its cap, and persist the
/// updated record (preserving the original expiry).
fn check_and_consume(token: &str) -> EgResult<()> {
    let key = token_key(token);

    let Some(mut record) = Cache::get_global(&key)? else {
        return Err("Invalid or expired session token".into());
    };

    let now = now_epoch();
    let expires_at = record["expires_at"].as_i64().unwrap_or(0);

    if expires_at <= now {
        Cache::del_global(&key)?;
        return Err("Session token has expired".into());
    }

    let uses = record["uses"].as_i64().unwrap_or(0) + 1;
    if uses > MAX_USES {
        Cache::del_global(&key)?;
        return Err("Session token call limit exceeded".into());
    }

    record["uses"] = uses.into();

    // Re-store with the remaining lifetime so counting a use does not slide
    // the original expiry forward.
    let remaining = (expires_at - now) as u32;
    Cache::set_global_for(&key, record, remaining)?;

    Ok(())
}
