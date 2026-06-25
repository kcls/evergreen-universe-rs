//! Minimal synchronous client for Cloudflare Turnstile siteverify.
//!
//! Turnstile is a standalone CAPTCHA-alternative; it does not require the
//! site to be proxied by Cloudflare.  The browser obtains a response token
//! from the widget and we verify it here, server-side, against the
//! siteverify endpoint using our secret key.
//!
//! # Reference
//! * <https://developers.cloudflare.com/turnstile/get-started/server-side-validation/>

use eg::EgResult;
use evergreen as eg;

const SITEVERIFY_URL: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";

/// Generic error returned to the caller (details are logged).
const TURNSTILE_ERROR: &str = "CAPTCHA verification error";

#[derive(Debug, serde::Deserialize)]
struct SiteVerifyResponse {
    success: bool,
    #[serde(default, rename = "error-codes")]
    error_codes: Vec<String>,
}

/// Verify a Turnstile response token against Cloudflare.
///
/// Returns Ok(true) when Cloudflare confirms the token, Ok(false) when it
/// rejects it, and Err on a configuration or transport failure.  The
/// TURNSTILE_SECRET env var supplies the secret key.
pub fn verify(response: &str, remoteip: Option<&str>) -> EgResult<bool> {
    let secret = std::env::var("TURNSTILE_SECRET").map_err(|_| {
        log::error!("Missing TURNSTILE_SECRET env var");
        TURNSTILE_ERROR
    })?;

    let mut params: Vec<(&str, String)> =
        vec![("secret", secret), ("response", response.to_string())];

    if let Some(ip) = remoteip {
        params.push(("remoteip", ip.to_string()));
    }

    let client = reqwest::blocking::Client::new();

    let result = client
        .post(SITEVERIFY_URL)
        .form(&params)
        .send()
        .map_err(|e| {
            log::error!("Turnstile request failed: {e}");
            TURNSTILE_ERROR
        })?;

    let body = result.text().map_err(|e| {
        log::error!("Turnstile response read failed: {e}");
        TURNSTILE_ERROR
    })?;

    let parsed: SiteVerifyResponse = serde_json::from_str(&body).map_err(|e| {
        log::error!("Turnstile parse failed: {e}; body={body}");
        TURNSTILE_ERROR
    })?;

    if !parsed.success {
        log::warn!("Turnstile verification rejected: {:?}", parsed.error_codes);
    }

    Ok(parsed.success)
}
