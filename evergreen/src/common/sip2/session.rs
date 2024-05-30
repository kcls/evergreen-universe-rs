use crate as eg;
use eg::EgValue;
use eg::EgResult;
use eg::Editor;
use eg::common::auth;
use eg::osrf::cache::Cache;
use std::collections::HashMap;
use std::fmt;

// TODO session auth caching and storage.
//
// propose removing sip.session from DB.
//
// Maybe create new cache type "extended" for storing longer-term data
// like sip sessions and persist auth tokens.. or just extend the max
// cache time on the default global cache, maybe just a day or two.
//
// sip clients will reconnect if disconnected due to expired session
// data or loss of cache server. auth type can still be "staff" since
// backend will re-login as needed.
//
// Can get rid of the "persist" flag on sip.account
//
// sip sessions are removed from the cache by the mediator every
// time a client disconnects so they won't linger for long time
// in the cache.
//
// current/Perl implementation is compliated/over-engineered and
// opens the door to leaving sip session data in the database indefinitely
// on mediator error since there's no timeout mechanism there.

const CACHE_PFX: &str = "sip2";

const SUPPORTED_MESSAGES_LEN: usize = 16;

pub type SupportedMessages = [&'static str; SUPPORTED_MESSAGES_LEN];

/// Supported Messages (BX)
///
/// Currently hard-coded, since it's based on availabilty of
/// functionality in the code, but it could be moved into the database
/// to limit access for specific setting groups.
const INSTITUTION_SUPPORTS: [&'static str; SUPPORTED_MESSAGES_LEN] = [
    "Y", // patron status request,
    "Y", // checkout,
    "Y", // checkin,
    "N", // block patron,
    "Y", // acs status,
    "N", // request sc/acs resend,
    "Y", // login,
    "Y", // patron information,
    "N", // end patron session,
    "Y", // fee paid,
    "Y", // item information,
    "N", // item status update,
    "N", // patron enable,
    "N", // hold,
    "Y", // renew,
    "N", // renew all,
];

pub struct SipFilter {
    /// 2-character SIP field code.
    identifier: String,

    /// Remove the entire field
    strip: bool,

    /// Replace the content of the field with this value.
    replace_with: Option<String>,
}

impl SipFilter {
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
    pub fn strip(&self) -> bool {
        self.strip
    }
    pub fn replace_with(&self) -> Option<&str> {
        self.replace_with.as_deref()
    }
}

pub struct Config {
    institution: String,
    supports: [&'static str; SUPPORTED_MESSAGES_LEN],
    settings: HashMap<String, EgValue>,
    filters: Vec<SipFilter>,
}

impl Config {
    pub fn institution(&self) -> &str {
        &self.institution
    }
    pub fn supports(&self) -> &[&'static str; SUPPORTED_MESSAGES_LEN] {
        &self.supports
    }
    pub fn settings(&self) -> &HashMap<String, EgValue> {
        &self.settings
    }
    pub fn filters(&self) -> &Vec<SipFilter> {
        &self.filters
    }
}

pub struct Session {
    editor: Editor,
    seskey: String,
    sip_account: EgValue,
    config: Option<Config>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Session ({}) [{}]", self.seskey, self.sip_account["sip_username"].str().unwrap())
    }
}

impl Session {
    pub fn new(editor: &Editor, seskey: &str, sip_account: EgValue) -> Self {
        Session {
            editor: editor.clone(),
            seskey: seskey.to_string(),
            sip_account,
            config: None,
        }
    }

    pub fn editor(&mut self) ->  &mut Editor {
        &mut self.editor
    }

    pub fn seskey(&self) -> &str {
        &self.seskey
    }

    pub fn sip_account(&self) -> &EgValue {
        &self.sip_account
    }

    pub fn config(&self) -> Option<&Config> {
        self.config.as_ref()
    }

    pub fn load_config(&mut self) -> EgResult<()> {
        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": {
                "sipsetg": ["settings", "filters"]
            }
        };

        let group = self.editor.retrieve_with_ops(
            "sipsetg",
            self.sip_account["setting_group"].int()?,
            flesh
        )?.ok_or_else(|| self.editor.die_event())?;

        let mut config = Config {
            institution: group["institution"].string()?,
            supports: INSTITUTION_SUPPORTS,
            settings: HashMap::new(),
            filters: Vec::new(),
        };

        for setting in group["settings"].members() {
            config.settings.insert(
                setting["name"].string()?,
                EgValue::parse(setting["value"].str()?)?,
            );
        };

        for filter in group["filters"].members() {
            if filter["enabled"].boolish() {
                let f = SipFilter {
                    identifier: filter["identifier"].string()?,
                    strip: filter["strip"].boolish(),
                    replace_with: filter["replace_with"].as_str().map(|v| v.to_string()),
                };

                config.filters.push(f);
            }
        }

        log::debug!("{self} loaded settings: {:?}", config.settings);

        Ok(())
    }

    /// Load the session from the cache by session key.
    pub fn from_cache(editor: &mut Editor, seskey: &str) -> EgResult<Option<Session>> {
        let mut cached = match Cache::get_global(&format!("{CACHE_PFX}:{seskey}"))? {
            Some(c) => c,
            None => {
                log::warn!("No SIP session found for key {seskey}");
                return Ok(None);
            }
        };

        let sip_account = cached["sip_account"].take();

        let auth_token = cached["ils_token"].as_str()
            .ok_or_else(|| format!("Cached session has no authtoken string"))?;

        let mut session = Session::new(editor, seskey, sip_account);
        session.editor.set_authtoken(auth_token);

        // Make sure our auth session is still valid and set the 'requestor'
        // value on our editor.
        if !session.editor.checkauth()? {
            session.refresh_auth_token()?;
        }

        return Ok(Some(session))
    }

    /// Put this session in to the cache.
    pub fn to_cache(&self) -> EgResult<()> {
        let authtoken = self.editor.authtoken()
            .ok_or_else(|| format!("Cannot cache session with no authoken"))?;

        let cache_val = eg::hash! {
            sip_account: self.sip_account.clone(),
            ils_token: authtoken,
        };

        // Cache the session using the default max cache time.
        Cache::set_global(&format!("{CACHE_PFX}:{}", self.seskey), cache_val)
    }

    /// Get a new authtoken from the ILS.
    ///
    /// This is necessary when creating a new session or when a session
    /// is pulled from the cache and its authtoken has expired.
    fn refresh_auth_token(&mut self) -> EgResult<()> {
        let user_id = self.sip_account["usr"].int()?;

        let mut auth_args = auth::InternalLoginArgs::new(user_id, auth::LoginType::Staff);

        if let Some(ws) = self.sip_account["workstation"]["name"].as_str() {
            auth_args.set_workstation(ws);
        }

        let auth_ses = auth::Session::internal_session_api(self.editor.client_mut(), &auth_args)?
            .ok_or_else(|| format!("Cannot create internal auth session for usr {user_id}"))?;

        self.editor.set_authtoken(auth_ses.token());

        if !self.editor.checkauth()? {
            Err(format!("Cannot verify new authtoken?").into())
        } else {
            Ok(())
        }
    }
}

