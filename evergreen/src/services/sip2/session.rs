use eg::common::auth;
use eg::osrf::cache::Cache;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
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
// Can get rid of the "persist" flag on sip.account.  If accounts are
// cache-only, this is unnecesssary.
//
// sip sessions are removed from the cache by the mediator every
// time a client disconnects so they won't linger for long time
// in the cache.
//
// current/Perl implementation is unnecessarily complicated and
// opens the door to leaving sip session data in the database indefinitely
// on mediator error since there's no timeout mechanism there.

const CACHE_PFX: &str = "sip2";

/// Supported Messages (BX)
///
/// By order of appearance in the INSTITUTION_SUPPORTS string:
/// patron status request
/// checkout
/// checkin
/// block patron
/// acs status
/// request sc/acs resend
/// login
/// patron information
/// end patron session
/// fee paid
/// item information
/// item status update
/// patron enable
/// hold
/// renew
/// renew all
const INSTITUTION_SUPPORTS: &str = "YYYYYNYYYYYNNNYY";

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Config {
    institution: String,
    supports: &'static str,
    settings: HashMap<String, EgValue>,
    filters: Vec<SipFilter>,
}

impl Config {
    pub fn institution(&self) -> &str {
        &self.institution
    }
    pub fn supports(&self) -> &'static str {
        self.supports
    }
    pub fn default_supports() -> &'static str {
        INSTITUTION_SUPPORTS
    }
    pub fn settings(&self) -> &HashMap<String, EgValue> {
        &self.settings
    }
    pub fn filters(&self) -> &Vec<SipFilter> {
        &self.filters
    }

    pub fn setting_is_true(&self, name: &str) -> bool {
        if let Some(val) = self.settings.get(name) {
            val.boolish()
        } else {
            false
        }
    }
}

pub struct Session {
    editor: Editor,
    seskey: String,
    sip_account: EgValue,
    config: Config,

    /// Any time we encounter a new org unit, add it here.
    org_cache: HashMap<i64, EgValue>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Session ({}) [{}]",
            self.seskey,
            self.sip_account["sip_username"].str().unwrap()
        )
    }
}

impl Session {
    pub fn new(editor: &Editor, seskey: &str, sip_account: EgValue) -> EgResult<Self> {
        let mut editor = editor.clone();
        let config = Session::load_config(&mut editor, sip_account["setting_group"].int()?)?;

        log::debug!("Session {seskey} loaded config: {:?}", config);

        Ok(Session {
            seskey: seskey.to_string(),
            editor,
            sip_account,
            config,
            org_cache: HashMap::new(),
        })
    }

    pub fn org_cache(&self) -> &HashMap<i64, EgValue> {
        &self.org_cache
    }

    pub fn org_cache_mut(&mut self) -> &mut HashMap<i64, EgValue> {
        &mut self.org_cache
    }

    pub fn editor(&mut self) -> &mut Editor {
        &mut self.editor
    }

    pub fn seskey(&self) -> &str {
        &self.seskey
    }

    pub fn sip_account(&self) -> &EgValue {
        &self.sip_account
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn load_config(editor: &mut Editor, setting_group: i64) -> EgResult<Config> {
        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": {
                "sipsetg": ["settings", "filters"]
            }
        };

        let group = editor
            .retrieve_with_ops("sipsetg", setting_group, flesh)?
            .ok_or_else(|| editor.die_event())?;

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
        }

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

        Ok(config)
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

        let auth_token = cached["ils_token"]
            .as_str()
            .ok_or_else(|| "Cached session has no authtoken string".to_string())?;

        let mut session = Session::new(editor, seskey, sip_account)?;
        session.editor.set_authtoken(auth_token);

        // Make sure our auth session is still valid and set the 'requestor'
        // value on our editor.
        if !session.editor.checkauth()? {
            session.refresh_auth_token()?;
        }

        Ok(Some(session))
    }

    /// Put this session in to the cache.
    pub fn to_cache(&self) -> EgResult<()> {
        let authtoken = self
            .editor
            .authtoken()
            .ok_or_else(|| "Cannot cache session with no authoken".to_string())?;

        let cache_val = eg::hash! {
            sip_account: self.sip_account.clone(),
            ils_token: authtoken,
        };

        // Cache the session using the default max cache time.
        Cache::set_global(&format!("{CACHE_PFX}:{}", self.seskey), cache_val)
    }

    pub fn remove_from_cache(&self) -> EgResult<()> {
        Cache::del_global(&format!("{CACHE_PFX}:{}", self.seskey))
    }

    /// Get a new authtoken from the ILS.
    ///
    /// This is necessary when creating a new session or when a session
    /// is pulled from the cache and its authtoken has expired.
    pub fn refresh_auth_token(&mut self) -> EgResult<()> {
        let user_id = self.sip_account["usr"].int()?;

        let mut auth_args = auth::InternalLoginArgs::new(user_id, auth::LoginType::Staff);

        if let Some(ws) = self.sip_account["workstation"]["name"].as_str() {
            auth_args.set_workstation(ws);
        }

        let auth_ses =
            auth::Session::internal_session_api(self.editor.client_mut(), &auth_args)?
                .ok_or_else(|| format!("Cannot create internal auth session for usr {user_id}"))?;

        self.editor.set_authtoken(auth_ses.token());

        if !self.editor.checkauth()? {
            Err("Cannot verify new authtoken?".to_string().into())
        } else {
            Ok(())
        }
    }
}
