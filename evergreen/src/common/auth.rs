use crate as eg;
use eg::common::settings::Settings;
use eg::constants as C;
use eg::date;
use eg::osrf::cache::Cache;
use eg::osrf::sclient::HostSettings;
use eg::util;
use eg::{Client, Editor, EgError, EgEvent, EgResult, EgValue};
use md5;
use std::fmt;

const LOGIN_TIMEOUT: i32 = 30;

// Default time for extending a persistent session: ten minutes
const DEFAULT_RESET_INTERVAL: i32 = 10 * 60;

fn cache_key(token: &str) -> String {
    format!("{}{}", C::OILS_AUTH_CACHE_PRFX, token)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoginType {
    Temp,
    Opac,
    Staff,
    Persist,
}

impl TryFrom<&str> for LoginType {
    type Error = EgError;
    fn try_from(s: &str) -> EgResult<LoginType> {
        match s {
            "opac" => Ok(Self::Opac),
            "staff" => Ok(Self::Staff),
            "persist" => Ok(Self::Persist),
            "temp" => Ok(Self::Temp),
            _ => Err(format!("Invalid login type: {s}. Using temp instead").into()),
        }
    }
}

impl From<&LoginType> for &str {
    fn from(lt: &LoginType) -> &'static str {
        match *lt {
            LoginType::Temp => "temp",
            LoginType::Opac => "opac",
            LoginType::Staff => "staff",
            LoginType::Persist => "persist",
        }
    }
}

impl fmt::Display for LoginType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: &str = (self).into();
        write!(f, "{}", s)
    }
}

pub struct LoginArgs {
    pub username: String,
    pub password: String,
    pub login_type: LoginType,
    pub workstation: Option<String>,
}

impl LoginArgs {
    /// # Examples
    /// ```
    /// use evergreen::common::auth;
    /// let args = auth::LoginArgs::new(
    ///   "my-staff-username",
    ///   "my-staff-password",
    ///   auth::LoginType::Staff,
    ///   Some("branch1-circ-workstation")
    /// );
    /// assert_eq!(args.username(), "my-staff-username");
    /// ```
    pub fn new(
        username: &str,
        password: &str,
        login_type: impl Into<LoginType>,
        workstation: Option<&str>,
    ) -> Self {
        LoginArgs {
            username: username.to_string(),
            password: password.to_string(),
            login_type: login_type.into(),
            workstation: match workstation {
                Some(w) => Some(w.to_string()),
                _ => None,
            },
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn login_type(&self) -> &LoginType {
        &self.login_type
    }

    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }

    pub fn to_eg_value(&self) -> EgValue {
        let lt: &str = self.login_type().into();

        let mut jv = eg::hash! {
            username: self.username(),
            password: self.password(),
            "type": lt,
        };

        if let Some(w) = &self.workstation {
            jv["workstation"] = EgValue::from(w.as_str());
        }

        jv
    }
}

#[derive(Debug)]
pub struct InternalLoginArgs {
    pub user_id: i64,
    pub org_unit: Option<i64>,
    pub login_type: LoginType,
    pub workstation: Option<String>,
}

impl InternalLoginArgs {
    pub fn new(user_id: i64, login_type: impl Into<LoginType>) -> Self {
        InternalLoginArgs {
            user_id,
            login_type: login_type.into(),
            org_unit: None,
            workstation: None,
        }
    }
    pub fn set_org_unit(&mut self, org_unit: i64) {
        self.org_unit = Some(org_unit);
    }
    pub fn set_workstation(&mut self, workstation: &str) {
        self.workstation = Some(workstation.to_string());
    }

    pub fn to_eg_value(&self) -> EgValue {
        let lt: &str = (&self.login_type).into();

        let mut jv = eg::hash! {
            "login_type": lt,
            "user_id": self.user_id,
        };

        if let Some(w) = &self.workstation {
            jv["workstation"] = EgValue::from(w.as_str());
        }

        if let Some(w) = self.org_unit {
            jv["org_unit"] = EgValue::from(w);
        }

        jv
    }
}

pub struct Session {
    user: EgValue,

    token: String,

    /// Duration of the authentication session
    authtime: u32,

    /// Workstation name if applied
    /// Sessions pulled from the cache will not have a Workstation
    /// value, only "wsid" values.
    workstation: Option<String>,

    /// Epoch seconds where this session expires from cache.
    /// Only applied to Persist sessions.
    endtime: Option<i64>,

    /// Amount to exist Persist sessions.
    reset_interval: Option<i64>,
}

impl Session {
    /// Get the auth session matching the provided auth token.
    ///
    /// Uses our internal cache, not the API.
    pub fn from_cache(token: &str) -> EgResult<Option<Session>> {
        let mut cache_val = match Cache::get_global(&cache_key(token))? {
            Some(v) => v,
            None => return Ok(None),
        };

        let authtime = cache_val["authtime"].int()? as u32;
        let user = cache_val["userobj"].take();
        let endtime = cache_val["endtime"].as_i64();
        let reset_interval = cache_val["reset_interval"].as_i64();

        let ses = Session {
            user,
            authtime,
            endtime,
            reset_interval,
            workstation: None,
            token: token.to_string(),
        };

        Ok(Some(ses))
    }

    pub fn remove(&self) -> EgResult<()> {
        Cache::del_global(&cache_key(self.token()))
    }

    /// Logout and remove the cached auth session.
    pub fn logout(client: &Client, token: &str) -> EgResult<()> {
        let mut ses = client.session("open-ils.auth");
        let mut req = ses.request("open-ils.auth.session.delete", token)?;
        // We don't care so much about the response from logout,
        // only that the call completed OK.
        req.recv_with_timeout(LOGIN_TIMEOUT)?;
        Ok(())
    }

    /// Login and acquire an authtoken.
    ///
    /// Returns None on login failure, Err on error.
    pub fn login(client: &Client, args: &LoginArgs) -> EgResult<Option<Session>> {
        let params = vec![args.to_eg_value()];
        let mut ses = client.session("open-ils.auth");
        let mut req = ses.request("open-ils.auth.login", params)?;

        let eg_val = match req.recv_with_timeout(LOGIN_TIMEOUT)? {
            Some(v) => v,
            None => Err(format!("Login Timed Out"))?,
        };

        Session::handle_auth_response(&args.workstation, &eg_val)
    }

    /// Create an authtoken for an internal auth session via the API.
    ///
    /// Returns None on login failure, Err on error.
    pub fn internal_session_api(
        client: &Client,
        args: &InternalLoginArgs,
    ) -> EgResult<Option<Session>> {
        let params = vec![args.to_eg_value()];
        let mut ses = client.session("open-ils.auth_internal");
        let mut req = ses.request("open-ils.auth_internal.session.create", params)?;

        let eg_val = match req.recv_with_timeout(LOGIN_TIMEOUT)? {
            Some(v) => v,
            None => Err(format!("Login Timed Out"))?,
        };

        Session::handle_auth_response(&args.workstation, &eg_val)
    }

    fn handle_auth_response(
        workstation: &Option<String>,
        response: &EgValue,
    ) -> EgResult<Option<Session>> {
        let mut evt = match EgEvent::parse(&response) {
            Some(e) => e,
            None => {
                return Err(format!("Unexpected response: {:?}", response).into());
            }
        };

        if !evt.is_success() {
            log::warn!("Login failed: {evt:?}");
            return Ok(None);
        }

        if !evt.payload().is_object() {
            return Err(format!("Unexpected response: {}", evt).into());
        }

        let token = evt.payload_mut()["authtoken"]
            .take_string()
            .ok_or_else(|| format!("Auth cache value has invalid authtoken"))?;

        let authtime = evt.payload()["authtime"].int()? as u32;
        let user = evt.payload_mut()["userobj"].take();

        let mut auth_ses = Session {
            user,
            token,
            authtime: authtime,
            workstation: None,
            endtime: None,
            reset_interval: None,
        };

        if let Some(w) = workstation {
            auth_ses.workstation = Some(String::from(w));
        }

        Ok(Some(auth_ses))
    }

    /// Create an internal auth session and store it directly in the cache.
    pub fn internal_session(editor: &mut Editor, args: &InternalLoginArgs) -> EgResult<Session> {
        let mut user = editor
            .retrieve("au", args.user_id)?
            .ok_or_else(|| editor.die_event())?;

        // Clear the (mostly dummy) password values.
        user["passwd"].take();

        if let Some(workstation) = args.workstation.as_deref() {
            let mut ws = editor
                .search("aws", eg::hash! {"name": workstation})?
                .pop()
                .ok_or_else(|| editor.die_event())?;

            user["wsid"] = ws["id"].take();
            user["ws_ou"] = ws["owning_lib"].take();
        } else {
            user["ws_ou"] = user["home_ou"].clone();
        }

        let org_id = match args.org_unit {
            Some(id) => id,
            None => user["ws_ou"].int()?,
        };

        let duration = get_auth_duration(editor, org_id, user["home_ou"].int()?, &args.login_type)?;

        let authtoken = format!("{:x}", md5::compute(util::random_number(20)));

        let mut cache_val = eg::hash! {
            "authtime": duration,
            "userobj": user.clone(),
        };

        if args.login_type == LoginType::Persist {
            // Add entries for endtime and reset_interval, so that we can
            // gracefully extend the session a bit if the user is active
            // toward the end of the duration originally specified.
            cache_val["endtime"] = EgValue::from(date::epoch_secs().floor() as u32 + duration);

            // Reset interval is hard-coded for now, but if we ever want to make it
            // configurable, this is the place to do it:
            cache_val["reset_interval"] = DEFAULT_RESET_INTERVAL.into();
        }

        let endtime = cache_val["endtime"].as_int();
        let reset_interval = cache_val["reset_interval"].as_int();

        Cache::set_global_for(&cache_key(&authtoken), cache_val, duration)?;

        let auth_ses = Session {
            user,
            token: authtoken,
            authtime: duration,
            endtime,
            reset_interval,
            workstation: args.workstation.clone(),
        };

        Ok(auth_ses)
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn authtime(&self) -> u32 {
        self.authtime
    }

    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }

    pub fn endtime(&self) -> Option<i64> {
        self.endtime
    }

    pub fn reset_interval(&self) -> Option<i64> {
        self.reset_interval
    }
    pub fn user(&self) -> &EgValue {
        &self.user
    }
}

/// Returns the auth session duration in seconds for the provided
/// login type, context org unit(s), and host settings.
pub fn get_auth_duration(
    editor: &mut Editor,
    org_id: i64,
    user_home_ou: i64,
    auth_type: &LoginType,
) -> EgResult<u32> {
    // First look for an org unit setting.

    let setting_name = match auth_type {
        LoginType::Opac => "auth.opac_timeout",
        LoginType::Staff => "auth.staff_timeout",
        LoginType::Temp => "auth.temp_timeout",
        LoginType::Persist => "auth.persistent_login_interval",
    };

    let mut settings = Settings::new(editor);
    settings.set_org_id(org_id);

    let mut interval = settings.get_value(setting_name)?;

    if interval.is_null() && user_home_ou != org_id {
        // If the provided context org unit has no setting, see if
        // a setting is applied to the user's home org unit.
        settings.set_org_id(user_home_ou);
        interval = settings.get_value(setting_name)?;
    }

    let interval_binding;
    if interval.is_null() {
        // No org unit setting.  Use the default.

        let setkey = // TODO change key names?
            format!("apps/open-ils.auth_internal/app_settings/default_timeout/{auth_type}");

        interval_binding = HostSettings::get(&setkey)?.clone();
        interval = &interval_binding;
    }

    if let Some(num) = interval.as_int() {
        Ok(num as u32)
    } else if let Some(s) = interval.as_str() {
        date::interval_to_seconds(&s).map(|n| n as u32)
    } else {
        Ok(0)
    }
}
