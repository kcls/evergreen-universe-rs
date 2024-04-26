use crate as eg;
use eg::{Editor, EgResult, EgValue, EgError, EgEvent, Client};
use eg::date;
use std::fmt;
use eg::osrf::sclient::HostSettings;
use eg::common::settings::Settings;

const LOGIN_TIMEOUT: i32 = 30;

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
    token: String,
    authtime: usize,
    workstation: Option<String>,
}

impl Session {
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

    /// Create an authtoken for an internal auth session.
    ///
    /// Returns None on login failure, Err on error.
    pub fn internal_session(
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
        let evt = match EgEvent::parse(&response) {
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

        let token = match evt.payload()["authtoken"].as_str() {
            Some(t) => String::from(t),
            None => {
                return Err(format!("Unexpected response: {}", evt).into());
            }
        };

        let authtime = match evt.payload()["authtime"].as_usize() {
            Some(t) => t,
            None => {
                return Err(format!("Unexpected response: {}", evt).into());
            }
        };

        let mut auth_ses = Session {
            token: token,
            authtime: authtime,
            workstation: None,
        };

        if let Some(w) = workstation {
            auth_ses.workstation = Some(String::from(w));
        }

        Ok(Some(auth_ses))
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn authtime(&self) -> usize {
        self.authtime
    }

    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }
}

/// Returns the auth session duration in seconds for the provided
/// login type and context org unit(s) and host settings.
pub fn get_auth_duration(
    editor: &mut Editor,
    org_id: i64,
    user_home_ou: i64,
    host_settings: &HostSettings,
    auth_type: &LoginType,
) -> EgResult<i64> {
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

    if interval.is_null() {
        // No org unit setting.  Use the default.

        let setkey =
            format!("apps/open-ils.auth_internal/app_settings/default_timeout/{auth_type}");

        interval = host_settings.value(&setkey);
    }

    if let Some(num) = interval.as_int() {
        Ok(num)
    } else if let Some(s) = interval.as_str() {
        date::interval_to_seconds(&s)
    } else {
        Ok(0)
    }
}
