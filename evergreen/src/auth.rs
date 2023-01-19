use super::event;
use json;
use opensrf::client::Client;

const LOGIN_TIMEOUT: i32 = 30;

#[derive(Debug)]
pub enum AuthLoginType {
    Temp,
    Opac,
    Staff,
    Persist,
}

impl From<&str> for AuthLoginType {
    fn from(s: &str) -> Self {
        match s {
            "opac" => Self::Opac,
            "staff" => Self::Staff,
            "persist" => Self::Persist,
            "temp" => Self::Temp,
            _ => {
                log::error!("Invalid login type: {s}. Using temp instead");
                Self::Temp
            }
        }
    }
}

impl From<&AuthLoginType> for &str {
    fn from(lt: &AuthLoginType) -> &'static str {
        match *lt {
            AuthLoginType::Temp => "temp",
            AuthLoginType::Opac => "opac",
            AuthLoginType::Staff => "staff",
            AuthLoginType::Persist => "persist",
        }
    }
}

pub struct AuthLoginArgs {
    pub username: String,
    pub password: String,
    pub login_type: AuthLoginType,
    pub workstation: Option<String>,
}

impl AuthLoginArgs {
    pub fn new<T>(username: &str, password: &str, login_type: T, workstation: Option<&str>) -> Self
    where
        T: Into<AuthLoginType>,
    {
        AuthLoginArgs {
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

    pub fn login_type(&self) -> &AuthLoginType {
        &self.login_type
    }

    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let lt: &str = self.login_type().into();

        let mut jv = json::object! {
            username: self.username(),
            password: self.password(),
            "type": lt,
        };

        if let Some(w) = &self.workstation {
            jv["workstation"] = json::from(w.as_str());
        }

        jv
    }
}


#[derive(Debug)]
pub struct AuthInternalLoginArgs {
    pub user_id: i64,
    pub org_unit: Option<i64>,
    pub login_type: AuthLoginType,
    pub workstation: Option<String>,
}

impl AuthInternalLoginArgs {

    pub fn new<T>(user_id: i64, login_type: T) -> Self
    where
        T: Into<AuthLoginType>,
    {
        AuthInternalLoginArgs {
            user_id,
            login_type: login_type.into(),
            org_unit: None,
            workstation: None,
        }
    }

    pub fn to_json_value(&self) -> json::JsonValue {
        let lt: &str = (&self.login_type).into();

        let mut jv = json::object! {
            "login_type": lt,
            "user_id": self.user_id,
        };

        if let Some(w) = &self.workstation {
            jv["workstation"] = json::from(w.as_str());
        }

        if let Some(w) = self.org_unit {
            jv["org_unit"] = json::from(w);
        }

        jv
    }
}

pub struct AuthSession {
    token: String,
    authtime: usize,
    workstation: Option<String>,
}

impl AuthSession {

    /// Login and acquire an authtoken.
    ///
    /// Returns None on login failure, Err on error.
    pub fn login(client: &Client, args: &AuthLoginArgs) -> Result<Option<AuthSession>, String> {
        let params = vec![args.to_json_value()];
        let mut ses = client.session("open-ils.auth");
        let mut req = ses.request("open-ils.auth.login", params)?;

        let json_val = match req.recv(LOGIN_TIMEOUT)? {
            Some(v) => v,
            None => {
                return Err("Login Timed Out".to_string());
            }
        };

        AuthSession::handle_auth_response(&args.workstation, &json_val)
    }

    /// Login and acquire an authtoken.
    ///
    /// Returns None on login failure, Err on error.
    pub fn create_internal_session(client: &Client,
        args: &AuthInternalLoginArgs) -> Result<Option<AuthSession>, String> {

        let params = vec![args.to_json_value()];
        let mut ses = client.session("open-ils.auth_internal");
        let mut req = ses.request("open-ils.auth_internal.session.create", params)?;

        let json_val = match req.recv(LOGIN_TIMEOUT)? {
            Some(v) => v,
            None => {
                return Err("Login Timed Out".to_string());
            }
        };

        AuthSession::handle_auth_response(&args.workstation, &json_val)
    }

    fn handle_auth_response(workstation: &Option<String>,
        response: &json::JsonValue) -> Result<Option<AuthSession>, String> {

        let evt = match event::EgEvent::parse(&response) {
            Some(e) => e,
            None => {
                return Err(format!("Unexpected response: {:?}", response));
            }
        };

        if !evt.success() {
            log::warn!("Login failed");
            return Ok(None);
        }

        if !evt.payload().is_object() {
            return Err(format!("Unexpected response: {}", evt));
        }

        let token = match evt.payload()["authtoken"].as_str() {
            Some(t) => String::from(t),
            None => {
                return Err(format!("Unexpected response: {}", evt));
            }
        };

        let authtime = match evt.payload()["authtime"].as_usize() {
            Some(t) => t,
            None => {
                return Err(format!("Unexpected response: {}", evt));
            }
        };

        let mut auth_ses = AuthSession {
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

