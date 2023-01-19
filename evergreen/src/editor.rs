use super::event::EgEvent;
use super::idl;
use opensrf as osrf;
use osrf::params::ApiParams;
use std::sync::Arc;

const DEFAULT_TIMEOUT: i32 = 60;

/// Specifies Which service are we communicating with.
#[derive(Debug, Clone, PartialEq)]
pub enum Personality {
    Cstore,
    Pcrud,
    ReporterStore,
}

impl From<&str> for Personality {
    fn from(s: &str) -> Self {
        match s {
            "open-ils.pcrud" => Self::Pcrud,
            "open-ils.reporter-store" => Self::ReporterStore,
            _ => Self::Cstore,
        }
    }
}

impl From<&Personality> for &str {
    fn from(p: &Personality) -> &'static str {
        match *p {
            Personality::Cstore => "open-ils.cstore",
            Personality::Pcrud => "open-ils.pcrud",
            Personality::ReporterStore => "open-ils.reporter-store",
        }
    }
}

pub struct QueryOps {
    limit: Option<usize>,
    offset: Option<usize>,
    order_by: Option<(String, String)>,
}

pub struct Editor {
    client: osrf::Client,
    session: Option<osrf::SessionHandle>,
    idl: Arc<idl::Parser>,

    personality: Personality,
    authtoken: Option<String>,
    authtime: Option<usize>,
    requestor: Option<json::JsonValue>,
    timeout: i32,

    /// True if the caller wants us to perform actions within
    /// a transaction.  Write actions require this.
    xact_wanted: bool,

    /// ID for currently active transaction.
    xact_id: Option<String>,

    /// Most recent non-success event
    last_event: Option<EgEvent>,
}

impl Editor {
    pub fn new(client: &osrf::Client, idl: &Arc<idl::Parser>) -> Self {
        Editor {
            client: client.clone(),
            idl: idl.clone(),
            personality: "".into(),
            timeout: DEFAULT_TIMEOUT,
            xact_wanted: false,
            xact_id: None,
            session: None,
            authtoken: None,
            authtime: None,
            requestor: None,
            last_event: None,
        }
    }

    pub fn with_auth(client: &osrf::Client, idl: &Arc<idl::Parser>, authtoken: &str) -> Self {
        let mut editor = Editor::new(client, idl);
        editor.authtoken = Some(authtoken.to_string());
        editor
    }

    pub fn with_auth_xact(client: &osrf::Client, idl: &Arc<idl::Parser>, authtoken: &str) -> Self {
        let mut editor = Editor::new(client, idl);
        editor.authtoken = Some(authtoken.to_string());
        editor.xact_wanted = true;
        editor
    }

    /// Verify our authtoken is still valid.
    ///
    /// Update our "requestor" object to match the user object linked
    /// to the authtoken in the cache.
    pub fn checkauth(&mut self) -> Result<bool, String> {
        let token = match self.authtoken() {
            Some(t) => t,
            None => {
                return Ok(false);
            }
        };

        let service = "open-ils.auth";
        let method = "open-ils.auth.session.retrieve";
        let params = vec![json::from(token), json::from(true)];

        let resp_op = self.client.sendrecv(service, method, &params)?.next();

        if let Some(ref user) = resp_op {
            if let Some(evt) = EgEvent::parse(&user) {
                log::debug!("Editor checkauth call returned non-success event: {}", evt);
                self.set_last_event(evt);
                return Ok(false);
            }

            if user.has_key("usrname") {
                self.requestor = Some(user.to_owned());
                return Ok(true);
            }
        }

        log::debug!("Editor checkauth call returned unexpected data: {resp_op:?}");

        self.set_last_event(EgEvent::new("NO_SESSION"));
        Ok(false)
    }

    pub fn personality(&self) -> &Personality {
        &self.personality
    }

    pub fn authtoken(&self) -> Option<&str> {
        self.authtoken.as_deref()
    }

    pub fn set_authtoken(&mut self, token: &str) {
        self.authtoken = Some(token.to_string())
    }

    pub fn authtime(&self) -> Option<usize> {
        self.authtime
    }

    fn has_session(&self) -> bool {
        self.session.is_some()
    }

    fn has_xact_id(&self) -> bool {
        self.xact_id.is_some()
    }

    pub fn requestor(&self) -> Option<&json::JsonValue> {
        self.requestor.as_ref()
    }

    pub fn set_requestor(&mut self, r: &json::JsonValue)  {
        self.requestor = Some(r.clone())
    }

    pub fn last_event(&self) -> Option<&EgEvent> {
        self.last_event.as_ref()
    }

    fn set_last_event(&mut self, evt: EgEvent) {
        self.last_event = Some(evt);
    }

    /// Rollback the active transaction, disconnect from the worker,
    /// and return the last_event value.
    pub fn die_event(&mut self) -> Result<Option<&EgEvent>, String> {
        self.rollback()?;
        Ok(self.last_event())
    }

    /// Rollback the active transaction and disconnect from the worker.
    pub fn rollback(&mut self) -> Result<(), String> {
        self.xact_rollback()?;
        self.disconnect()
    }

    /// Generate a method name prefixed with the app name of our personality.
    fn app_method(&self, part: &str) -> String {
        let p: &str = self.personality().into();
        format!("{p}.{}", part)
    }

    pub fn xact_rollback(&mut self) -> Result<(), String> {
        if self.has_session() && self.has_xact_id() {
            self.request_np(&self.app_method("transaction.rollback"))?;
        }

        self.xact_id = None;
        self.xact_wanted = false;

        Ok(())
    }

    pub fn xact_begin(&mut self) -> Result<(), String> {
        if let Some(id) = self.request_np(&self.app_method("transaction.begin"))? {
            if let Some(id_str) = id.as_str() {
                log::debug!("New transaction started with id {}", id_str);
                self.xact_id = Some(id_str.to_string());
            }
        }
        Ok(())
    }

    pub fn xact_commit(&mut self) -> Result<(), String> {

        if self.has_session() && self.has_xact_id() {
            let xact_id = self.xact_id.as_ref().unwrap().to_string();
            let method = self.app_method("transaction.commit");
            self.request(&method, xact_id.as_str())?;
        }

        self.xact_id = None;
        self.xact_wanted = false;

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), String> {
        self.xact_rollback()?;

        if let Some(ref ses) = self.session {
            ses.disconnect()?;
        }
        self.session = None;
        Ok(())
    }

    pub fn connect(&mut self) -> Result<(), String> {
        self.session().connect()
    }

    /// Send an API request without any parameters.
    ///
    /// See request() for more.
    fn request_np(&mut self, method: &str) -> Result<Option<json::JsonValue>, String> {
        let params: Vec<json::JsonValue> = Vec::new();
        self.request(method, params)
    }

    fn logtag(&self) -> String {

        let requestor = match self.requestor() {
            Some(req) => format!("{}", req),
            None => "0".to_string(),
        };

        format!("editor[{}|{}]",
            match self.has_xact_id() { true => "1", _ => "0" },
            requestor
        )
    }

    fn args_to_string(&self, params: &ApiParams) -> Result<String, String> {
        let mut buf = String::new();
        for p in params.params().iter() {

            if self.idl.is_idl_object(p) {

                if let Some(pkv) = self.idl.get_pkey_value(p) {
                    buf.push_str(&pkv);
                } else {
                    buf.push_str("<new object>");
                }
            } else {
                buf.push_str(&p.dump());
            }
        }

        Ok(buf)
    }

    /// Send an API request to our service/worker with parameters.
    ///
    /// All requests return at most a single response.
    fn request<T>(&mut self, method: &str, params: T) -> Result<Option<json::JsonValue>, String>
    where
        T: Into<ApiParams>,
    {
        let params: ApiParams = params.into();

        log::info!("{} request {} {}", self.logtag(), method, self.args_to_string(&params)?);

        let mut req = match self.session().request(method, params) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Request failed: {e}. Performing rollback/disconnect as needed");
                self.rollback()?;
                Err(e)?
            }
        };

        req.recv(self.timeout)
    }

    /// Returns our mutable session, creating a new one if needed.
    fn session(&mut self) -> &mut osrf::SessionHandle {
        if self.session.is_none() {
            self.session = Some(self.client.session(self.personality().into()));
        }

        self.session.as_mut().unwrap()
    }

    fn get_class(&self, idlclass: &str) -> Result<&idl::Class, String> {
        match self.idl.classes().get(idlclass) {
            Some(c) => Ok(c),
            None => Err(format!("No such IDL class: {idlclass}")),
        }
    }

    /// Returns the fieldmapper value for the IDL class, replacing
    /// "::" with "." so the value matches how it's formatted in
    /// cstore, etc. API calls.
    fn get_fieldmapper(&self, idlclass: &str) -> Result<String, String> {
        let class = self.get_class(idlclass)?;

        match class.fieldmapper() {
            Some(s) => Ok(s.replace("::", ".")),
            None => Err(format!("IDL class has no fieldmapper value: {idlclass}")),
        }
    }

    pub fn retrieve<T>(&mut self, idlclass: &str, id: T) -> Result<Option<json::JsonValue>, String>
    where
        T: Into<ApiParams>,
    {
        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.retrieve"));

        self.request(&method, id)
    }

    pub fn search(
        &mut self,
        idlclass: &str,
        query: json::JsonValue,
    ) -> Result<Vec<json::JsonValue>, String> {
        self.search_with_ops(idlclass, query, json::JsonValue::Null)
    }

    pub fn search_with_ops(
        &mut self,
        idlclass: &str,
        query: json::JsonValue,
        ops: json::JsonValue, // flesh, etc.
    ) -> Result<Vec<json::JsonValue>, String> {
        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.search.atomic"));

        if let Some(jvec) = self.request(&method, vec![query, ops])? {
            if let json::JsonValue::Array(vec) = jvec {
                return Ok(vec);
            }
        }

        Err(format!("Unexpected response to method {method}"))
    }

    pub fn update(
        &mut self,
        object: &json::JsonValue,
    ) -> Result<(), String> {

        if !self.has_xact_id() {
            Err(format!("Transaction required for UPDATE"))?;
        }

        let idlclass = match object[idl::CLASSNAME_KEY].as_str() {
            Some(c) => c,
            None => Err(format!("update() called on non-IDL object"))?,
        };

        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.update"));

        if let Some(resp) = self.request(&method, object)? {
            log::debug!("Update call returned {:?}", resp);
        } else {
            Err(format!("Update returned no response"))?;
        }

        Ok(())
    }
}
