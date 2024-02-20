use crate::event::EgEvent;
use crate::idl;
use crate::result::{EgError, EgResult};
use crate::util;
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

/*
pub struct QueryOps {
    limit: Option<usize>,
    offset: Option<usize>,
    order_by: Option<(String, String)>,
}
*/

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

    has_pending_changes: bool,
}

impl Clone for Editor {
    fn clone(&self) -> Editor {
        let mut e = Editor::new(&self.client, &self.idl);
        e.personality = self.personality().clone();
        e.authtoken = self.authtoken().map(str::to_string);
        e.requestor = self.requestor().map(|r| r.clone());
        e
    }
}

impl Editor {
    /// Create a new minimal Editor
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
            has_pending_changes: false,
        }
    }

    /// Apply a new request timeout value in seconds.
    pub fn set_timeout(&mut self, timeout: i32) {
        self.timeout = timeout;
    }

    /// Reset to the default timeout
    pub fn reset_timeout(&mut self) {
        self.timeout = DEFAULT_TIMEOUT;
    }

    pub fn client_mut(&mut self) -> &mut osrf::Client {
        &mut self.client
    }

    /// True if create/update/delete have been called within a
    /// transaction that has yet to be committed or rolled back.
    ///
    /// This has no effect on the Editor, but may be useful to
    /// the calling code.
    pub fn has_pending_changes(&self) -> bool {
        self.has_pending_changes
    }

    /// Create an editor with an existing authtoken
    pub fn with_auth(client: &osrf::Client, idl: &Arc<idl::Parser>, authtoken: &str) -> Self {
        let mut editor = Editor::new(client, idl);
        editor.authtoken = Some(authtoken.to_string());
        editor
    }

    /// Create an editor with an existing authtoken, with the "transaction
    /// wanted" flag set by default.
    pub fn with_auth_xact(client: &osrf::Client, idl: &Arc<idl::Parser>, authtoken: &str) -> Self {
        let mut editor = Editor::new(client, idl);
        editor.authtoken = Some(authtoken.to_string());
        editor.xact_wanted = true;
        editor
    }

    /// Offer a read-only version of the IDL to anyone who needs it.
    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }

    /// Verify our authtoken is still valid.
    ///
    /// Update our "requestor" object to match the user object linked
    /// to the authtoken in the cache.
    pub fn checkauth(&mut self) -> EgResult<bool> {
        let token = match self.authtoken() {
            Some(t) => t,
            None => return Ok(false),
        };

        let service = "open-ils.auth";
        let method = "open-ils.auth.session.retrieve";
        let params = vec![json::from(token), json::from(true)];

        let mut ses = self.client.session(service);
        let resp_op = ses.request(method, params)?.first()?;

        if let Some(user) = resp_op {
            if let Some(evt) = EgEvent::parse(&user) {
                log::debug!("Editor checkauth call returned non-success event: {}", evt);
                self.set_last_event(evt);
                return Ok(false);
            }

            if user.has_key("usrname") {
                self.requestor = Some(user);
                return Ok(true);
            }
        }

        log::debug!("Editor checkauth call returned unexpected data");

        // Login failure is not considered an error.
        self.set_last_event(EgEvent::new("NO_SESSION"));
        Ok(false)
    }

    pub fn personality(&self) -> &Personality {
        &self.personality
    }

    pub fn authtoken(&self) -> Option<&str> {
        self.authtoken.as_deref()
    }

    /// Set the authtoken value.
    pub fn set_authtoken(&mut self, token: &str) {
        self.authtoken = Some(token.to_string())
    }

    /// Set the authtoken value and verify the authtoken is valid
    pub fn apply_authtoken(&mut self, token: &str) -> EgResult<bool> {
        self.set_authtoken(token);
        self.checkauth()
    }

    pub fn authtime(&self) -> Option<usize> {
        self.authtime
    }

    fn has_xact_id(&self) -> bool {
        self.xact_id.is_some()
    }

    /// ID of the requestor
    ///
    /// Panics if the requestor value is unset, i.e. checkauth() has
    /// not successfully run, or for some reason the requestor ID is
    /// non-numeric.
    pub fn requestor_id(&self) -> i64 {
        util::json_int(&self.requestor().unwrap()["id"]).unwrap()
    }

    /// Org Unit ID of the requestor's workstation.
    ///
    /// Panics if requestor value is unset.
    pub fn requestor_ws_ou(&self) -> i64 {
        util::json_int(&self.requestor().unwrap()["ws_ou"]).unwrap()
    }

    /// Workstation ID of the requestor's workstation.
    ///
    /// Panics if requestor value is unset.
    pub fn requestor_ws_id(&self) -> Option<i64> {
        if let Some(r) = self.requestor() {
            if let Ok(n) = util::json_int(&r["ws_id"]) {
                return Some(n);
            }
        }
        None
    }

    pub fn perm_org(&self) -> i64 {
        match self.requestor.as_ref() {
            Some(r) => match r["ws_ou"].as_i64() {
                Some(v) => v,
                None => r["home_ou"].as_i64().unwrap(), // required
            },
            None => -1,
        }
    }

    pub fn requestor(&self) -> Option<&json::JsonValue> {
        self.requestor.as_ref()
    }

    /// Returns Err if no requestor value is set.
    pub fn has_requestor(&self) -> EgResult<()> {
        self.requestor
            .as_ref()
            .map(|_| ())
            .ok_or_else(|| format!("Editor requestor is unset").into())
    }

    pub fn set_requestor(&mut self, r: &json::JsonValue) {
        self.requestor = Some(r.clone())
    }

    pub fn last_event(&self) -> Option<&EgEvent> {
        self.last_event.as_ref()
    }

    pub fn take_last_event(&mut self) -> Option<EgEvent> {
        self.last_event.take()
    }

    /// Panics if there is no last event
    pub fn last_event_unchecked(&self) -> &EgEvent {
        self.last_event().unwrap()
    }

    pub fn event_as_err(&self) -> EgError {
        match self.last_event() {
            Some(e) => EgError::Event(e.clone()),
            None => EgError::Debug("Editor Has No Event".to_string()),
        }
    }

    /// Returns our last event as JSON or JsonValue::Null if we have
    /// no last event.
    pub fn event(&self) -> json::JsonValue {
        match self.last_event() {
            Some(e) => e.to_json_value(),
            None => json::JsonValue::Null,
        }
    }

    fn set_last_event(&mut self, evt: EgEvent) {
        self.last_event = Some(evt);
    }

    /// Rollback the active transaction, disconnect from the worker,
    /// and return an EgError-wrapped variant of the last event.
    ///
    /// The raw event can still be accessed via self.last_event().
    pub fn die_event(&mut self) -> EgError {
        if let Err(e) = self.rollback() {
            return e;
        }
        match self.last_event() {
            Some(e) => EgError::Event(e.clone()),
            None => EgError::Debug("Die-Event Called With No Event".to_string()),
        }
    }

    /// Rollback the active transaction, disconnect from the worker,
    /// and an EgError using the provided message as either the
    /// debug text on our last_event or as the string contents
    /// of an EgError::Debug variant.
    pub fn die_event_msg(&mut self, msg: &str) -> EgError {
        if let Err(e) = self.rollback() {
            return e;
        }
        match self.last_event() {
            Some(e) => {
                let mut e2 = e.clone();
                e2.set_debug(msg);
                EgError::Event(e2)
            }
            None => EgError::Debug(msg.to_string()),
        }
    }

    /// Rollback the active transaction and disconnect from the worker.
    pub fn rollback(&mut self) -> EgResult<()> {
        self.xact_rollback()?;
        self.disconnect()
    }

    /// Commit the active transaction and disconnect from the worker.
    pub fn commit(&mut self) -> EgResult<()> {
        self.xact_commit()?;
        self.disconnect()
    }

    /// Generate a method name prefixed with the app name of our personality.
    fn app_method(&self, part: &str) -> String {
        let p: &str = self.personality().into();
        format!("{p}.{}", part)
    }

    pub fn in_transaction(&self) -> bool {
        if let Some(ref ses) = self.session {
            ses.connected() && self.has_xact_id()
        } else {
            false
        }
    }

    /// Rollback a database transaction.
    ///
    /// This variation does not send a DISCONNECT to the connected worker.
    pub fn xact_rollback(&mut self) -> EgResult<()> {
        if self.in_transaction() {
            self.request_np(&self.app_method("transaction.rollback"))?;
        }

        self.xact_id = None;
        self.xact_wanted = false;
        self.has_pending_changes = false;

        Ok(())
    }

    /// Start a new transaction, connecting to a worker if necessary.
    pub fn xact_begin(&mut self) -> EgResult<()> {
        self.connect()?;
        if let Some(id) = self.request_np(&self.app_method("transaction.begin"))? {
            if let Some(id_str) = id.as_str() {
                log::debug!("New transaction started with id {}", id_str);
                self.xact_id = Some(id_str.to_string());
            }
        }
        Ok(())
    }

    /// Commit a database transaction.
    ///
    /// This variation does not send a DISCONNECT to the connected worker.
    pub fn xact_commit(&mut self) -> EgResult<()> {
        if self.in_transaction() {
            // We can take() the xact_id here because we're clearing
            // it below anyway.  This avoids a .to_string() as a way
            // to get around the mutable borrow from self.request().
            let xact_id = self.xact_id.take().unwrap();
            let method = self.app_method("transaction.commit");
            self.request(&method, xact_id)?;
        }

        self.xact_id = None;
        self.xact_wanted = false;
        self.has_pending_changes = false;

        Ok(())
    }

    /// End the stateful conversation with the remote worker.
    pub fn disconnect(&mut self) -> EgResult<()> {
        self.xact_rollback()?;

        if let Some(ref ses) = self.session {
            ses.disconnect()?;
        }
        self.session = None;
        Ok(())
    }

    /// Start a stateful conversation with a worker.
    pub fn connect(&mut self) -> EgResult<()> {
        if let Some(ref ses) = self.session {
            if ses.connected() {
                // Already connected.
                return Ok(());
            }
        }
        self.session().connect()?;
        Ok(())
    }

    /// Send an API request without any parameters.
    ///
    /// See request() for more.
    fn request_np(&mut self, method: &str) -> EgResult<Option<json::JsonValue>> {
        let params: Vec<json::JsonValue> = Vec::new();
        self.request(method, params)
    }

    fn logtag(&self) -> String {
        let requestor = match self.requestor() {
            Some(req) => match util::json_int(&req["id"]) {
                Ok(n) => n,
                _ => 0,
            },
            _ => 0,
        };

        format!(
            "editor[{}|{}]",
            match self.has_xact_id() {
                true => "1",
                _ => "0",
            },
            requestor
        )
    }

    fn args_to_string(&self, params: &ApiParams) -> String {
        let mut buf = String::new();
        for p in params.params().iter() {
            if self.idl.is_idl_object(p) {
                if let Some(pkv) = self.idl.get_pkey_value(p) {
                    if pkv.is_null() {
                        buf.push_str("<new object>");
                    } else {
                        buf.push_str(&pkv.dump());
                    }
                } else {
                    buf.push_str("<new object>");
                }
            } else {
                buf.push_str(&p.dump());
            }

            buf.push_str(" ");
        }

        buf.trim().to_string()
    }

    /// Send an API request to our service/worker with parameters.
    ///
    /// All requests return at most a single response.
    fn request(
        &mut self,
        method: &str,
        params: impl Into<ApiParams>,
    ) -> EgResult<Option<json::JsonValue>> {
        let params: ApiParams = params.into();

        log::info!(
            "{} request {} {}",
            self.logtag(),
            method,
            self.args_to_string(&params)
        );

        if method.contains("create") || method.contains("update") || method.contains("delete") {
            if !self.has_xact_id() {
                self.disconnect()?;
                Err(format!(
                    "Attempt to update DB while not in a transaction : {method}"
                ))?;
            }

            if params.params().len() == 0 {
                Err(EgError::Debug(format!(
                    "Create/update/delete calls require a parameter"
                )))?;
            }

            // Verify the object provided is a valid IDL object with
            // correctly spelled keys.
            self.idl().verify_object(&params.params()[0])?;

            // Write calls also get logged to the activity log
            log::info!(
                "ACT:{} request {} {}",
                self.logtag(),
                method,
                self.args_to_string(&params)
            );
        }

        let mut req = self.session().request(method, params).or_else(|e| {
            self.rollback()?;
            Err(e)
        })?;

        req.first_with_timeout(self.timeout)
            .map_err(|e| EgError::Debug(e))
    }

    /// Returns our mutable session, creating a new one if needed.
    fn session(&mut self) -> &mut osrf::SessionHandle {
        if self.session.is_none() {
            self.session = Some(self.client.session(self.personality().into()));
        }

        self.session.as_mut().unwrap()
    }

    /// Get an IDL class by class name.
    fn get_class(&self, idlclass: &str) -> EgResult<&idl::Class> {
        match self.idl.classes().get(idlclass) {
            Some(c) => Ok(c),
            None => Err(format!("No such IDL class: {idlclass}").into()),
        }
    }

    /// Returns the fieldmapper value for the IDL class, replacing
    /// "::" with "." so the value matches how it's formatted in
    /// cstore, etc. API calls.
    fn get_fieldmapper(&self, idlclass: &str) -> EgResult<String> {
        let class = self.get_class(idlclass)?;

        match class.fieldmapper() {
            Some(s) => Ok(s.replace("::", ".")),
            None => Err(format!("IDL class has no fieldmapper value: {idlclass}").into()),
        }
    }

    pub fn json_query(&mut self, query: json::JsonValue) -> EgResult<Vec<json::JsonValue>> {
        self.json_query_with_ops(query, json::JsonValue::Null)
    }

    pub fn json_query_with_ops(
        &mut self,
        query: json::JsonValue,
        ops: json::JsonValue,
    ) -> EgResult<Vec<json::JsonValue>> {
        let method = self.app_method(&format!("json_query.atomic"));

        if let Some(jvec) = self.request(&method, vec![query, ops])? {
            if let json::JsonValue::Array(vec) = jvec {
                return Ok(vec);
            }
        }

        Err(format!("Unexpected response to method {method}").into())
    }

    pub fn retrieve(
        &mut self,
        idlclass: &str,
        id: impl Into<ApiParams>,
    ) -> EgResult<Option<json::JsonValue>> {
        self.retrieve_with_ops(idlclass, id, json::JsonValue::Null)
    }

    pub fn retrieve_with_ops(
        &mut self,
        idlclass: &str,
        id: impl Into<ApiParams>,
        ops: json::JsonValue, // flesh, etc.
    ) -> EgResult<Option<json::JsonValue>> {
        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.retrieve"));

        let mut params: ApiParams = id.into();
        params.add(ops);

        let resp_op = self.request(&method, params)?;

        if resp_op.is_none() {
            // not-found is not necessarily an error.
            let key = fmapper.replace(".", "_").to_uppercase();
            self.set_last_event(EgEvent::new(&format!("{key}_NOT_FOUND")));
        }

        Ok(resp_op)
    }

    pub fn search(
        &mut self,
        idlclass: &str,
        query: json::JsonValue,
    ) -> EgResult<Vec<json::JsonValue>> {
        self.search_with_ops(idlclass, query, json::JsonValue::Null)
    }

    pub fn search_with_ops(
        &mut self,
        idlclass: &str,
        query: json::JsonValue,
        ops: json::JsonValue, // flesh, etc.
    ) -> EgResult<Vec<json::JsonValue>> {
        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.search.atomic"));

        if let Some(jvec) = self.request(&method, vec![query, ops])? {
            if let json::JsonValue::Array(vec) = jvec {
                return Ok(vec);
            }
        }

        Err(format!("Unexpected response to method {method}").into())
    }

    /// Update an object.
    pub fn update(&mut self, object: json::JsonValue) -> EgResult<()> {
        if !self.has_xact_id() {
            Err(format!("Transaction required for UPDATE"))?;
        }

        let idlclass = match object[idl::CLASSNAME_KEY].as_str() {
            Some(c) => c,
            None => Err(format!("update() called on non-IDL object"))?,
        };

        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.update"));

        // Update calls return the pkey of the object on success,
        // nothing on error.
        if self.request(&method, object)?.is_none() {
            Err(format!("Update returned no response"))?;
        }

        self.has_pending_changes = true;

        Ok(())
    }

    /// Returns the newly created object.
    pub fn create(&mut self, object: json::JsonValue) -> EgResult<json::JsonValue> {
        if !self.has_xact_id() {
            Err(format!("Transaction required for CREATE"))?;
        }

        let idlclass = match object[idl::CLASSNAME_KEY].as_str() {
            Some(s) => s.to_string(),
            None => return Err(format!("CREATE called on non-IDL object: {object:?}").into()),
        };

        let fmapper = self.get_fieldmapper(&idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.create"));

        if let Some(resp) = self.request(&method, object)? {
            if let Some(pkey) = self.idl.get_pkey_value(&resp) {
                log::info!("Created new {idlclass} object with pkey: {}", pkey.dump());
            } else {
                // Don't think we can get here, but mabye.
                log::debug!("Created new {idlclass} object: {resp:?}");
            }

            self.has_pending_changes = true;

            Ok(resp)
        } else {
            Err(format!("Create returned no response").into())
        }
    }

    /// Delete an IDL Object.
    ///
    /// Response is the PKEY value as a JsonValue.
    pub fn delete(&mut self, object: json::JsonValue) -> EgResult<json::JsonValue> {
        if !self.has_xact_id() {
            Err(format!("Transaction required for DELETE"))?;
        }

        let idlclass = object[idl::CLASSNAME_KEY]
            .as_str()
            .ok_or_else(|| format!("DELETE called on non-IDL object {object:?}"))?;

        let fmapper = self.get_fieldmapper(idlclass)?;

        let method = self.app_method(&format!("direct.{fmapper}.delete"));

        if let Some(resp) = self.request(&method, object)? {
            self.has_pending_changes = true;
            Ok(resp)
        } else {
            Err(format!("Create returned no response").into())
        }
    }

    /// Returns Result of true if our authenticated requestor has the
    /// specified permission at their logged in workstation org unit,
    /// or their home org unit if no workstation is active.
    pub fn allowed(&mut self, perm: &str) -> EgResult<bool> {
        self.allowed_maybe_at(perm, None)
    }

    /// Returns Result of true if our authenticated requestor has the
    /// specified permission at the specified org unit.
    pub fn allowed_at(&mut self, perm: &str, org_id: i64) -> EgResult<bool> {
        self.allowed_maybe_at(perm, Some(org_id))
    }

    fn allowed_maybe_at(&mut self, perm: &str, org_id_op: Option<i64>) -> EgResult<bool> {
        let user_id = match self.requestor() {
            Some(r) => util::json_int(&r["id"])?,
            None => return Ok(false),
        };

        let org_id = match org_id_op {
            Some(i) => i,
            None => self.perm_org(),
        };

        let query = json::object! {
            select: {
                au: [ {
                    transform: "permission.usr_has_perm",
                    alias: "has_perm",
                    column: "id",
                    params: json::array! [perm, json::from(org_id)]
                } ]
            },
            from: "au",
            where: {id: user_id},
        };

        let resp = self.json_query(query)?;
        let has_perm = util::json_bool(&resp[0]["has_perm"]);

        if !has_perm {
            let mut evt = EgEvent::new("PERM_FAILURE");
            evt.set_ils_perm(perm);
            if org_id > 0 {
                evt.set_ils_perm_loc(org_id);
            }
            self.set_last_event(evt);
        }

        Ok(has_perm)
    }
}
