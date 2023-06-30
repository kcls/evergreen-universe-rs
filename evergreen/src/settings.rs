//! General purpose org / workstation / user setting fetcher and cache.
//! Primarily uses the 'actor.get_cascade_setting()' DB function.
use crate::editor::Editor;
use crate::util;
use json::Value;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::time::Instant;

const JSON_NULL: json::Value = json::Value::Null;

// Setting names consist only of letters, numbers, unders, and dots.
// This is crucial since the names are encoded as an SQL TEXT[] parameter
// during lookuping.
const SETTING_NAME_REGEX: &str = "[^a-zA-Z0-9_\\.]";

/// Setting entries cached longer than this will be refreshed on
/// future lookup.  This is backstop to prevent settings from
/// sticking around too long in long-running processes.
const DEFAULT_SETTING_ENTRY_TIMEOUT: u64 = 600; // seconds

/// SettingType may come in handy later when we need to know
/// more about the types.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingType {
    name: String,
    has_org_setting: bool,
    has_user_setting: bool,
    has_workstation_setting: bool,
}

impl SettingType {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn has_org_setting(&self) -> bool {
        self.has_org_setting
    }
    pub fn has_user_setting(&self) -> bool {
        self.has_user_setting
    }
    pub fn has_workstation_setting(&self) -> bool {
        self.has_workstation_setting
    }
}

/// Defines the context under which a setting is retrieved.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettingContext {
    org_id: Option<i64>,
    user_id: Option<i64>,
    workstation_id: Option<i64>,
}

impl fmt::Display for SettingContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SettingContext org={:?} user={:?} workstation={:?}",
            self.org_id, self.user_id, self.workstation_id
        )
    }
}

impl SettingContext {
    pub fn new() -> SettingContext {
        SettingContext {
            org_id: None,
            user_id: None,
            workstation_id: None,
        }
    }
    pub fn set_org_id(&mut self, org_id: i64) {
        self.org_id = Some(org_id);
    }
    pub fn set_user_id(&mut self, user_id: i64) {
        self.user_id = Some(user_id);
    }
    pub fn set_workstation_id(&mut self, workstation_id: i64) {
        self.workstation_id = Some(workstation_id);
    }
    pub fn org_id(&self) -> &Option<i64> {
        &self.org_id
    }
    pub fn user_id(&self) -> &Option<i64> {
        &self.user_id
    }
    pub fn workstation_id(&self) -> &Option<i64> {
        &self.workstation_id
    }
    pub fn org_id_json(&self) -> json::Value {
        self.org_id.map(json::from).unwrap_or(JSON_NULL)
    }
    pub fn user_id_json(&self) -> json::Value {
        self.user_id.map(json::from).unwrap_or(JSON_NULL)
    }
    pub fn workstation_id_json(&self) -> json::Value {
        self.workstation_id.map(json::from).unwrap_or(JSON_NULL)
    }

    /// Returns true if this context has enough information to
    /// perform setting lookups.
    ///
    /// Workstation ID alone is not enough, since actor.get_cascade_setting()
    /// requires a user ID for workstation lookups.
    pub fn is_viable(&self) -> bool {
        self.org_id.is_some() || self.user_id.is_some()
    }
}

///
///
/// Each SettingEntry is linked to its runtime context via the HashMap
/// it's stored in.
#[derive(Debug)]
pub struct SettingEntry {
    value: json::Value,
    lookup_time: Instant,
}

impl SettingEntry {
    pub fn value(&self) -> &json::Value {
        &self.value
    }
    pub fn lookup_time(&self) -> &Instant {
        &self.lookup_time
    }
}

pub struct Settings {
    editor: Editor,
    default_context: SettingContext,
    name_regex: Option<Regex>,
    cache: HashMap<SettingContext, HashMap<String, SettingEntry>>,
}

impl Settings {
    /// Create a new settings instance from an active Editor.
    ///
    /// The Editor instance should be fully setup (e.g. checkauth()
    /// already run) before using it to create a Settings instance.
    pub fn new(editor: &Editor) -> Settings {
        let mut sc = Settings {
            name_regex: None,
            editor: editor.clone(),
            cache: HashMap::new(),
            default_context: SettingContext::new(),
        };

        sc.apply_editor(&editor);

        sc
    }

    pub fn set_editor(&mut self, e: &Editor) {
        self.apply_editor(e);
        self.editor = e.clone();
    }

    /// Apply context values pulled from the Editor.
    pub fn apply_editor(&mut self, e: &Editor) {
        // See if we can pull context data from our editor.
        if let Some(reqr) = e.requestor() {
            if let Ok(id) = util::json_int(&reqr["id"]) {
                self.default_context.user_id = Some(id);
            }
            if let Ok(id) = util::json_int(&reqr["wsid"]) {
                self.default_context.workstation_id = Some(id);
            }
            if let Ok(id) = util::json_int(&reqr["ws_ou"]) {
                self.default_context.org_id = Some(id);
            } else if let Ok(id) = util::json_int(&reqr["home_ou"]) {
                self.default_context.org_id = Some(id);
            }
        }
    }

    /// Manually set/override the context org unit ID.
    pub fn set_org_id(&mut self, org_id: i64) {
        self.default_context.org_id = Some(org_id);
    }

    /// Manually set/override the context user ID.
    pub fn set_user_id(&mut self, user_id: i64) {
        self.default_context.user_id = Some(user_id);
    }

    /// Manually set/override the context workstation ID.
    pub fn set_workstation_id(&mut self, workstation_id: i64) {
        self.default_context.workstation_id = Some(workstation_id);
    }

    /// Clear all cached values now.
    pub fn reset(&mut self) {
        self.cache.clear();
    }

    /// Returns a setting value using the default context.
    ///
    /// Returns JSON null if no setting exists.
    pub fn get_value(&mut self, name: &str) -> Result<&json::Value, String> {
        // Clone needed here because get_context_value mutably borrows
        // self a number of times.
        self.get_context_value(&self.default_context.clone(), name)
    }

    /// Returns a setting value for the provided context.
    pub fn get_context_value(
        &mut self,
        context: &SettingContext,
        name: &str,
    ) -> Result<&json::Value, String> {
        let hash = match self.cache.get(context) {
            Some(h) => h,
            None => {
                self.cache.insert(context.clone(), HashMap::new());
                self.cache.get(context).unwrap()
            }
        };

        if !hash.contains_key(name) {
            self.fetch_context_values(context, &[name])?;
        }

        if let Some(v) = self.get_cached_value(context, name) {
            return Ok(v);
        }

        // Should never get here since fetch_values() above will
        // cache the fetched value.
        Err(format!("Unable to pull value from cache for {name}"))
    }

    pub fn get_cached_value(&mut self, context: &SettingContext, name: &str) -> Option<&json::Value> {
        let hash = match self.cache.get_mut(context) {
            Some(h) => h,
            None => return None,
        };

        let entry = match hash.get(name) {
            Some(e) => e,
            None => return None,
        };

        if entry.lookup_time().elapsed().as_secs() >= DEFAULT_SETTING_ENTRY_TIMEOUT {
            hash.remove(name);
            None
        } else {
            Some(hash.get(name).unwrap().value())
        }
    }

    /// Batch setting value fetch.
    ///
    /// Returns String Err on load failure or invalid setting name.
    /// On success, values are stored in the local cache for this
    /// Setting instance.
    pub fn fetch_values(&mut self, names: &[&str]) -> Result<(), String> {
        self.fetch_context_values(&self.default_context.clone(), names)
    }

    /// Batch setting value fetch.
    ///
    /// Returns String Err on load failure or invalid setting name.
    /// On success, values are stored in the local cache for this
    /// Setting instance.
    pub fn fetch_context_values(
        &mut self,
        context: &SettingContext,
        names: &[&str],
    ) -> Result<(), String> {
        if !context.is_viable() {
            Err(format!(
                "Cannot retrieve settings without user_id or org_id"
            ))?;
        }

        let user_id = context.user_id_json();
        let org_id = context.org_id_json();
        let workstation_id = context.workstation_id_json();

        if self.name_regex.is_none() {
            // Avoid recompiling the same regex -- it's not cheap.
            self.name_regex = Some(Regex::new(SETTING_NAME_REGEX).unwrap());
        }

        let reg = self.name_regex.as_ref().unwrap();

        for name in names {
            if reg.is_match(name) {
                Err(format!("Invalid setting name: {name}"))?;
            }
        }

        // First param is an SQL TEXT[].
        // e.g. '{foo.bar,foo.baz}'
        let names = format!("{{{}}}", names.join(","));

        let query = json::object! {
            from: [
                "actor.get_cascade_setting_batch",
                names, org_id, user_id, workstation_id
            ]
        };

        let settings = self.editor.json_query(query)?;

        for set in settings {
            self.store_setting_value(context, &set)?;
        }

        Ok(())
    }

    fn store_setting_value(
        &mut self,
        context: &SettingContext,
        setting: &json::Value,
    ) -> Result<(), String> {
        let value = match setting["value"].as_str() {
            Some(v) => match json::parse(v) {
                Ok(vv) => vv,
                Err(e) => Err(format!("Cannot parse setting value: {e}"))?,
            },
            None => json::Value::Null,
        };

        let name = setting["name"]
            .as_str()
            .ok_or(format!("Setting has no name"))?;

        let entry = SettingEntry {
            value: value,
            lookup_time: Instant::now(),
        };

        let hash = match self.cache.get_mut(context) {
            Some(h) => h,
            None => {
                self.cache.insert(context.clone(), HashMap::new());
                self.cache.get_mut(context).unwrap()
            }
        };

        hash.insert(name.to_string(), entry);

        Ok(())
    }
}
