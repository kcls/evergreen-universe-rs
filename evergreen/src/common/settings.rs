//! General purpose org / workstation / user setting fetcher and cache.
//! Primarily uses the 'actor.get_cascade_setting()' DB function.
use crate as eg;
use eg::{Editor, EgResult, EgValue};
use regex::Regex;
use std::collections::HashMap;
use std::fmt;

// Setting names consist only of letters, numbers, unders, and dots.
// This is crucial since the names are encoded as an SQL TEXT[] parameter
// during lookuping.
const SETTING_NAME_REGEX: &str = "[^a-zA-Z0-9_\\.]";

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
    pub fn org_id_value(&self) -> EgValue {
        self.org_id.map(EgValue::from).unwrap_or(eg::NULL)
    }
    pub fn user_id_value(&self) -> EgValue {
        self.user_id.map(EgValue::from).unwrap_or(eg::NULL)
    }
    pub fn workstation_id_value(&self) -> EgValue {
        self.workstation_id.map(EgValue::from).unwrap_or(eg::NULL)
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
    value: EgValue,
}

impl SettingEntry {
    pub fn value(&self) -> &EgValue {
        &self.value
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
            if let Ok(id) = reqr.id() {
                self.default_context.user_id = Some(id);
            }
            if let Some(id) = reqr["wsid"].as_int() {
                self.default_context.workstation_id = Some(id);
            }
            if let Some(id) = reqr["ws_ou"].as_int() {
                self.default_context.org_id = Some(id);
            } else if let Some(id) = reqr["home_ou"].as_int() {
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
    pub fn get_value(&mut self, name: &str) -> EgResult<&EgValue> {
        // Clone needed here because get_context_value mutably borrows
        // self a number of times.
        self.get_context_value(&self.default_context.clone(), name)
    }

    /// Shortcut for get_context_value with an org unit ID set.
    pub fn get_value_at_org(&mut self, name: &str, org_id: i64) -> EgResult<&EgValue> {
        let mut ctx = SettingContext::new();
        ctx.set_org_id(org_id);
        self.get_context_value(&ctx, name)
    }

    /// Returns a setting value for the provided context.
    pub fn get_context_value(
        &mut self,
        context: &SettingContext,
        name: &str,
    ) -> EgResult<&EgValue> {
        if self.cache.get(context).is_none() {
            self.cache.insert(context.clone(), HashMap::new());
        }

        if self.get_cached_value(context, name).is_none() {
            // No value in the cache.  Fetch it.
            self.fetch_context_values(context, &[name])?;
        }

        // fetch_context_values guarantees a value is applied
        // for this setting in the cache (defaulting to json null).
        self.get_cached_value(context, name)
            .ok_or_else(|| format!("Setting value missing from cache").into())
    }

    pub fn get_cached_value(&mut self, context: &SettingContext, name: &str) -> Option<&EgValue> {
        let hash = match self.cache.get_mut(context) {
            Some(h) => h,
            None => return None,
        };

        if hash.get(name).is_none() {
            return None;
        }

        Some(hash.get(name).unwrap().value())
    }

    /// Batch setting value fetch.
    ///
    /// Returns String Err on load failure or invalid setting name.
    /// On success, values are stored in the local cache for this
    /// Setting instance.
    pub fn fetch_values(&mut self, names: &[&str]) -> EgResult<()> {
        self.fetch_context_values(&self.default_context.clone(), names)
    }

    /// Fetch (pre-cache) a batch of values for a given org unit.
    pub fn fetch_values_for_org(&mut self, org_id: i64, names: &[&str]) -> EgResult<()> {
        let mut ctx = SettingContext::new();
        ctx.set_org_id(org_id);

        let names: Vec<&str> = names
            .iter()
            .filter(|n| self.get_cached_value(&ctx, n).is_none())
            .map(|n| *n)
            .collect();

        if names.len() > 0 {
            self.fetch_context_values(&ctx, names.as_slice())
        } else {
            Ok(())
        }
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
    ) -> EgResult<()> {
        if !context.is_viable() {
            Err(format!(
                "Cannot retrieve settings without user_id or org_id"
            ))?;
        }

        let user_id = context.user_id_value();
        let org_id = context.org_id_value();
        let workstation_id = context.workstation_id_value();

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

        let query = eg::hash! {
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

    fn store_setting_value(&mut self, context: &SettingContext, setting: &EgValue) -> EgResult<()> {
        let value = match setting["value"].as_str() {
            Some(v) => match EgValue::parse(v) {
                Ok(vv) => vv,
                Err(e) => Err(format!("Cannot parse setting value: {e}"))?,
            },
            None => EgValue::Null,
        };

        let name = setting["name"]
            .as_str()
            .ok_or_else(|| format!("Setting has no name"))?;

        let entry = SettingEntry { value };

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
