use super::editor::Editor;
use super::util;
use json::JsonValue;
use regex::Regex;
/// General purpose org / workstation / user setting fetcher and cache.
use std::collections::HashMap;

const JSON_NULL: JsonValue = JsonValue::Null;

// Setting names consist only of letters, numbers, unders, and dots.
// This is crucial since the names are encoded as an SQL TEXT[] parameter
// during lookuping.
const SETTING_NAME_REGEX: &str = "[^a-zA-Z0-9_\\.]";

#[derive(Debug, Clone)]
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

pub struct SettingsCache {
    editor: Editor,
    org_id: Option<i64>,
    user_id: Option<i64>,
    workstation_id: Option<i64>,
    name_regex: Option<Regex>,
    cache: HashMap<String, JsonValue>,
    types: HashMap<String, SettingType>,
}

impl SettingsCache {
    pub fn new(editor: &Editor) -> SettingsCache {
        let mut sc = SettingsCache {
            org_id: None,
            user_id: None,
            workstation_id: None,
            name_regex: None,
            editor: editor.clone(),
            cache: HashMap::new(),
            types: HashMap::new(),
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
                self.user_id = Some(id);
            }
            if let Ok(id) = util::json_int(&reqr["wsid"]) {
                self.workstation_id = Some(id);
            }
            if let Ok(id) = util::json_int(&reqr["ws_ou"]) {
                self.org_id = Some(id);
            } else if let Ok(id) = util::json_int(&reqr["home_ou"]) {
                self.org_id = Some(id);
            }
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

    pub fn reset(&mut self) {
        self.cache.clear();
    }

    /// Returns a setting value using the default context info.
    ///
    /// Returns JSON null if no setting exists.
    pub fn get_value(&mut self, name: &str) -> Result<&JsonValue, String> {
        if !self.cache.contains_key(name) {
            self.fetch_values(&[name])?;
        }
        Ok(self.cache.get(name).unwrap_or(&JSON_NULL))
    }

    /// Batch setting value fetch.
    ///
    /// Returns String Err on load failure or invalid setting name.
    pub fn fetch_values(&mut self, names: &[&str]) -> Result<(), String> {
        let user_id = match self.user_id {
            Some(id) => json::from(id),
            None => JSON_NULL,
        };

        let org_id = match self.org_id {
            Some(id) => json::from(id),
            None => JSON_NULL,
        };

        let workstation_id = match self.workstation_id {
            Some(id) => json::from(id),
            None => JSON_NULL,
        };

        if user_id.is_null() && org_id.is_null() {
            Err(format!(
                "Cannot retrieve settings without user_id or org_id"
            ))?;
        }

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
        let names = format!("{{{}}}", names.join(","));

        let query = json::object! {
            from: [
                "actor.get_cascade_setting_batch",
                names, org_id, user_id, workstation_id
            ]
        };

        let settings = self.editor.json_query(query)?;

        for set in settings {
            self.add_setting_value(&set)?;
        }

        Ok(())
    }

    fn add_setting_value(&mut self, setting: &JsonValue) -> Result<(), String> {
        let value = match setting["value"].as_str() {
            Some(v) => match json::parse(v) {
                Ok(vv) => vv,
                Err(e) => Err(format!("Cannot parse setting value: {e}"))?,
            },
            None => JsonValue::Null,
        };

        let name = setting["name"]
            .as_str()
            .ok_or(format!("Setting has no name"))?;
        let has_org_setting = util::json_bool(&setting["has_org_setting"]);
        let has_user_setting = util::json_bool(&setting["has_user_setting"]);
        let has_workstation_setting = util::json_bool(&setting["has_workstation_setting"]);

        let st = SettingType {
            name: name.to_string(),
            has_org_setting,
            has_user_setting,
            has_workstation_setting,
        };

        self.types.insert(name.to_string(), st);
        self.cache.insert(name.to_string(), value);

        Ok(())
    }
}
