use crate::editor::Editor;
use crate::result::{EgError, EgResult};
use crate::util;
use json::JsonValue;
use opensrf::util::thread_id;
use std::collections::HashMap;
use std::process;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EventState {
    Pending,
    Collecting,
    Collected,
    Valid,
    Invalid,
    Reacting,
    Cleaning,
    Complete,
    Error,
}

#[rustfmt::skip]
impl TryFrom<&str> for EventState {
    type Error = EgError;
    fn try_from(value: &str) -> Result<Self, EgError> {
        match value {
            "pending"    => Ok(Self::Pending),
            "collecting" => Ok(Self::Collecting),
            "collected"  => Ok(Self::Collected),
            "valid"      => Ok(Self::Valid),
            "invalid"    => Ok(Self::Invalid),
            "reacting"   => Ok(Self::Reacting),
            "cleaning"   => Ok(Self::Cleaning),
            "complete"   => Ok(Self::Complete),
            "error"      => Ok(Self::Error),
            _            => Err(format!("Invalid Trigger Event State: {value}").into()),
        }
    }
}

#[rustfmt::skip]
impl From<EventState> for &'static str {
    fn from(state: EventState) -> &'static str {
        match state {
            EventState::Pending    => "pending",
            EventState::Collecting => "collecting",
            EventState::Collected  => "collected",
            EventState::Valid      => "valid",
            EventState::Invalid    => "invalid",
            EventState::Reacting   => "reacting",
            EventState::Cleaning   => "cleaning",
            EventState::Complete   => "complete",
            EventState::Error      => "error",
        }
    }
}

pub struct Event {
    id: i64,
    event_def: JsonValue,
    group_value: Option<JsonValue>,
    state: EventState,
    target: JsonValue,
    target_pkey: JsonValue,
    core_class: String,
    user_data: Option<JsonValue>,
    environment: HashMap<String, JsonValue>,
}

impl Event {
    pub fn core_class(&self) -> &str {
        self.core_class.as_str()
    }

    pub fn from_id(editor: &mut Editor, id: i64) -> EgResult<Event> {
        let flesh = json::object! {
            "flesh": 2,
            "flesh_fields": {
                "atev": ["event_def"],
                "atevdef": ["hook", "env", "params"]
            }
        };

        let mut atev = editor
            .retrieve_with_ops("atev", id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        // De-flesh the event so we can track the event-def separately.
        let event_def = atev["event_def"].take();

        // required field w/ in-db enum of values.
        let state: EventState = atev["state"].as_str().unwrap().try_into()?;

        let user_data = if let Some(data) = atev["user_data"].as_str() {
            match json::parse(data) {
                Ok(d) => Some(d),
                Err(e) => {
                    return Err(format!("Invalid user data for event {id}: {e} {data}").into())
                }
            }
        } else {
            None
        };

        let classname = atev["hook"]["core_type"].as_str().unwrap().to_string(); // required

        let target_pkey = atev["target"].clone();

        Ok(Event {
            id,
            event_def,
            state,
            user_data,
            target_pkey,
            group_value: None,
            target: JsonValue::Null,
            core_class: classname,
            environment: HashMap::new(),
        })
    }

    /// Update the event state and related state-tracking values.
    pub fn update_state(&mut self, editor: &mut Editor, state: EventState) -> EgResult<()> {
        let state_str: &str = state.into();

        editor.xact_begin()?;

        let mut atev = editor
            .retrieve("atev", self.id)?
            .ok_or_else(|| format!("Our event disappeared from the DB?"))?;

        atev["state"] = json::from(state_str);
        atev["update_time"] = json::from("now");
        atev["update_process"] = json::from(format!("{}-{}", process::id(), thread_id()));

        if atev["start_time"].is_null() && state != EventState::Pending {
            atev["start_time"] = json::from("now");
        }

        if state == EventState::Complete {
            atev["complete_time"] = json::from("now");
        }

        editor.update(atev)?;

        editor.xact_commit()
    }

    pub fn build_environment(&mut self, editor: &mut Editor) -> EgResult<()> {
        self.update_state(editor, EventState::Collecting)?;

        // Map the environment path strings into a cstore flesh object.
        let mut flesh = json::object! {"flesh_fields": {}};
        let mut flesh_depth = 1;

        let mut paths: Vec<&str> = self.event_def["env"]
            .members()
            .map(|e| e["path"].as_str().unwrap()) // required string field
            .collect();

        let group_field: String;
        if let Some(gfield) = self.event_def["group_field"].as_str() {
            // If there is a group field path, flesh it as well.
            // The last component in the dotpath is field name that
            // represents the group value.  It does not require fleshing.
            let mut gfield: Vec<&str> = gfield.split(".").collect();
            gfield.pop();
            if gfield.len() > 0 {
                group_field = gfield.join(".");
                paths.push(&group_field);
            }
        }

        let flesh = editor
            .idl()
            .field_paths_to_flesh(self.core_class(), paths.as_slice())?;

        // Fetch our target object with the needed fleshing.
        self.target = editor
            .retrieve(self.core_class(), &self.target_pkey)?
            .ok_or_else(|| editor.die_event())?;

        self.set_group_value(editor)?;

        // TODO additional data is needed for user_message support.

        self.update_state(editor, EventState::Collected)
    }

    /// If this event has a group_field, extract the value referred
    /// to by the field path and save it for later.
    fn set_group_value(&mut self, editor: &Editor) -> EgResult<()> {
        let gfield_path = match self.event_def["group_field"].as_str() {
            Some(f) => f,
            None => return Ok(()),
        };

        let mut obj = &self.target;

        for part in gfield_path.split(".") {
            obj = &obj[part];
        }

        let pkey_value;
        if editor.idl().is_idl_object(obj) {
            // The object may have been fleshed beyond where we
            // need it via the environment.  Get the objects pkey value

            pkey_value = editor.idl().get_pkey_value(obj).ok_or_else(|| {
                format!("Group field object has no primary key? path={gfield_path}")
            })?;

            obj = &pkey_value;
        }

        if obj.is_string() || obj.is_number() {
            self.group_value = Some(obj.clone());
            Ok(())
        } else {
            Err(format!("Invalid group field path: {gfield_path}").into())
        }
    }

    /// Returns true if the event is considered valid.
    pub fn validate(&mut self, editor: &mut Editor) -> EgResult<bool> {
        self.update_state(editor, EventState::Validating);

        // TODO stacked validators
        // Add a validator mapping table, but have it default to
        // the single validator value if no table maps exist?

        self.update_state(editor, EventState::Valid)
    }
}
