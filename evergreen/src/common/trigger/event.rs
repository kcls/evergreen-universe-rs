use crate::editor::Editor;
use crate::result::{EgResult, EgError};
use std::collections::HashMap;
use std::process;
use json::JsonValue;
use opensrf::util::thread_id;
use crate::util;

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
            "collected" => Ok(Self::Collected),
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
    atev: JsonValue,
    event_def: JsonValue,
    state: EventState,
    target: JsonValue,
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

        let mut atev = editor.retrieve_with_ops("atev", id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        // De-flesh the event so we can track the event-def separately.
        let def_id = atev["event_def"]["id"].clone();
        let event_def = atev["event_def"].take();
        atev["event_def"] = def_id;

        // required field w/ in-db enum of values.
        let state: EventState = atev["state"].as_str().unwrap().try_into()?;

        let user_data = if let Some(data) = atev["user_data"].as_str() {
            match json::parse(data) {
                Ok(d) => Some(d),
                Err(e) => return Err(format!(
                    "Invalid user data for event {id}: {e} {data}").into()),
            }
        } else {
            None
        };

        let classname = atev["hook"]["core_type"].as_str().unwrap().to_string(); // required

        let target = editor.retrieve(&classname, atev["target"].clone())?
            .ok_or_else(|| editor.die_event())?;

        Ok(Event {
            id,
            atev,
            event_def,
            state,
            target,
            user_data,
            core_class: classname,
            environment: HashMap::new(),
        })
    }

    /// Update the event state and related state-tracking values.
    pub fn update_state(&mut self, editor: &mut Editor, state: EventState) -> EgResult<()> {
        let state_str: &str = state.into();

        self.atev["state"] = json::from(state_str);
        self.atev["update_time"] = json::from("now");
        self.atev["update_process"] = 
            json::from(format!("{}-{}", process::id(), thread_id()));

        if self.atev["start_time"].is_null() && state != EventState::Pending {
            self.atev["start_time"] = json::from("now");
        }

        if state == EventState::Complete {
            self.atev["complete_time"] = json::from("now");
        }

        editor.update(self.atev.clone())
    }

    pub fn build_environment(&mut self, editor: &mut Editor) -> EgResult<()> {
        self.update_state(editor, EventState::Collecting)?;

        // Map the environment path strings into a cstore flesh object.
        let mut flesh = json::object! {"flesh_fields": {}};
        let mut flesh_depth = 1;

        let paths: Vec<&str> = self.event_def["env"]
            .members()
            .map(|e| e["path"].as_str().unwrap()) // required string field
            .collect();

        let flesh = editor.idl().field_paths_to_flesh(self.core_class(), paths.as_slice())?;

        // TODO make target an Option so it can be fetched later.

        self.update_state(editor, EventState::Collected)
    }
}
