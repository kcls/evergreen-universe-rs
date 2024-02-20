use crate::editor::Editor;
use crate::result::{EgResult, EgError};
use std::collections::HashMap;
use json::JsonValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EventState {
    Pending,
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
            "pending"   => Ok(Self::Pending),
            "valid"     => Ok(Self::Valid),
            "invalid"   => Ok(Self::Invalid),
            "reacting"  => Ok(Self::Reacting),
            "cleaning"  => Ok(Self::Cleaning),
            "complete"  => Ok(Self::Complete),
            "error"     => Ok(Self::Error),
            _           => Err(format!("Invalid Trigger Event State: {value}").into()),
        }
    }
}

#[rustfmt::skip]
impl From<&EventState> for &'static str {
    fn from(state: &EventState) -> &'static str {
        match *state {
            EventState::Pending   => "pending",
            EventState::Valid     => "valid",
            EventState::Invalid   => "invalid",
            EventState::Reacting  => "reacting",
            EventState::Cleaning  => "cleaning",
            EventState::Complete  => "complete",
            EventState::Error     => "error",
        }
    }
}


pub struct Event {
    id: i64,
    atev: JsonValue,
    state: EventState,
    target: JsonValue,
    user_data: Option<JsonValue>,
    environment: HashMap<String, JsonValue>,
}

impl Event {
    pub fn from_id(editor: &mut Editor, id: i64) -> EgResult<Event> {
        let flesh = json::object! {
            "flesh": 2,
            "flesh_fields": {
                "atev": ["event_def"],
                "atevdef": ["hook", "env", "params"]
            }
        };

        let atev = editor.retrieve_with_ops("atev", id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        // required field
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

        let classname = atev["hook"]["core_type"].as_str().unwrap(); // required

        let target = editor.retrieve(classname, atev["target"].clone())?
            .ok_or_else(|| editor.die_event())?;

        Ok(Event {
            id,
            atev,
            state,
            target,
            user_data,
            environment: HashMap::new(),
        })
    }
}
