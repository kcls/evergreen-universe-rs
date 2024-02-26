use crate::common::trigger::validator;
use crate::editor::Editor;
use crate::result::{EgError, EgResult};
use crate::util;
use json::JsonValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EventState {
    Pending,
    Collecting,
    Collected,
    Validating,
    Valid,
    Invalid,
    Reacting,
    Reacted,
    Cleaning,
    Complete,
    Error,
}

impl TryFrom<&str> for EventState {
    type Error = EgError;
    fn try_from(value: &str) -> Result<Self, EgError> {
        match value {
            "pending" => Ok(Self::Pending),
            "collecting" => Ok(Self::Collecting),
            "collected" => Ok(Self::Collected),
            "validating" => Ok(Self::Validating),
            "valid" => Ok(Self::Valid),
            "invalid" => Ok(Self::Invalid),
            "reacting" => Ok(Self::Reacting),
            "reacted" => Ok(Self::Reacted),
            "cleaning" => Ok(Self::Cleaning),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            _ => Err(format!("Invalid Trigger Event State: {value}").into()),
        }
    }
}

impl From<EventState> for &'static str {
    fn from(state: EventState) -> &'static str {
        match state {
            EventState::Pending => "pending",
            EventState::Collecting => "collecting",
            EventState::Collected => "collected",
            EventState::Validating => "validating",
            EventState::Valid => "valid",
            EventState::Invalid => "invalid",
            EventState::Reacting => "reacting",
            EventState::Reacted => "reacted",
            EventState::Cleaning => "cleaning",
            EventState::Complete => "complete",
            EventState::Error => "error",
        }
    }
}

pub struct Event {
    id: i64,
    event_def: i64,
    state: EventState,
    target: JsonValue,
    group_value: Option<JsonValue>,
    user_data: Option<JsonValue>,
}

impl Event {
    /// Create an Event from an un-fleshed "atev" object.
    pub fn from_source(source: JsonValue) -> EgResult<Event> {
        // required field w/ limited set of values
        let state: EventState = source["state"].as_str().unwrap().try_into()?;

        let id = util::json_int(&source["id"])?;
        let event_def = util::json_int(&source["event_def"])?;

        let user_data = if let Some(data) = source["user_data"].as_str() {
            match json::parse(data) {
                Ok(d) => Some(d),
                Err(e) => {
                    return Err(format!("Invalid user data for event {id}: {e} {data}").into())
                }
            }
        } else {
            None
        };

        Ok(Event {
            id,
            event_def,
            state,
            user_data,
            group_value: None,
            target: JsonValue::Null,
        })
    }

    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn event_def(&self) -> i64 {
        self.event_def
    }
    pub fn target(&self) -> &JsonValue {
        &self.target
    }
    pub fn set_target(&mut self, target: JsonValue) {
        self.target = target
    }

    /// Pkey value may be a number or string.
    pub fn target_pkey(&self) -> &JsonValue {
        &self.target
    }
    pub fn state(&self) -> EventState {
        self.state
    }
    pub fn set_state(&mut self, state: EventState) {
        self.state = state;
    }
    pub fn user_data(&self) -> Option<&JsonValue> {
        self.user_data.as_ref()
    }
    pub fn group_value(&self) -> Option<&JsonValue> {
        self.group_value.as_ref()
    }
    pub fn set_group_value(&mut self, value: JsonValue) {
        self.group_value = Some(value);
    }
}
