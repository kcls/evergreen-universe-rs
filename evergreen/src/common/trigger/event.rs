use crate as eg;
use eg::result::{EgError, EgResult};
use eg::EgValue;
use std::fmt;

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

/// # Examples
/// ```
/// use evergreen::common::trigger::EventState;
/// assert!(EventState::try_from("complete").is_ok());
/// assert!(EventState::try_from("alligator").is_err());
/// ```
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
    target: EgValue,
    target_pkey: EgValue,
    group_value: Option<EgValue>,
    user_data: Option<EgValue>,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "A/T Event id={}", self.id)
    }
}

impl Event {
    /// Create an Event from an un-fleshed "atev" object.
    pub fn from_source(source: EgValue) -> EgResult<Event> {
        // required field w/ limited set of values
        let state: EventState = source["state"].as_str().unwrap().try_into()?;

        let id = source.id()?;
        let event_def = source["event_def"].int()?;

        let user_data = if let Some(data) = source["user_data"].as_str() {
            match EgValue::parse(data) {
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
            target_pkey: source["target"].clone(),
            target: EgValue::Null,
        })
    }

    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn event_def(&self) -> i64 {
        self.event_def
    }
    pub fn target(&self) -> &EgValue {
        &self.target
    }
    pub fn set_target(&mut self, target: EgValue) {
        self.target = target
    }

    /// Pkey value may be a number or string.
    pub fn target_pkey(&self) -> &EgValue {
        &self.target_pkey
    }
    pub fn state(&self) -> EventState {
        self.state
    }
    pub fn set_state(&mut self, state: EventState) {
        self.state = state;
    }
    pub fn user_data(&self) -> Option<&EgValue> {
        self.user_data.as_ref()
    }
    pub fn group_value(&self) -> Option<&EgValue> {
        self.group_value.as_ref()
    }
    pub fn set_group_value(&mut self, value: EgValue) {
        self.group_value = Some(value);
    }
}
