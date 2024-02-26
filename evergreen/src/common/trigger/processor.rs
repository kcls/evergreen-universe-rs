/// Main entry point for processing A/T events related to a
/// given event definition.
use crate::common::trigger::{Event, EventState};
use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use json::JsonValue;
use opensrf::util::thread_id;
use std::fmt;
use std::process;
use std::cell::RefCell;

// Add feature to roll-back failures and reset event states.
// Add a retry state that's only processed intentionally?
pub struct Processor {
    pub editor: RefCell<Editor>,
    event_def_id: i64,
    event_def: JsonValue,
    target_flesh: JsonValue,
}

impl fmt::Display for Processor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Processor A/T Definition [id={}] '{}'",
            self.event_def_id, self.event_def["name"]
        )
    }
}

impl Processor {
    pub fn new(editor: &Editor, event_def_id: i64) -> EgResult<Self> {
        let mut editor = editor.clone(); // TODO

        let flesh = json::object! {
            "flesh": 1,
            "flesh_fields": {"atevdef": ["hook", "env", "params"]}
        };

        let event_def = editor
            .retrieve_with_ops("atevdef", event_def_id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        let mut proc = Self {
            event_def,
            event_def_id,
            target_flesh: JsonValue::Null,
            editor: RefCell::new(editor),
        };

        proc.set_target_flesh()?;

        Ok(proc)
    }

    /*
    pub fn from_event(editor: &Editor, event_id: i64) -> EgResult<Self> {

    }
    */

    pub fn event_def_id(&self) -> i64 {
        self.event_def_id
    }
    pub fn event_def(&self) -> &JsonValue {
        &self.event_def
    }
    pub fn core_type(&self) -> &str {
        self.event_def["hook"]["core_type"].as_str().unwrap()
    }
    pub fn user_field(&self) -> Option<&str> {
        self.event_def["usr_field"].as_str()
    }
    pub fn group_field(&self) -> Option<&str> {
        self.event_def["group_field"].as_str()
    }
    pub fn validator(&self) -> &str {
        self.event_def["validator"].as_str().unwrap()
    }
    pub fn reactor(&self) -> &str {
        self.event_def["reactor"].as_str().unwrap()
    }
    pub fn environment(&self) -> &JsonValue {
        &self.event_def["env"]
    }

    /// Will be a JSON array
    pub fn params(&self) -> &JsonValue {
        &self.event_def["params"]
    }

    /// Compile the flesh expression we'll use each time we
    /// fetch an event from the database.
    fn set_target_flesh(&mut self) -> EgResult<()> {
        let mut paths: Vec<&str> = self
            .environment()
            .members()
            .map(|e| e["path"].as_str().unwrap()) // required
            .collect();

        let group_field: String;
        if let Some(gfield) = self.group_field() {
            // If there is a group field path, flesh it as well.
            let mut gfield: Vec<&str> = gfield.split(".").collect();

            // However, drop the final part which is a field name
            // and does not need to be fleshed.
            gfield.pop();

            if gfield.len() > 0 {
                group_field = gfield.join(".");
                paths.push(&group_field);
            }
        }

        self.target_flesh = self
            .editor
            .borrow()
            .idl()
            .field_paths_to_flesh(self.core_type(), paths.as_slice())?;

        Ok(())
    }

    /// Returns the parameter value with the provided name or None if no
    /// such parameter exists.
    pub fn param_value(&self, param_name: &str) -> Option<&JsonValue> {
        for param in self.params().members() {
            if param["param"].as_str() == Some(param_name) {
                return Some(&param["value"]);
            }
        }
        None
    }

    /// Returns the parameter value with the provided name as a &str or
    /// None if no such parameter exists OR the parameter is not a JSON
    /// string.
    pub fn param_value_as_str(&self, param_name: &str) -> Option<&str> {
        if let Some(pval) = self.param_value(param_name) {
            pval["value"].as_str()
        } else {
            None
        }
    }

    /// Returns true if a parameter value exists and has truthy,
    /// false otherwise.
    pub fn param_value_as_bool(&self, param_name: &str) -> bool {
        if let Some(pval) = self.param_value(param_name) {
            util::json_bool(&pval["value"])
        } else {
            false
        }
    }

    pub fn set_event_state(&self, event: &mut Event, state: EventState) -> EgResult<()> {
        self.set_event_state_impl(event, state, None)
    }

    pub fn set_event_state_error(&self, event: &mut Event, error_text: &str) -> EgResult<()> {
        self.set_event_state_impl(event, EventState::Error, Some(error_text))
    }

    /// Update the event state and related state-tracking values.
    fn set_event_state_impl(
        &self,
        event: &mut Event,
        state: EventState,
        error_text: Option<&str>,
    ) -> EgResult<()> {
        event.set_state(state);

        let state_str: &str = state.into();

        self.editor.borrow_mut().xact_begin()?;

        let mut atev = self
            .editor
            .borrow_mut()
            .retrieve("atev", event.id())?
            .ok_or_else(|| format!("Our event disappeared from the DB?"))?;

        if let Some(err) = error_text {
            let output = json::object! {
                "data": err,
                "is_error": true,
                // TODO locale
            };

            let output = self.editor.borrow().idl().create_from("ateo", output)?;
            let mut result = self.editor.borrow_mut().create(output)?;

            atev["error_output"] = result["id"].take();
        }

        atev["state"] = json::from(state_str);
        atev["update_time"] = json::from("now");
        atev["update_process"] = json::from(format!("{}-{}", process::id(), thread_id()));

        if atev["start_time"].is_null() && state != EventState::Pending {
            atev["start_time"] = json::from("now");
        }

        if state == EventState::Complete {
            atev["complete_time"] = json::from("now");
        }

        self.editor.borrow_mut().update(atev)?;

        self.editor.borrow_mut().xact_commit()
    }

    /// Flesh the target linked to this event and set the event
    /// group value if necessary.
    pub fn collect(&self, event: &mut Event) -> EgResult<()> {
        self.set_event_state(event, EventState::Collecting)?;

        // Fetch our target object with the needed fleshing.
        // clone() is required for retrieve()
        let flesh = self.target_flesh.clone();
        //let core_type = self.core_type().to_string(); // parallel mut's
        let core_type = self.core_type();

        let target = self
            .editor
            .borrow_mut()
            .retrieve_with_ops(&core_type, event.target_pkey(), flesh)?
            .ok_or_else(|| self.editor.borrow_mut().die_event())?;

        event.set_target(target);

        self.set_group_value(event)?;

        // TODO additional data is needed for user_message support.

        self.set_event_state(event, EventState::Collected)
    }

    /// If this is a grouped event, apply the group-specific value
    /// to the provided event.
    ///
    /// This value is used to sort events into like groups.
    fn set_group_value(&self, event: &mut Event) -> EgResult<()> {
        let gfield_path = match self.group_field() {
            Some(f) => f,
            None => return Ok(()),
        };

        let mut obj = event.target();

        for part in gfield_path.split(".") {
            obj = &obj[part];
        }

        let pkey_value;
        if self.editor.borrow().idl().is_idl_object(obj) {
            // The object may have been fleshed beyond where we
            // need it during target collection. If so, extract
            // the pkey value from the fleshed object.

            pkey_value = self.editor.borrow().idl().get_pkey_value(obj).ok_or_else(|| {
                format!("Group field object has no primary key? path={gfield_path}")
            })?;

            obj = &pkey_value;
        }

        if obj.is_string() || obj.is_number() {
            event.set_group_value(obj.clone());
            Ok(())
        } else {
            Err(format!("Invalid group field path: {gfield_path}").into())
        }
    }

    pub fn process_event(&self, event_id: i64) -> EgResult<()> {
        log::info!("{self} processing event {event_id}");

        let event = self.editor.borrow_mut().retrieve("atev", event_id)?
            .ok_or_else(|| self.editor.borrow_mut().die_event())?;

        let mut event = Event::from_source(event)?;

        self.collect(&mut event)?;

        if self.validate(&mut event)? {
            self.react(&mut [&mut event])?;
        }

        Ok(())
    }

    pub fn process_event_group(&self, event_ids: &[i64]) -> EgResult<()> {
        log::info!("{self} processing event group {event_ids:?}");

        let query = json::object! {"id": event_ids};
        let atevs = self.editor.borrow_mut().search("atev", query)?;

        if atevs.len() == 0 {
            return Err(format!("No such events: {event_ids:?}").into());
        }

        let mut valid_events = Vec::new();
        for atev in atevs {
            let mut event = Event::from_source(atev)?;

            self.collect(&mut event)?;
            if self.validate(&mut event)? {
                valid_events.push(event);
            }
        }

        if valid_events.len() == 0 {
            // No valid events to react
            return Ok(());
        }

        let mut refs: Vec<&mut Event> = valid_events.iter_mut().collect();
        let slice = &mut refs[..];
        self.react(slice)
    }
}

