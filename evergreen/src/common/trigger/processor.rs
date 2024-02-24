/// Main entry point for processing A/T events related to a
/// given event definition.
use crate::common::trigger::{Event, EventState};
use crate::editor::Editor;
use crate::idl;
use crate::result::EgResult;
use crate::util;
use json::JsonValue;
use opensrf::util::thread_id;
use std::fmt;
use std::process;

// TODO
// set state to 'found' once an event is loaded.
//
// Add feature to roll-back failures and reset event states.
// Add a retry state that's only processed intentionally?
//
// Set Error state / text on failed validator, etc. in the calling
// code instead of within the handling module.

pub struct Processor {
    pub editor: Editor,
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
        let mut editor = editor.clone();

        let flesh = json::object! {
            "flesh": 1,
            "flesh_fields": {"atevdef": ["hook", "env", "params"]}
        };

        let event_def = editor
            .retrieve_with_ops("atevdef", event_def_id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        let mut proc = Self {
            event_def,
            editor,
            event_def_id,
            target_flesh: JsonValue::Null,
        };

        proc.set_target_flesh()?;

        Ok(proc)
    }

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

    pub fn set_event_state(&mut self, event: &mut Event, state: EventState) -> EgResult<()> {
        self.set_event_state_impl(event, state, None)
    }

    pub fn set_event_state_error(&mut self, event: &mut Event, error_text: &str) -> EgResult<()> {
        self.set_event_state_impl(event, EventState::Error, Some(error_text))
    }

    /// Update the event state and related state-tracking values.
    fn set_event_state_impl(
        &mut self,
        event: &mut Event,
        state: EventState,
        error_text: Option<&str>,
    ) -> EgResult<()> {
        event.set_state(state);

        let state_str: &str = state.into();

        self.editor.xact_begin()?;

        if let Some(err) = error_text {
            // TODO create action_trigger.event_output and link it
        }

        let mut atev = self
            .editor
            .retrieve("atev", event.id())?
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

        self.editor.update(atev)?;

        self.editor.xact_commit()
    }

    /// Flesh the target linked to this event and set the event
    /// group value if necessary.
    pub fn collect(&mut self, event: &mut Event) -> EgResult<()> {
        self.set_event_state(event, EventState::Collecting)?;

        // Fetch our target object with the needed fleshing.
        // clone() is required for retrieve()
        let flesh = self.target_flesh.clone();
        let core_type = self.core_type().to_string(); // parallel mut's

        let target = self
            .editor
            .retrieve_with_ops(&core_type, event.target_pkey(), flesh)?
            .ok_or_else(|| self.editor.die_event())?;

        event.set_target(target);

        self.set_group_value(event)?;

        // TODO additional data is needed for user_message support.

        self.set_event_state(event, EventState::Collected)
    }

    /// If this is a grouped event, apply the group-specific value
    /// to the provided event.
    ///
    /// This value is used to sort events into like groups.
    fn set_group_value(&mut self, event: &mut Event) -> EgResult<()> {
        let gfield_path = match self.group_field() {
            Some(f) => f,
            None => return Ok(()),
        };

        let mut obj = event.target();

        for part in gfield_path.split(".") {
            obj = &obj[part];
        }

        let pkey_value;
        if self.editor.idl().is_idl_object(obj) {
            // The object may have been fleshed beyond where we
            // need it during target collection. If so, extract
            // the pkey value from the fleshed object.

            pkey_value = self.editor.idl().get_pkey_value(obj).ok_or_else(|| {
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
}
