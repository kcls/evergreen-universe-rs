/// Main entry point for processing A/T events related to a
/// given event definition.
use crate as eg;
use eg::common::trigger::{Event, EventState};
use eg::util::thread_id;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use std::fmt;
use std::process;

// Add feature to roll-back failures and reset event states.
// Add a retry state that's only processed intentionally?
pub struct Processor<'a> {
    pub editor: &'a mut Editor,
    event_def_id: i64,
    event_def: EgValue,
    target_flesh: EgValue,
}

impl fmt::Display for Processor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Processor A/T Definition [id={}] '{}'",
            self.event_def_id, self.event_def["name"]
        )
    }
}

impl<'a> Processor<'a> {
    pub fn new(editor: &'a mut Editor, event_def_id: i64) -> EgResult<Processor> {
        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": {"atevdef": ["hook", "env", "params"]}
        };

        let event_def = editor
            .retrieve_with_ops("atevdef", event_def_id, flesh)?
            .ok_or_else(|| editor.die_event())?;

        let mut proc = Self {
            event_def,
            event_def_id,
            target_flesh: EgValue::Null,
            editor,
        };

        proc.set_target_flesh()?;

        Ok(proc)
    }

    /// One-off single event processor without requiring a standalone Processor
    pub fn process_event_once(editor: &mut Editor, event_id: i64) -> EgResult<Event> {
        let jevent = editor
            .retrieve("atev", event_id)?
            .ok_or_else(|| editor.die_event())?;

        let mut proc = Processor::new(editor, jevent["event_def"].int_required())?;

        let mut event = Event::from_source(jevent)?;

        proc.process_event(&mut event)?;

        Ok(event)
    }

    /// Process a single event via an existing Processor
    pub fn process_event(&mut self, event: &mut Event) -> EgResult<()> {
        log::info!("{self} processing event {}", event.id());

        self.collect(event)?;

        if self.validate(event)? {
            self.react(&mut [event])?;
            self.set_event_state(event, EventState::Complete)?;
        }

        Ok(())
    }

    /// One-off event group processor without requiring a standalone Processor
    ///
    /// Returns all processed events, even if invalid.
    pub fn process_event_group_once(
        editor: &mut Editor,
        event_ids: &[i64],
    ) -> EgResult<Vec<Event>> {
        let query = eg::hash! {"id": event_ids};
        let mut jevents = editor.search("atev", query)?;

        if jevents.len() == 0 {
            return Err(format!("No such events: {event_ids:?}").into());
        }

        // Here we trust that events from the database are shaped correctly.
        let mut events: Vec<Event> = Vec::new();
        for jevent in jevents.drain(..) {
            events.push(Event::from_source(jevent)?);
        }

        let mut proc = Processor::new(editor, events[0].id())?;

        let mut slice = events.iter_mut().collect::<Vec<&mut Event>>();
        proc.process_event_group(&mut slice[..])?;

        Ok(events)
    }

    pub fn process_event_group(&mut self, events: &mut [&mut Event]) -> EgResult<()> {
        let mut valid_events: Vec<&mut Event> = Vec::new();
        for event in events.iter_mut() {
            self.collect(event)?;
            if self.validate(event)? {
                valid_events.push(event);
            }
        }

        if valid_events.len() == 0 {
            // No valid events to react
            return Ok(());
        }

        let slice = &mut valid_events[..];
        self.react(slice)?;

        for event in valid_events {
            self.set_event_state(event, EventState::Complete)?;
        }

        Ok(())
    }

    pub fn event_def_id(&self) -> i64 {
        self.event_def_id
    }
    pub fn event_def(&self) -> &EgValue {
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
    pub fn environment(&self) -> &EgValue {
        &self.event_def["env"]
    }

    /// Will be a JSON array
    pub fn params(&self) -> &EgValue {
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
    pub fn param_value(&mut self, param_name: &str) -> Option<&EgValue> {
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
    pub fn param_value_as_str(&mut self, param_name: &str) -> Option<&str> {
        if let Some(pval) = self.param_value(param_name) {
            pval["value"].as_str()
        } else {
            None
        }
    }

    /// Returns true if a parameter value exists and has truthy,
    /// false otherwise.
    pub fn param_value_as_bool(&mut self, param_name: &str) -> bool {
        if let Some(pval) = self.param_value(param_name) {
            pval["value"].as_boolish()
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

        let mut atev = self
            .editor
            .retrieve("atev", event.id())?
            .ok_or_else(|| format!("Our event disappeared from the DB?"))?;

        if let Some(err) = error_text {
            let mut output = eg::hash! {
                "data": err,
                "is_error": true,
                // TODO locale
            };
            output.bless("ateo")?;

            let mut result = self.editor.create(output)?;

            atev["error_output"] = result["id"].take();
        }

        atev["state"] = EgValue::from(state_str);
        atev["update_time"] = EgValue::from("now");
        atev["update_process"] = EgValue::from(format!("{}-{}", process::id(), thread_id()));

        if atev["start_time"].is_null() && state != EventState::Pending {
            atev["start_time"] = EgValue::from("now");
        }

        if state == EventState::Complete {
            atev["complete_time"] = EgValue::from("now");
        }

        self.editor.update(atev)?;

        self.editor.xact_commit()?;

        if state == EventState::Complete || state == EventState::Error {
            // If we're likely done, force a disconnect.
            // This does not prevent additional connects/begins/etc.
            self.editor.disconnect()
        } else {
            Ok(())
        }
    }

    /// Flesh the target linked to this event and set the event
    /// group value if necessary.
    pub fn collect(&mut self, event: &mut Event) -> EgResult<()> {
        log::info!("{self} collecting {event}");

        self.set_event_state(event, EventState::Collecting)?;

        // Fetch our target object with the needed fleshing.
        // clone() is required for retrieve()
        let flesh = self.target_flesh.clone();
        let core_type = self.core_type().to_string(); // parallel mut's

        let target = self
            .editor
            .retrieve_with_ops(&core_type, event.target_pkey().clone(), flesh)?
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

        let obj_clone;
        if let Some(pkey) = obj.pkey_value() {
            // The object may have been fleshed beyond where we
            // need it during target collection. If so, extract
            // the pkey value from the fleshed object.
            obj_clone = pkey.clone();
        } else {
            obj_clone = obj.clone();
        }

        if obj.is_string() || obj.is_number() {
            event.set_group_value(obj_clone);
            Ok(())
        } else {
            Err(format!("Invalid group field path: {gfield_path}").into())
        }
    }
}
