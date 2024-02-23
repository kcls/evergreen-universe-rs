use crate::common::trigger::{Event, EventState, Processor};
use crate::editor::Editor;
/// Base module for A/T Reactors
use crate::result::EgResult;

mod circ;

/// Add reactor routines to the Processor.
impl Processor {
    /// React or more events.
    ///
    /// Assumes all events use the same event-def / reactor.
    pub fn react(&mut self, events: &[&Event]) -> EgResult<()> {
        if events.len() == 0 {
            return Ok(());
        }

        // required string field.
        /*
        let reactor = events[0].event_def()["reactor"].as_str().unwrap();

        match reactor {
            "NOOP_True" || "NOOP_False" => Ok(()),
            "Circ::AutoRenew" => circ::autorenew(editor, events),
            _ => Err(format!("No such reactor: {reactor}").into()),
        }
        */

        Ok(())
    }
}
