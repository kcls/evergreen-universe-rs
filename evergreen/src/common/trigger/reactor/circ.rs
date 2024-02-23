//! Base module for A/T Reactors
use crate::editor::Editor;
use crate::common::trigger::{Event, EventState, Processor};
use crate::result::EgResult;

impl Processor {
    pub fn autorenew(&mut self, events: &[&mut Event]) -> EgResult<()> {

        Ok(())
    }
}

