use crate::common::circulator::Circulator;
use crate::event::EgEvent;

pub struct CheckinResult {
    events: Vec<EgEvent>,
}

impl CheckinResult {
    fn new() -> CheckinResult {
        CheckinResult {
            events: Vec::new(),
        }
    }
}

impl Circulator {
    pub fn checkin(&mut self) -> Result<CheckinResult, String> {
        let mut result = CheckinResult::new();

        Ok(result)
    }
}

