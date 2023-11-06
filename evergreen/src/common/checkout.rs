use crate::common::circulator::{CircOp, Circulator};
use crate::common::billing;
use crate::common::holds;
use crate::common::penalty;
use crate::common::targeter;
use crate::common::transit;
use crate::common::noncat;
use crate::constants as C;
use crate::date;
use crate::event::EgEvent;
use crate::result::{EgError, EgResult};
use crate::util::{json_bool, json_float, json_int, json_string};
use chrono::{Duration, Local, Timelike};
use json::JsonValue;
use std::collections::HashSet;

/// Performs item checkins
impl Circulator {

    /// Checkout an item.
    ///
    /// Returns Ok(()) if the active transaction should be committed and
    /// Err(EgError) if the active transaction should be rolled backed.
    pub fn checkout(&mut self) -> EgResult<()> {
        if self.circ_op == CircOp::Unset {
            self.circ_op = CircOp::Checkout;
        }

        if self.patron.is_none() {
            return self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND");
        }

        if self.is_noncat {
            return self.checkout_noncat();
        }

        log::info!("{self} starting checkout");


        Ok(())
    }


    fn checkout_noncat(&mut self) -> EgResult<()> {
        let noncat_type = match self.options.get("noncat_type") {
            Some(v) => v,
            None => return Err(format!("noncat_type required").into()),
        };

        let circ_lib = match self.options.get("noncat_circ_lib") {
            Some(cl) => json_int(&cl)?,
            None => self.circ_lib
        };

        let count = match self.options.get("noncat_count") {
            Some(c) => json_int(&c)?,
            None => 1,
        };

        let mut checkout_time = None;
        if let Some(ct) = self.options.get("checkout_time") {
            if let Some(ct2) = ct.as_str() {
                checkout_time = Some(ct2);
            }
        }

        let circs = noncat::checkout(
            &mut self.editor,
            json_int(&self.patron.as_ref().unwrap()["id"])?,
            json_int(&noncat_type)?,
            circ_lib,
            count,
            checkout_time,
        )?;

        let mut evt = EgEvent::success();
        if circs.len() > 0 {
            // Perl API only returns the last created circulation
            evt.set_payload(json::object! {"noncat_circ": circs[circs.len() - 1].to_owned()});
        }
        self.add_event(evt);

        Ok(())
    }
}


