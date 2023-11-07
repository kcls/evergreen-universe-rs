use crate::common::billing;
use crate::common::circulator::{CircOp, Circulator};
use crate::common::holds;
use crate::common::noncat;
use crate::common::org;
use crate::common::penalty;
use crate::common::targeter;
use crate::common::transit;
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

        if !self.is_renewal() {
            // We'll already be init-ed if we're renewing.
            self.init()?;
        }

        if self.patron.is_none() {
            return self.exit_err_on_event_code("ACTOR_USER_NOT_FOUND");
        }

        self.handle_deleted_copy();

        if self.is_noncat {
            return self.checkout_noncat();
        }

        if self.is_precat() {
            self.create_precat_copy()?;
        } else if self.is_precat_copy() {
            self.exit_err_on_event_code("ITEM_NOT_CATALOGED")?;
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
            None => self.circ_lib,
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

        let mut circs = noncat::checkout(
            &mut self.editor,
            json_int(&self.patron.as_ref().unwrap()["id"])?,
            json_int(&noncat_type)?,
            circ_lib,
            count,
            checkout_time,
        )?;

        let mut evt = EgEvent::success();
        if let Some(c) = circs.pop() {
            // Perl API only returns the last created circulation
            evt.set_payload(json::object! {"noncat_circ": c});
        }
        self.add_event(evt);

        Ok(())
    }

    fn create_precat_copy(&mut self) -> EgResult<()> {
        if !self.is_renewal() {
            if !self.editor.allowed("CREATE_PRECAT")? {
                return Err(self.editor.die_event());
            }
        }

        // We already have a matching precat copy.
        // Update so we can reuse it.
        if self.copy.is_some() {
            return self.update_existing_precat();
        }

        let dummy_title = self
            .options
            .get("dummy_title")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let dummy_author = self
            .options
            .get("dummy_author")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let dummy_isbn = self
            .options
            .get("dummy_isbn")
            .map(|dt| dt.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        let circ_modifier = self
            .options
            .get("circ_modifier")
            .map(|m| m.as_str())
            .unwrap_or(Some(""))
            .unwrap();

        // Barcode required to get this far.
        let copy_barcode = self.copy_barcode.as_deref().unwrap();

        log::info!("{self} creating new pre-cat copy {copy_barcode}");

        let copy = json::object! {
            "circ_lib": self.circ_lib,
            "creator": self.editor.requestor_id(),
            "editor": self.editor.requestor_id(),
            "barcode": copy_barcode,
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
            "call_number": C::PRECAT_CALL_NUMBER,
            "loan_duration": C::PRECAT_COPY_LOAN_DURATION,
            "fine_level": C::PRECAT_COPY_FINE_LEVEL,
        };

        let mut copy = self.editor.idl().create_from("acp", copy)?;

        let pclib = self
            .settings
            .get_value_at_org("circ.pre_cat_copy_circ_lib", self.circ_lib)?;

        if let Some(sn) = pclib.as_str() {
            let o = org::by_shortname(&mut self.editor, sn)?;
            copy["circ_lib"] = json::from(o["id"].clone());
        }

        self.copy = Some(self.editor.create(copy)?);

        Ok(())
    }

    fn update_existing_precat(&mut self) -> EgResult<()> {
        let copy = self.copy.as_ref().unwrap(); // known good.

        log::info!("{self} modifying existing pre-cat copy {}", copy["id"]);

        let dummy_title = self
            .options
            .get("dummy_title")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_title"].as_str())
            .unwrap_or("");

        let dummy_author = self
            .options
            .get("dummy_author")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_author"].as_str())
            .unwrap_or("");

        let dummy_isbn = self
            .options
            .get("dummy_isbn")
            .map(|dt| dt.as_str())
            .unwrap_or(copy["dummy_isbn"].as_str())
            .unwrap_or("");

        let circ_modifier = self
            .options
            .get("circ_modifier")
            .map(|m| m.as_str())
            .unwrap_or(copy["circ_modifier"].as_str());

        self.update_copy(json::object! {
            "editor": self.editor.requestor_id(),
            "edit_date": "now",
            "dummy_title": dummy_title,
            "dummy_author": dummy_author,
            "dummy_isbn": dummy_isbn,
            "circ_modifier": circ_modifier,
        })?;

        return Ok(());
    }
}
