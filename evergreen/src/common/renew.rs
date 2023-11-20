use crate::common::circulator::{CircOp, Circulator};
use crate::common::holds;
use crate::date;
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util::{json_bool, json_bool_op, json_int};
use json::JsonValue;
/*
use crate::common::bib;
use crate::common::billing;
use crate::common::noncat;
use crate::common::org;
use crate::common::penalty;
use crate::constants as C;
use std::time::Duration;
*/

/// Performs item checkins
impl Circulator {
    /// Renew a circulation.
    ///
    /// Returns Ok(()) if the active transaction should be committed and
    /// Err(EgError) if the active transaction should be rolled backed.
    pub fn renew(&mut self) -> EgResult<()> {
        self.circ_op = CircOp::Renew;
        self.init()?;

        log::info!("{self} starting renew");

        self.load_renewal_circ()?;
        self.basic_renewal_checks()?;

        // Do this after self.basic_renewal_checks which may change
        // our circ lib.
        if !self
            .editor
            .as_mut()
            .unwrap()
            .allowed_at("COPY_CHECKOUT", self.circ_lib)?
        {
            return Err(self.editor().die_event());
        }

        self.checkin()?;
        self.checkout()
    }

    /// Find the circ we're trying to renew and extra the patron info.
    pub fn load_renewal_circ(&mut self) -> EgResult<()> {
        let mut query = json::object! {
            "target_copy": self.copy_id,
            "xact_finish": JsonValue::Null,
            "checkin_time": JsonValue::Null,
        };

        if self.patron_id > 0 {
            // Renewal caller does not always pass patron data.
            query["usr"] = json::from(self.patron_id);
        }

        let flesh = json::object! {
            "flesh": 2,
            "flesh_fields": {
                "circ": ["usr"],
                "au": ["card"],
            }
        };

        let mut circ = self
            .editor()
            .search_with_ops("circ", query, flesh)?
            .pop()
            .ok_or_else(|| self.editor().die_event())?;

        let circ_id = json_int(&circ["id"])?;
        let patron = circ["usr"].take(); // fleshed
        self.patron_id = json_int(&patron["id"])?;
        self.patron = Some(patron);

        // Replace the usr value which was null-ified above w/ take()
        circ["usr"] = json::from(self.patron_id);

        self.parent_circ = Some(circ_id);
        self.circ = Some(circ);

        Ok(())
    }

    /// Check various perms, policies, limits before proceeding with
    /// checkin+checkout.
    fn basic_renewal_checks(&mut self) -> EgResult<()> {
        let circ = self.circ.as_ref().unwrap();
        let patron = self.patron.as_ref().unwrap();

        let orig_circ_lib = json_int(&circ["circ_lib"])?;

        let renewal_remaining = json_int(&circ["renewal_remaining"])?;
        // NULL-able
        let auto_renewal_remaining = json_int(&circ["auto_renewal_remaining"]);

        let expire_date = patron["expire_date"].as_str().unwrap(); // required
        let expire_dt = date::parse_datetime(&expire_date)?;

        let circ_lib = self.set_renewal_circ_lib(orig_circ_lib)?;

        if self.patron_id != self.requestor_id() {
            if !self.editor().allowed_at("RENEW_CIRC", circ_lib)? {
                return Err(self.editor().die_event());
            }
        }

        if renewal_remaining < 1 {
            self.exit_err_on_event_code("MAX_RENEWALS_REACHED")?;
        }

        self.renewal_remaining = renewal_remaining - 1;

        // NULL-able field.
        if let Ok(n) = auto_renewal_remaining {
            if n < 1 {
                self.exit_err_on_event_code("MAX_RENEWALS_REACHED")?;
            }
            self.auto_renewal_remaining = Some(n - 1);
        }

        // See if it's OK to renew items for expired patron accounts.
        if expire_dt < date::now() {
            let allow = self.settings.get_value("circ.renew.expired_patron_allow")?;
            if !json_bool(allow) {
                self.exit_err_on_event_code("PATRON_ACCOUNT_EXPIRED")?;
            }
        }

        let copy_id = self.copy_id;
        let block_for_holds = json_bool(self.settings.get_value("circ.block_renews_for_holds")?);

        if block_for_holds {
            let holds = holds::find_nearest_permitted_hold(self.editor(), copy_id, true)?;
            if holds.is_some() {
                self.add_event(EgEvent::new("COPY_NEEDED_FOR_HOLD"));
            }
        }

        Ok(())
    }

    fn set_renewal_circ_lib(&mut self, orig_circ_lib: i64) -> EgResult<i64> {
        let is_opac = json_bool_op(self.options.get("opac_renewal"));
        let is_auto = json_bool_op(self.options.get("auto_renewal"));
        let is_desk = json_bool_op(self.options.get("desk_renewal"));

        if is_opac || is_auto {
            if let Some(setting) = self
                .editor()
                .retrieve("cgf", "circ.opac_renewal.use_original_circ_lib")?
                .take()
            {
                if json_bool(&setting["enabled"]) {
                    self.circ_lib = orig_circ_lib;
                    self.settings.set_org_id(orig_circ_lib);
                    return Ok(orig_circ_lib);
                }
            }
        }

        if is_desk {
            if let Some(setting) = self
                .editor()
                .retrieve("cgf", "circ.desk_renewal.use_original_circ_lib")?
                .take()
            {
                if json_bool(&setting["enabled"]) {
                    self.circ_lib = orig_circ_lib;
                    self.settings.set_org_id(orig_circ_lib);
                    return Ok(orig_circ_lib);
                }
            }
        }

        // Shouldn't get here, but maybe possible.
        Ok(self.circ_lib)
    }
}
