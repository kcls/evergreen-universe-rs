//! Base module for A/T Validators
use crate as eg;
use eg::common::holdings;
use eg::common::trigger::{Event, EventState, Processor};
use eg::constants as C;
use eg::date;
use eg::EgResult;

/// Add validation routines to the Processor.
impl Processor<'_> {
    /// Validate an event.
    ///
    /// TODO stacked validators.
    ///
    /// Loading modules dynamically is not as simple in Rust as in Perl.
    /// Hard-code a module-mapping instead. (*shrug* They all require
    /// code changes).
    pub fn validate(&mut self, event: &mut Event) -> EgResult<bool> {
        log::info!("{self} validating {event}");

        self.set_event_state(event, EventState::Validating)?;

        let validator = self.validator();

        let validate_result = match validator {
            "NOOP_True" => Ok(true),
            "NOOP_False" => Ok(false),
            "CircIsOpen" => self.circ_is_open(event),
            "CircIsOverdue" => self.circ_is_overdue(event),
            "HoldIsAvailable" => self.hold_is_available(event),
            "HoldIsCancelled" => self.hold_is_canceled(event),
            "HoldNotifyCheck" => self.hold_notify_check(event),
            "MinPassiveTargetAge" => self.min_passive_target_age(event),
            "PatronBarred" => self.patron_is_barred(event),
            "PatronNotBarred" => self.patron_is_barred(event).map(|val| !val),
            "ReservationIsAvailable" => self.reservation_is_available(event),
            _ => Err(format!("No such validator: {validator}").into()),
        };

        if let Ok(valid) = validate_result {
            if valid {
                self.set_event_state(event, EventState::Validating)?;
            } else {
                self.set_event_state(event, EventState::Invalid)?;
            }
        }

        validate_result
    }

    /// True if the target circulation is still open.
    fn circ_is_open(&mut self, event: &Event) -> EgResult<bool> {
        if event.target()["checkin_time"].is_string() {
            return Ok(false);
        }

        if event.target()["xact_finish"].is_string() {
            return Ok(false);
        }

        if self.param_value("min_target_age").is_some() {
            if let Some(fname) = self.param_value_as_str("target_age_field") {
                if fname == "xact_start" {
                    return self.min_passive_target_age(event);
                }
            }
        }

        Ok(true)
    }

    fn min_passive_target_age(&mut self, event: &Event) -> EgResult<bool> {
        let min_target_age = self
            .param_value_as_str("min_target_age")
            .ok_or_else(|| format!("'min_target_age' parameter required for MinPassiveTargetAge"))?
            .to_string();

        let age_field = self.param_value_as_str("target_age_field").ok_or_else(|| {
            format!("'target_age_field' parameter or delay_field required for MinPassiveTargetAge")
        })?;

        let age_field_val = &event.target()[age_field];
        let age_date_str = age_field_val.as_str().ok_or_else(|| {
            format!(
                "MinPassiveTargetAge age field {age_field} has unexpected value: {}",
                age_field_val.dump()
            )
        })?;

        let age_field_ts =
            date::add_interval(date::parse_datetime(age_date_str)?, &min_target_age)?;

        Ok(age_field_ts <= date::now())
    }

    fn circ_is_overdue(&mut self, event: &Event) -> EgResult<bool> {
        if event.target()["checkin_time"].is_string() {
            return Ok(false);
        }

        if let Some(stop_fines) = event.target()["stop_fines"].as_str() {
            if stop_fines == "MAXFINES" || stop_fines == "LONGOVERDUE" {
                return Ok(false);
            }
        }

        if self.param_value("min_target_age").is_some() {
            if let Some(fname) = self.param_value_as_str("target_age_field") {
                if fname == "xact_start" {
                    return self.min_passive_target_age(event);
                }
            }
        }

        // due_date is a required string field.
        let due_date = event.target()["due_date"].as_str().unwrap();
        let due_date_ts = date::parse_datetime(due_date)?;

        Ok(due_date_ts < date::now())
    }

    /// True if the hold is ready for pickup.
    fn hold_is_available(&mut self, event: &Event) -> EgResult<bool> {
        if !self.hold_notify_check(event)? {
            return Ok(false);
        }

        let hold = event.target();

        // Start with some simple tests.
        let canceled = hold["cancel_time"].is_string();
        let fulfilled = hold["fulfillment_time"].is_string();
        let captured = hold["capture_time"].is_string();
        let shelved = hold["shelf_time"].is_string();

        if canceled || fulfilled || !captured || !shelved {
            return Ok(false);
        }

        // Verify shelf lib matches pickup lib -- it's not sitting on
        // the wrong shelf somewhere.
        //
        // Accommodate fleshing
        let shelf_lib = match hold["current_shelf_lib"].as_i64() {
            Some(id) => id,
            None => match hold["current_shelf_lib"]["id"].as_i64() {
                Some(id) => id,
                None => return Ok(false),
            },
        };

        let pickup_lib = hold["pickup_lib"]
            .as_int()
            .unwrap_or(hold["pickup_lib"].id()?);

        if shelf_lib != pickup_lib {
            return Ok(false);
        }

        // Verify we have a targted copy and it has the expected status.
        let copy_status = if let Some(copy_id) = hold["current_copy"].as_i64() {
            holdings::copy_status(&mut self.editor, Some(copy_id), None)?
        } else if hold["current_copy"].is_object() {
            holdings::copy_status(&mut self.editor, None, Some(&hold["current_copy"]))?
        } else {
            -1
        };

        Ok(copy_status == C::COPY_STATUS_ON_HOLDS_SHELF)
    }

    fn hold_is_canceled(&mut self, event: &Event) -> EgResult<bool> {
        if self.hold_notify_check(event)? {
            Ok(event.target()["cancel_time"].is_string())
        } else {
            Ok(false)
        }
    }

    /// Returns false if a notification parameter is present and the
    /// hold in question is inconsistent with the parameter.
    ///
    /// In general, if this test fails, the event should not proceed
    /// to reacting.
    ///
    /// Assumes the hold in question == the event.target().
    fn hold_notify_check(&mut self, event: &Event) -> EgResult<bool> {
        let hold = event.target();

        if self.param_value_as_bool("check_email_notify") && !hold["email_notify"].boolish() {
            return Ok(false);
        }

        if self.param_value_as_bool("check_sms_notify") && !hold["sms_notify"].boolish() {
            return Ok(false);
        }

        if self.param_value_as_bool("check_phone_notify") && !hold["phone_notify"].boolish() {
            return Ok(false);
        }

        Ok(true)
    }

    fn reservation_is_available(&mut self, event: &Event) -> EgResult<bool> {
        let res = event.target();
        Ok(res["cancel_time"].is_null()
            && !res["capture_time"].is_null()
            && !res["current_resource"].is_null())
    }

    fn patron_is_barred(&mut self, event: &Event) -> EgResult<bool> {
        Ok(event.target()["barred"].boolish())
    }

    // Perl has CircIsAutoRenewable but it oddly creates the same
    // events (hook 'autorenewal') that the autorenewal reactor creates,
    // and it's not used in the default A/T definitions.  Guessing that
    // validator should be removed from the Perl.

    // TODO PatronNotInCollections
}
