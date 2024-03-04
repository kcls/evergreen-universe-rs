//! Base module for A/T Reactors
use crate::auth::{AuthInternalLoginArgs, AuthSession};
use crate::common::{trigger, trigger::Event, trigger::Processor};
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util;
use json;

impl Processor<'_> {
    pub fn autorenew(&mut self, events: &mut [&mut Event]) -> EgResult<()> {
        let patron_id = util::json_int(&events[0].target()["usr"])?;

        let patron = self
            .editor
            .retrieve("au", patron_id)?
            .ok_or_else(|| self.editor.die_event())?;

        let home_ou = util::json_int(&patron["home_ou"])?;

        let mut auth_args = AuthInternalLoginArgs::new(patron_id, "opac");
        auth_args.set_org_unit(home_ou);

        let auth_ses = AuthSession::internal_session(self.editor.client_mut(), &auth_args)?
            .ok_or_else(|| format!("Cannot create internal auth session"))?;

        for event in events {
            self.renew_one_circ(auth_ses.token(), patron_id, event)?;
        }

        Ok(())
    }

    fn renew_one_circ(&mut self, authtoken: &str, patron_id: i64, event: &Event) -> EgResult<()> {
        let copy_id = &event.target()["target_copy"];

        log::info!(
            "Auto-Renewing Circ id={} copy={copy_id}",
            event.target()["id"]
        );

        let params = vec![
            json::from(authtoken),
            json::object! {
                "patron_id": patron_id,
                "copy_id": copy_id.clone(),
                "auto_renewal": true
            },
        ];

        let mut response = self
            .editor
            .client_mut()
            .send_recv_one("open-ils.circ", "open-ils.circ.renew", params)?
            .ok_or_else(|| format!("Renewal returned no response"))?;

        // API may return an EgEvent or a list of them.  We're only
        // interested in the first event.
        let evt = if response.is_array() {
            response.array_remove(0)
        } else {
            response
        };

        let eg_evt = EgEvent::parse(&evt)
            .ok_or_else(|| format!("Renew returned unexpected data: {}", evt.dump()))?;

        let source_circ = event.target();
        let new_circ = &eg_evt.payload()["circ"];

        let mut new_due_date = "";
        let mut old_due_date = "";
        let mut fail_reason = "";
        let mut total_remaining;
        let mut auto_remaining;

        let success = eg_evt.is_success();
        if success && new_circ.is_object() {
            new_due_date = new_circ["due_date"].as_str().unwrap(); // required
            total_remaining = util::json_int(&new_circ["renewal_remaining"])?;
            auto_remaining = util::json_int(&new_circ["auto_renewal_remaining"])?;
        } else {
            old_due_date = source_circ["due_date"].as_str().unwrap(); // required
            total_remaining = util::json_int(&source_circ["renewal_remaining"])?;
            auto_remaining = util::json_int(&source_circ["auto_renewal_remaining"])?;
            fail_reason = eg_evt.desc().unwrap_or("");
        }

        if total_remaining < 0 {
            total_remaining = 0;
        }
        if auto_remaining < 0 {
            auto_remaining = 0;
        }
        if auto_remaining < total_remaining {
            auto_remaining = total_remaining;
        }

        let user_data = json::object! {
            "copy": copy_id.clone(),
            "is_renewed": success,
            "reason": fail_reason,
            "new_due_date": new_due_date,
            "old_due_date": old_due_date,
            "textcode": eg_evt.textcode(),
            "total_renewal_remaining": total_remaining,
            "auto_renewal_remaining": auto_remaining,
        };

        // Create the event from the source circ instead of the new
        // circ, since the renewal may have failed.  Fire and do not
        // forget so we don't flood A/T.
        trigger::create_events_for_object(
            &mut self.editor,
            "autorenewal",
            event.target(),
            util::json_int(&event.target()["circ_lib"])?,
            None,
            Some(&user_data),
            false,
        )
    }
}
