//! Base module for A/T Reactors
use crate as eg;
use eg::common::auth;
use eg::common::{trigger, trigger::Event, trigger::Processor};
use eg::EgEvent;
use eg::EgResult;
use eg::EgValue;

impl Processor<'_> {
    pub fn autorenew(&mut self, events: &mut [&mut Event]) -> EgResult<()> {
        let usr = &events[0].target()["usr"];
        // "usr" is either the id itself or a user object with an ID.
        let patron_id = usr.as_int().unwrap_or(usr.id()?);

        let home_ou = if usr.is_object() {
            usr["home_ou"].as_int().unwrap_or(usr["home_ou"].id()?)
        } else {
            // Fetch the patron so we can determine the home or unit
            let patron = self
                .editor
                .retrieve("au", patron_id)?
                .ok_or_else(|| self.editor.die_event())?;

            patron["home_ou"].int()?
        };

        let mut auth_args = auth::InternalLoginArgs::new(patron_id, auth::LoginType::Opac);
        auth_args.set_org_unit(home_ou);

        // TODO move to internal_session() / add Trigger worker cache.
        let auth_ses = auth::Session::internal_session_api(self.editor.client_mut(), &auth_args)?
            .ok_or_else(|| "Cannot create internal auth session".to_string())?;

        for event in events {
            self.renew_one_circ(auth_ses.token(), patron_id, event)?;
        }

        Ok(())
    }

    fn renew_one_circ(&mut self, authtoken: &str, patron_id: i64, event: &Event) -> EgResult<()> {
        let tc = &event.target()["target_copy"];
        let copy_id = tc.as_int().unwrap_or(tc.id()?);

        log::info!(
            "Auto-Renewing Circ id={} copy={copy_id}",
            event.target()["id"]
        );

        let params = vec![
            EgValue::from(authtoken),
            eg::hash! {
                "patron_id": patron_id,
                "copy_id": copy_id,
                "auto_renewal": true
            },
        ];

        log::info!("{self} renewing with params: {params:?}");

        let mut response = self
            .editor
            .client_mut()
            .send_recv_one("open-ils.circ", "open-ils.circ.renew", params)?
            .ok_or_else(|| "Renewal returned no response".to_string())?;

        // API may return an EgEvent or a list of them.  We're only
        // interested in the first event.
        let evt = if response.is_array() {
            response.array_remove(0)
        } else {
            response
        };

        let eg_evt = EgEvent::parse(&evt)
            .ok_or_else(|| format!("Renew returned unexpected data: {}", evt.dump()))?;

        log::info!("{self} autorenewal returned {eg_evt}");

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
            total_remaining = new_circ["renewal_remaining"].int()?;

            // nullable / maybe a string
            auto_remaining = new_circ["auto_renewal_remaining"].as_int().unwrap_or_default();
        } else {
            old_due_date = source_circ["due_date"].as_str().unwrap(); // required
            total_remaining = source_circ["renewal_remaining"].int()?;
            fail_reason = eg_evt.desc().unwrap_or("");

            // nullable / maybe a string
            auto_remaining = source_circ["auto_renewal_remaining"].as_int().unwrap_or_default();
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

        let user_data = eg::hash! {
            "copy": copy_id,
            "is_renewed": success,
            "reason": fail_reason,
            "new_due_date": new_due_date,
            "old_due_date": old_due_date,
            "textcode": eg_evt.textcode(),
            "total_renewal_remaining": total_remaining,
            "auto_renewal_remaining": auto_remaining,
        };

        let target = &event.target()["circ_lib"];
        let circ_lib = target.as_int().unwrap_or(target.id()?);

        // Create the event from the source circ instead of the new
        // circ, since the renewal may have failed.  Fire and do not
        // forget so we don't flood A/T.
        trigger::create_events_for_object(
            self.editor,
            "autorenewal",
            event.target(),
            circ_lib,
            None,
            Some(&user_data),
            false,
        )
    }
}
