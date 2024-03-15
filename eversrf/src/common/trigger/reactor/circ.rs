//! Base module for A/T Reactors
use crate::auth::{AuthInternalLoginArgs, AuthSession};
use crate::common::{trigger, trigger::Event, trigger::Processor};
use crate::event::EgEvent;
use crate::result::EgResult;
use crate::util::json_int;
use json;

impl Processor<'_> {
    pub fn autorenew(&mut self, events: &mut [&mut Event]) -> EgResult<()> {
        let usr = &events[0].target()["usr"];
        // "usr" is either the id itself or a user object with an ID.
        let patron_id = usr.as_int().unwrap_or(usr.id_required());

        let home_ou = if usr.is_object() {
            usr["home_ou"].as_int().unwrap_or(usr["home_ou"].id_required())
        } else {
            // Fetch the patron so we can determine the home or unit
            let patron = self
                .editor
                .retrieve("au", patron_id)?
                .ok_or_else(|| self.editor.die_event())?;

            json_int(&patron["home_ou"])?
        };

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
        let tc = &event.target()["target_copy"];
        let copy_id = tc.as_int().unwrap_or(tc.id_required());

        log::info!(
            "Auto-Renewing Circ id={} copy={copy_id}",
            event.target()["id"]
        );

        let params = vec![
            EgValue::from(authtoken),
            eg::hash! {
                "patron_id": patron_id,
                "copy_id": copy_id.clone(),
                "auto_renewal": true
            },
        ];

        log::info!("{self} renewing with params: {params:?}");

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
            total_remaining = json_int(&new_circ["renewal_remaining"])?; // required

            // nullable / maybe a string
            auto_remaining = if let Ok(r) = json_int(&new_circ["auto_renewal_remaining"]) {
                r
            } else {
                0
            };
        } else {
            old_due_date = source_circ["due_date"].as_str().unwrap(); // required
            total_remaining = json_int(&source_circ["renewal_remaining"])?;
            fail_reason = eg_evt.desc().unwrap_or("");

            // nullable / maybe a string
            auto_remaining = if let Ok(r) = json_int(&source_circ["auto_renewal_remaining"]) {
                r
            } else {
                0
            };
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
        let circ_lib = target.as_int().unwrap_or(target.id_required());

        // Create the event from the source circ instead of the new
        // circ, since the renewal may have failed.  Fire and do not
        // forget so we don't flood A/T.
        trigger::create_events_for_object(
            &mut self.editor,
            "autorenewal",
            event.target(),
            json_int(&circ_lib)?,
            None,
            Some(&user_data),
            false,
        )
    }
}
