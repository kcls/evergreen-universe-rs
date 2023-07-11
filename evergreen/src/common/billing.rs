use crate::common::org;
use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;


pub fn void_bills(
    editor: &mut Editor,
    billing_ids: &[i64], // money.billing.id
    note: Option<&str>
) -> Result<(), String> {

    let bills = editor.search("mb", json::object!{"id": billing_ids})?;

    for bill in bills {

        if util::json_bool(&bill["voided"]) {
            log::debug!("Billing {} already voided.  Skipping", bill["id"]);
            continue;
        }


        let xact = editor.retrieve("mbt", bill["xact"].clone())?;
        let xact = match xact {
            Some(x) => x,
            None => return editor.die_event(),
        };

        let xact_org = xact_org(editor, util::json_int(&xact["id"])?)?;
    }


    Ok(())
}

/// Given a transaction ID, return the context org_unit for the transaction.
pub fn xact_org(editor: &mut Editor, xact_id: i64) -> Result<i64, String> {
    // Is it a circulation?
    let query = json::object!{
        "select": {"circ": ["circ_lib"]},
        "from": "circ",
        "where": {"id": xact_id}
    };

    let hashlist = editor.json_query(query)?;

    if let Some(hash) = hashlist.first() {
        return Ok(util::json_int(&hash["circ_lib"])?);
    }

    // Is it a reservation?
    let query = json::object!{
        "select": {"bresv": ["request_lib"]},
        "from": "bresv",
        "where": {"id": xact_id},
    };

    let hashlist = editor.json_query(query)?;
    if let Some(hash) = hashlist.first() {
        return Ok(util::json_int(&hash["request_lib"])?);
    }

    // Guess it's just a misc. billing.
    let query = json::object!{
        "select": {"mg": ["billing_location"]},
        "from": "mg",
        "where": {"id": xact_id},
    };

    let hashlist = editor.json_query(query)?;
    if let Some(hash) = hashlist.first() {
        return Ok(util::json_int(&hash["billing_location"])?);
    }

    Err(format!("No Such Transaction: {xact_id}"))
}


