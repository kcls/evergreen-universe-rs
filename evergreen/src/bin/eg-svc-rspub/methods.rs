use evergreen as eg;
use eg::editor::Editor;
use opensrf::app::{ApplicationWorker};
use opensrf::message;
use opensrf::session::ServerSession;

// Import our app module
use crate::app;

pub fn get_barcodes(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;

    let authtoken = worker.authtoken(&method)?;

    let org_id = eg::util::json_int(method.params().get(1).unwrap())?;

    let context = method
        .params()
        .get(2)
        .unwrap()
        .as_str()
        .ok_or(format!("Context parameter must be a string"))?;

    let barcode = method
        .params()
        .get(3)
        .unwrap()
        .as_str()
        .ok_or(format!("Barcode parameter must be a string"))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.last_event().unwrap().to_json_value());
    }

    if !editor.allowed("STAFF_LOGIN", Some(org_id))? {
        return session.respond(editor.last_event().unwrap().to_json_value());
    }

    let query = json::object! {
        from: [
            "evergreen.get_barcodes",
            org_id, context, barcode
        ]
    };

    let result = editor.json_query(query)?;

    if context.ne("actor") {
        return session.respond(result);
    }

    let requestor_id = editor.requestor_id();
    let mut response: Vec<json::JsonValue> = Vec::new();

    for user_row in result {
        let user_id = eg::util::json_int(&user_row["id"])?;

        if user_id == requestor_id {
            // We're allowed to know about ourselves.
            response.push(user_row);
            continue;
        }

        // If the found user account is not "me", verify we
        // have permission to view said account.
        let u = editor.retrieve("au", user_id)?.unwrap();
        let home_ou = eg::util::json_int(&u["home_ou"])?;

        if editor.allowed("VIEW_USER", Some(home_ou))? {
            response.push(user_row);
        } else {
            response.push(editor.last_event().unwrap().into());
        }
    }

    session.respond(response)
}
