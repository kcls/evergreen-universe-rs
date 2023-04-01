use std::collections::HashMap;
use eg::editor::Editor;
use evergreen as eg;
use eg::apputil;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{MethodDef, ParamCount};
use opensrf::session::ServerSession;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static STATIC_METHODS: &[MethodDef] = &[
    MethodDef {
        name: "get_barcodes",
        param_count: ParamCount::Exactly(4),
        handler: get_barcodes,
    },
    MethodDef {
        name: "user_has_work_perm_at",
        param_count: ParamCount::Range(2, 3),
        handler: user_has_work_perm_at,
    },
];

pub fn get_barcodes(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    // Cast our worker instance into something we know how to use.
    let worker = app::RsPubWorker::downcast(worker)?;

    let authtoken = eg::util::json_string(method.param(0))?;
    let org_id = eg::util::json_int(method.param(1))?;
    let context = eg::util::json_string(method.param(2))?;
    let barcode = eg::util::json_string(method.param(3))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    if !editor.allowed("STAFF_LOGIN", Some(org_id))? {
        return session.respond(editor.event());
    }

    let query = json::object! {
        from: [
            "evergreen.get_barcodes",
            org_id, context.as_str(), barcode.as_str()
        ]
    };

    let result = editor.json_query(query)?;

    if context.ne("actor") {
        // Perm checks not needed for asset/serial/booking items.
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

        // Do we have permission to view info about this user?
        let u = editor.retrieve("au", user_id)?.unwrap();
        let home_ou = eg::util::json_int(&u["home_ou"])?;

        if editor.allowed("VIEW_USER", Some(home_ou))? {
            response.push(user_row);
        } else {
            response.push(editor.event());
        }
    }

    session.respond(response)
}

/// Returns a map of permission name to a list of org units where the
/// provided user (or the caller, if no user is specified) has each
/// of the provided permissions.
pub fn user_has_work_perm_at(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    // Cast our worker instance into something we know how to use.
    let worker = app::RsPubWorker::downcast(worker)?;

    let authtoken = eg::util::json_string(method.param(0))?;

    let perms = match method.param(1) {
        json::JsonValue::Array(v) => v,
        _ => Err(format!("Invalid value for 'perms' parameter"))?,
    };

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    // user_id parameter is optional
    let user_id = match method.params().get(2) {
        Some(id) => eg::util::json_int(id)?,
        None => editor.requestor_id(),
    };

    let mut map: HashMap<String, Vec<i64>> = HashMap::new();
    for perm in perms.iter() {
        let perm = eg::util::json_string(perm)?;
        map.insert(
            perm.to_string(),
            apputil::user_has_work_perm_at(&mut editor, user_id, &perm)?
        );
    }

    session.respond(map)
}

