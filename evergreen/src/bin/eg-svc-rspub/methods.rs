use eg::apputil;
use eg::editor::Editor;
use eg::settings::Settings;
use evergreen as eg;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::session::ServerSession;
use std::collections::HashMap;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[
    StaticMethod {
        name: "get_barcodes",
        desc: "Find matching barcodes by type",
        param_count: ParamCount::Exactly(4),
        handler: get_barcodes,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "Context",
                datatype: ParamDataType::String,
                desc: "Options: actor, asset, serial, or booking",
            },
            StaticParam {
                required: true,
                name: "Barcode",
                datatype: ParamDataType::String,
                desc: "Whole barcode or a partial 'completable' barcode",
            },
        ],
    },
    StaticMethod {
        name: "user_has_work_perm_at.batch",
        desc: "Find org units where the provided user has the requested permissions",
        param_count: ParamCount::Range(2, 3),
        handler: user_has_work_perm_at_batch,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "Authtoken",
            },
            StaticParam {
                required: true,
                name: "Permissions",
                datatype: ParamDataType::Array,
                desc: "List of permission codes",
            },
            StaticParam {
                required: false,
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to check permissions for; defaults to the API requestor",
            },
        ],
    },
    StaticMethod {
        name: "ou_setting.ancestor_default.batch",
        desc: "Get org unit setting values",
        param_count: ParamCount::Range(2, 3),
        handler: ou_setting_ancestor_default_batch,
        params: &[
            StaticParam {
                required: true,
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "Settings",
                datatype: ParamDataType::Array,
                desc: "List of setting names",
            },
            StaticParam {
                required: false,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "Authtoken.  Required for perm-protected settings",
            },
        ],
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
pub fn user_has_work_perm_at_batch(
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

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

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
            apputil::user_has_work_perm_at(&mut editor, user_id, &perm)?,
        );
    }

    session.respond(map)
}

pub fn ou_setting_ancestor_default_batch(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let org_id = eg::util::json_int(method.param(0))?;

    let perms = match method.param(1) {
        json::JsonValue::Array(v) => v,
        _ => Err(format!("Invalid value for 'perms' parameter"))?,
    };

    let mut editor = Editor::new(worker.client(), worker.env().idl());

    if let Some(token) = method.param(2).as_str() {
        // Authtoken is only required for perm-lmited org settings.
        // If it's provided, though, we gotta check it.
        editor.set_authtoken(token);
        if !editor.checkauth()? {
            return session.respond(editor.event());
        }
    }

    let mut settings = Settings::new(&editor);

    // Since this API specifically wants org unit settings and the user
    // provides some of the required context data, clear the workstation
    // ID in case we picked on up from the authtoken / editor and apply
    // the requested org id.
    settings.set_workstation_id(0);
    settings.set_org_id(org_id);

    for perm in perms.iter() {
        if let Some(name) = perm.as_str() {
            let mut obj = json::JsonValue::new_object();
            obj[name] = settings.get_value(name)?.clone();
            session.respond(obj)?;
        }
    }

    Ok(())
}
