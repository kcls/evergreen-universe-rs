use eg::common::circ;
use eg::common::penalty;
use eg::common::settings::Settings;
use eg::common::user;
use eg::editor::Editor;
use eg::util;
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
    StaticMethod {
        name: "settings.retrieve",
        desc: "Get workstation/user/org unit setting values",
        param_count: ParamCount::Range(1, 3),
        handler: retrieve_cascade_settigs,
        params: &[
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
                desc: "Authtoken.  Required for workstation, user, and perm-protected settings",
            },
            StaticParam {
                required: true,
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
        ],
    },
    StaticMethod {
        name: "user.opac.vital_stats",
        desc: "Key patron counts and info",
        param_count: ParamCount::Range(1, 2),
        handler: user_opac_vital_stats,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: false,
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID whose stats to load; defaults to requestor",
            },
        ],
    },
    StaticMethod {
        name: "renewal_chain.retrieve_by_circ.summary",
        desc: "Circulation Renewal Chain Summary",
        param_count: ParamCount::Exactly(2),
        handler: renewal_chain_summary,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "Circ ID",
                datatype: ParamDataType::Number,
                desc: "Circulation ID to lookup",
            },
        ],
    },
    StaticMethod {
        name: "prev_renewal_chain.retrieve_by_circ.summary",
        desc: "Previous Circulation Renewal Chain Summary",
        param_count: ParamCount::Exactly(2),
        handler: prev_renewal_chain_summary,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "Circ ID",
                datatype: ParamDataType::Number,
                desc: "Circulation ID to lookup",
            },
        ],
    },
    StaticMethod {
        name: "user.penalties.update",
        desc: "Update User Penalties",
        param_count: ParamCount::Range(2, 3),
        handler: update_penalties,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to Update",
            },
            StaticParam {
                required: false,
                name: "Only Penalties",
                datatype: ParamDataType::Array,
                desc: "Optionally limit to this list of penalties.
                    May be a list of strings (names) or numbers (IDs)",
            },
        ],
    },
    StaticMethod {
        name: "user.penalties.update_at_home",
        desc: "Update User Penalties using Staff Context Org Unit",
        param_count: ParamCount::Range(2, 3),
        handler: update_penalties,
        params: &[
            StaticParam {
                required: true,
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                required: true,
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to Update",
            },
            StaticParam {
                required: false,
                name: "Only Penalties",
                datatype: ParamDataType::Array,
                desc: "Optionally limit to this list of penalties.
                    May be a list of strings (names) or numbers (IDs)",
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

    // Extract the method call parameters.
    // Incorrectly shaped parameters will result in an error
    // response to the caller.
    let authtoken = util::json_string(method.param(0))?;
    let org_id = util::json_int(method.param(1))?;
    let context = util::json_string(method.param(2))?;
    let barcode = util::json_string(method.param(3))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    // Auth check
    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    // Perm check
    if !editor.allowed("STAFF_LOGIN", Some(org_id))? {
        return session.respond(editor.event());
    }

    // Inline JSON object construction
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

    // "actor" barcodes require additional perm checks.
    for user_row in result {
        let user_id = util::json_int(&user_row["id"])?;

        if user_id == requestor_id {
            // We're allowed to know about ourselves.
            response.push(user_row);
            continue;
        }

        // Do we have permission to view info about this user?
        let u = editor.retrieve("au", user_id)?.unwrap();
        let home_ou = util::json_int(&u["home_ou"])?;

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

    let authtoken = util::json_string(method.param(0))?;

    let perm_names: Vec<&str> = method
        .param(1)
        .members() // json array iterator
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if perm_names.len() == 0 {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client(), worker.env().idl());

    if !editor.apply_authtoken(&authtoken)? {
        return session.respond(editor.event());
    }

    // user_id parameter is optional
    let user_id = match method.params().get(2) {
        Some(id) => util::json_int(id)?,
        None => editor.requestor_id(),
    };

    let mut map: HashMap<String, Vec<i64>> = HashMap::new();
    for perm in perm_names {
        map.insert(
            perm.to_string(),
            user::has_work_perm_at(&mut editor, user_id, &perm)?,
        );
    }

    session.respond(map)
}

pub fn retrieve_cascade_settigs(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;

    let setting_names: Vec<&str> = method
        .param(0)
        .members() // json array iterator
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if setting_names.len() == 0 {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client(), worker.env().idl());

    // Authtoken is optional.  If set, verify it's valid and absorb
    // it into our editor so its context info can be picked up by
    // our Settings instance.
    if let Some(token) = method.param(1).as_str() {
        if !editor.apply_authtoken(token)? {
            return session.respond(editor.event());
        }
    }

    let mut settings = Settings::new(&editor);

    // If the caller requests values for a specific org unit, that
    // supersedes the org unit potentially linked to the workstation.
    if let Some(org_id) = method.param(2).as_i64() {
        settings.set_org_id(org_id);
    }

    // Pre-cache the settings en masse, then pull each from the settings
    // cache and return to the caller.
    settings.fetch_values(setting_names.as_slice())?;

    for name in setting_names {
        let mut obj = json::JsonValue::new_object();
        obj[name] = settings.get_value(name)?.clone();
        session.respond(obj)?;
    }

    Ok(())
}

pub fn ou_setting_ancestor_default_batch(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let org_id = util::json_int(method.param(0))?;

    let setting_names: Vec<&str> = method
        .param(1)
        .members() // iterate json array
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if setting_names.len() == 0 {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client(), worker.env().idl());
    let mut settings = Settings::new(&editor);

    settings.set_org_id(org_id);

    // If available, apply the authtoken to the editor after we apply
    // the org id to the Settings instance (above).  Otherwise, the
    // workstation org unit could supersede the requested org id.
    if let Some(token) = method.param(2).as_str() {
        // Authtoken is only required for perm-lmited org settings.
        // If it's provided, though, we gotta check it.
        if !editor.apply_authtoken(token)? {
            return session.respond(editor.event());
        }
        settings.set_user_id(editor.requestor_id());
    }

    // Pre-cache the settings en masse, then pull each from the settings
    // cache and return to the caller.
    settings.fetch_values(setting_names.as_slice())?;

    for name in setting_names {
        let mut obj = json::JsonValue::new_object();
        obj[name] = settings.get_value(name)?.clone();
        session.respond(obj)?;
    }

    Ok(())
}

pub fn user_opac_vital_stats(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    let user_id = method.param(1).as_i64().unwrap_or(editor.requestor_id());
    let mut user = match editor.retrieve("au", user_id)? {
        Some(u) => u,
        None => return session.respond(editor.event()),
    };

    if user_id != editor.requestor_id() {
        let home_ou = Some(util::json_int(&user["home_ou"])?);

        // This list of perms seems like overkill for summary data, but
        // it matches the perm checks of the existing open-ils.actor APIs.
        if !editor.allowed("VIEW_USER", home_ou)?
            || !editor.allowed("VIEW_USER_FINES_SUMMARY", home_ou)?
            || !editor.allowed("VIEW_CIRCULATIONS", home_ou)?
            || !editor.allowed("VIEW_HOLD", home_ou)?
        {
            return session.respond(editor.event());
        }
    }

    let holds = user::active_hold_counts(&mut editor, user_id)?;
    let fines = user::fines_summary(&mut editor, user_id)?;
    let checkouts = user::open_checkout_counts(&mut editor, user_id)?;

    let unread_query = json::object! {
        select: {aum: [{
            column: "id",
            transform: "count",
            aggregate: 1,
            alias: "count",
        }]},
        from: "aum",
        where: {
            usr: user_id,
            read_date: json::JsonValue::Null,
            deleted: "f",
            pub: "t",
        }
    };

    let mut unread_count = 0;
    if let Some(unread) = editor.json_query(unread_query)?.get(0) {
        unread_count = util::json_int(&unread["count"])?;
    }

    let resp = json::object! {
        fines: fines,
        holds: holds,
        checkouts: checkouts,
        messages: {unread: unread_count},
        user: {
            first_given_name: user["first_given_name"].take(),
            second_given_name: user["second_given_name"].take(),
            family_name: user["family_name"].take(),
            alias: user["alias"].take(),
            usrname: user["usrname"].take(),
        },
    };

    session.respond(resp)
}

pub fn renewal_chain_summary(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;
    let circ_id = util::json_int(method.param(1))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    if !editor.allowed("VIEW_CIRCULATIONS", None)? {
        return session.respond(editor.event());
    }

    let chain = circ::summarize_circ_chain(&mut editor, circ_id)?;

    session.respond(chain)
}

pub fn prev_renewal_chain_summary(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;
    let circ_id = util::json_int(method.param(1))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    if !editor.allowed("VIEW_CIRCULATIONS", None)? {
        return session.respond(editor.event());
    }

    let chain = circ::circ_chain(&mut editor, circ_id)?;
    let first_circ = &chain[0];

    // The previous circ chain contains the circ that occurred most recently
    // before the first circ in the latest circ chain.

    let query = json::object! {
        target_copy: util::json_int(&first_circ["target_copy"])?,
        xact_start: {"<": first_circ["xact_start"].as_str().unwrap()}, // xact_tart required
    };

    let flesh = json::object! {
        flesh: 1,
        flesh_fields: {
            aacs: [
                "active_circ",
                "aged_circ"
            ]
        },
        order_by: {aacs: "xact_start desc"},
        limit: 1
    };

    let prev_circ = editor.search_with_ops("aacs", query, flesh)?;

    if prev_circ.len() == 0 {
        // No previous circ chain
        return Ok(());
    }

    session.respond(circ::summarize_circ_chain(
        &mut editor,
        util::json_int(&prev_circ[0]["id"])?,
    )?)
}

pub fn update_penalties(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsPubWorker::downcast(worker)?;
    let authtoken = util::json_string(method.param(0))?;
    let user_id = util::json_int(method.param(1))?;

    let mut editor = Editor::with_auth(worker.client(), worker.env().idl(), &authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    let user = match editor.retrieve("au", user_id)? {
        Some(u) => u,
        None => return session.respond(editor.event()),
    };

    let mut context_org = util::json_int(&user["home_ou"])?;

    if !editor.allowed("UPDATE_USER", Some(context_org))? {
        return session.respond(editor.event());
    }

    if method.method().contains("_at_home") {
        context_org = editor.requestor_ws_ou();
    }

    let only_penalties = match method.params().get(2) {
        Some(op) => match op {
            json::JsonValue::Array(arr) => Some(arr),
            _ => None,
        },
        None => None,
    };

    editor.xact_begin()?;

    penalty::calculate_penalties(&mut editor, user_id, context_org, only_penalties)?;

    editor.commit()?;

    session.respond(1)
}
