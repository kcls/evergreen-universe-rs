use eg::common::penalty;
use eg::common::settings::Settings;
use eg::common::user;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::collections::HashMap;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "get_barcodes",
        desc: "Find matching barcodes by type",
        param_count: ParamCount::Exactly(4),
        handler: get_barcodes,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                name: "Context",
                datatype: ParamDataType::String,
                desc: "Options: actor, asset, serial, or booking",
            },
            StaticParam {
                name: "Barcode",
                datatype: ParamDataType::String,
                desc: "Whole barcode or a partial 'completable' barcode",
            },
        ],
    },
    StaticMethodDef {
        name: "user_has_work_perm_at.batch",
        desc: "Find org units where the provided user has the requested permissions",
        param_count: ParamCount::Range(2, 3),
        handler: user_has_work_perm_at_batch,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "Authtoken",
            },
            StaticParam {
                name: "Permissions",
                datatype: ParamDataType::Array,
                desc: "List of permission codes",
            },
            StaticParam {
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to check permissions for; defaults to the API requestor",
            },
        ],
    },
    StaticMethodDef {
        name: "ou_setting.ancestor_default.batch",
        desc: "Get org unit setting values",
        param_count: ParamCount::Range(2, 3),
        handler: ou_setting_ancestor_default_batch,
        params: &[
            StaticParam {
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
            StaticParam {
                name: "Settings",
                datatype: ParamDataType::Array,
                desc: "List of setting names",
            },
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "Authtoken.  Required for perm-protected settings",
            },
        ],
    },
    StaticMethodDef {
        name: "settings.retrieve",
        desc: "Get workstation/user/org unit setting values",
        param_count: ParamCount::Range(1, 3),
        handler: retrieve_cascade_settigs,
        params: &[
            StaticParam {
                name: "Settings",
                datatype: ParamDataType::Array,
                desc: "List of setting names",
            },
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "Authtoken.  Required for workstation, user, and perm-protected settings",
            },
            StaticParam {
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "",
            },
        ],
    },
    StaticMethodDef {
        name: "user.opac.vital_stats",
        desc: "Key patron counts and info",
        param_count: ParamCount::Range(1, 2),
        handler: user_opac_vital_stats,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID whose stats to load; defaults to requestor",
            },
        ],
    },
    StaticMethodDef {
        name: "user.penalties.update",
        desc: "Update User Penalties",
        param_count: ParamCount::Range(2, 3),
        handler: update_penalties,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to Update",
            },
            StaticParam {
                name: "Only Penalties",
                datatype: ParamDataType::Array,
                desc: "Optionally limit to this list of penalties.
                    May be a list of strings (names) or numbers (IDs)",
            },
        ],
    },
    StaticMethodDef {
        name: "user.penalties.update_at_home",
        desc: "Update User Penalties using Staff Context Org Unit",
        param_count: ParamCount::Range(2, 3),
        handler: update_penalties,
        params: &[
            StaticParam {
                name: "Authtoken",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "User ID",
                datatype: ParamDataType::Number,
                desc: "User ID to Update",
            },
            StaticParam {
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
    method: message::MethodCall,
) -> EgResult<()> {
    // Cast our worker instance into something we know how to use.
    let worker = app::RsActorWorker::downcast(worker)?;

    // Extract the method call parameters.
    // Incorrectly shaped parameters will result in an error
    // response to the caller.
    let authtoken = method.param(0).str()?;
    let org_id = method.param(1).int()?;
    let context = method.param(2).str()?;
    let barcode = method.param(3).str()?;

    let mut editor = Editor::with_auth(worker.client(), authtoken);

    // Auth check
    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    // Perm check
    if !editor.allowed_at("STAFF_LOGIN", org_id)? {
        return session.respond(editor.event());
    }

    // Inline JSON object construction
    let query = eg::hash! {
        from: [
            "evergreen.get_barcodes",
            org_id, context, barcode
        ]
    };

    let result = editor.json_query(query)?;

    if context.ne("actor") {
        // Perm checks not needed for asset/serial/booking items.
        return session.respond(result);
    }

    let requestor_id = editor.requestor_id()?;
    let mut response: Vec<EgValue> = Vec::new();

    // "actor" barcodes require additional perm checks.
    for user_row in result {
        let user_id = user_row.id()?;

        if user_id == requestor_id {
            // We're allowed to know about ourselves.
            response.push(user_row);
            continue;
        }

        // Do we have permission to view info about this user?
        let u = editor.retrieve("au", user_id)?.unwrap();
        let home_ou = u["home_ou"].int()?;

        if editor.allowed_at("VIEW_USER", home_ou)? {
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
    method: message::MethodCall,
) -> EgResult<()> {
    // Cast our worker instance into something we know how to use.
    let worker = app::RsActorWorker::downcast(worker)?;

    let authtoken = method.param(0).str()?;

    let perm_names: Vec<&str> = method
        .param(1)
        .members() // json array iterator
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if perm_names.is_empty() {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client());

    if !editor.apply_authtoken(authtoken)? {
        return session.respond(editor.event());
    }

    // user_id parameter is optional
    let user_id = match method.params().get(2) {
        Some(id) => id.int()?,
        None => editor.requestor_id()?,
    };

    let mut map: HashMap<String, Vec<i64>> = HashMap::new();
    for perm in perm_names {
        map.insert(
            perm.to_string(),
            user::has_work_perm_at(&mut editor, user_id, perm)?,
        );
    }

    session.respond(map)
}

pub fn retrieve_cascade_settigs(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsActorWorker::downcast(worker)?;

    let setting_names: Vec<&str> = method
        .param(0)
        .members() // json array iterator
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if setting_names.is_empty() {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client());

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
        let mut obj = EgValue::new_object();
        obj[name] = settings.get_value(name)?.clone();
        session.respond(obj)?;
    }

    Ok(())
}

pub fn ou_setting_ancestor_default_batch(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsActorWorker::downcast(worker)?;
    let org_id = method.param(0).int()?;

    let setting_names: Vec<&str> = method
        .param(1)
        .members() // iterate json array
        .filter(|v| v.is_string())
        .map(|v| v.as_str().unwrap())
        .collect();

    if setting_names.is_empty() {
        return Ok(());
    }

    let mut editor = Editor::new(worker.client());
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
        settings.set_user_id(editor.requestor_id()?);
    }

    // Pre-cache the settings en masse, then pull each from the settings
    // cache and return to the caller.
    settings.fetch_values(setting_names.as_slice())?;

    for name in setting_names {
        let mut obj = EgValue::new_object();
        obj[name] = settings.get_value(name)?.clone();
        session.respond(obj)?;
    }

    Ok(())
}

pub fn user_opac_vital_stats(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsActorWorker::downcast(worker)?;
    let authtoken = method.param(0).str()?;

    let mut editor = Editor::with_auth(worker.client(), authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    let user_id = method.param(1).as_i64().unwrap_or(editor.requestor_id()?);
    let mut user = match editor.retrieve("au", user_id)? {
        Some(u) => u,
        None => return session.respond(editor.event()),
    };

    if user_id != editor.requestor_id()? {
        let home_ou = user["home_ou"].int()?;

        // This list of perms seems like overkill for summary data, but
        // it matches the perm checks of the existing open-ils.actor APIs.
        if !editor.allowed_at("VIEW_USER", home_ou)?
            || !editor.allowed_at("VIEW_USER_FINES_SUMMARY", home_ou)?
            || !editor.allowed_at("VIEW_CIRCULATIONS", home_ou)?
            || !editor.allowed_at("VIEW_HOLD", home_ou)?
        {
            return session.respond(editor.event());
        }
    }

    let holds = user::active_hold_counts(&mut editor, user_id)?;
    let fines = user::fines_summary(&mut editor, user_id)?;
    let checkouts = user::open_checkout_counts(&mut editor, user_id)?;

    let unread_query = eg::hash! {
        select: {aum: [{
            column: "id",
            transform: "count",
            aggregate: 1,
            alias: "count",
        }]},
        from: "aum",
        where: {
            usr: user_id,
            read_date: EgValue::Null,
            deleted: "f",
            pub: "t",
        }
    };

    let mut unread_count = 0;
    if let Some(unread) = editor.json_query(unread_query)?.first() {
        unread_count = unread["count"].int()?;
    }

    let resp = eg::hash! {
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

pub fn update_penalties(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsActorWorker::downcast(worker)?;
    let authtoken = method.param(0).str()?;
    let user_id = method.param(1).int()?;

    let mut editor = Editor::with_auth(worker.client(), authtoken);

    if !editor.checkauth()? {
        return session.respond(editor.event());
    }

    let user = match editor.retrieve("au", user_id)? {
        Some(u) => u,
        None => return session.respond(editor.event()),
    };

    let mut context_org = user["home_ou"].int()?;

    if !editor.allowed_at("UPDATE_USER", context_org)? {
        return session.respond(editor.event());
    }

    if method.method().contains("_at_home") {
        if let Some(org) = editor.requestor_ws_ou() {
            context_org = org;
        }
    }

    let only_penalties = match method.params().get(2) {
        Some(op) => match op {
            EgValue::Array(arr) => Some(arr),
            _ => None,
        },
        None => None,
    };

    editor.xact_begin()?;

    penalty::calculate_penalties(&mut editor, user_id, context_org, only_penalties)?;

    editor.commit()?;

    session.respond(1)
}
