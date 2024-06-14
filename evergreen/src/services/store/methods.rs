use eg::common::jq::JsonQueryCompiler;
use eg::idl;
use eg::idldb::{FleshDef, IdlClassSearch, Translator};
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::EgResult;
use evergreen as eg;
use pg::types::ToSql;
use postgres as pg;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "transaction.begin",
        desc: "Start a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    StaticMethodDef {
        name: "transaction.rollback",
        desc: "Rollback a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    StaticMethodDef {
        name: "transaction.commit",
        desc: "Commit a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    // Stub method for *.create calls.  Not directly published.
    StaticMethodDef {
        name: "create-stub",
        desc: "Create a new IDL object",
        param_count: ParamCount::Exactly(1),
        handler: create,
        params: &[StaticParam {
            name: "IDL Object",
            datatype: ParamDataType::Object,
            desc: "Object to update",
        }],
    },
    // Stub method for *.retrieve calls. Not directly published.
    StaticMethodDef {
        name: "retrieve-stub",
        desc: "Retrieve an IDL object by its primary key",
        param_count: ParamCount::Range(1, 2),
        handler: retrieve,
        params: &[
            StaticParam {
                name: "primary-key",
                datatype: ParamDataType::Scalar,
                desc: "Primary Key Value",
            },
            StaticParam {
                name: "flesh",
                datatype: ParamDataType::Object,
                desc: "Flesh Fields Object",
            },
        ],
    },
    // Stub method for *.search calls. Not directly published.
    StaticMethodDef {
        name: "search-stub",
        desc: "search an IDL object by its primary key",
        param_count: ParamCount::Range(1, 2),
        handler: search,
        params: &[
            StaticParam {
                name: "query",
                datatype: ParamDataType::Object,
                desc: "Query Object",
            },
            StaticParam {
                name: "flesh",
                datatype: ParamDataType::Object,
                desc: "Flesh Fields Object",
            },
        ],
    },
    // Stub method for *.update calls. Not directly published.
    StaticMethodDef {
        name: "update-stub",
        desc: "Update an IDL object",
        param_count: ParamCount::Exactly(1),
        handler: update,
        params: &[StaticParam {
            name: "IDL Object",
            datatype: ParamDataType::Object,
            desc: "Object to update",
        }],
    },
    // Stub method for *.delete calls.  Not directly published.
    StaticMethodDef {
        name: "delete-stub",
        desc: "Delete an IDL object by its primary key",
        param_count: ParamCount::Exactly(1),
        handler: delete,
        params: &[StaticParam {
            name: "primary-key",
            datatype: ParamDataType::Scalar,
            desc: "Primary Key Value",
        }],
    },
    // Stub method for *.delete calls.  Not directly published.
    StaticMethodDef {
        name: "json_query",
        desc: "JSON Query",
        param_count: ParamCount::Exactly(1),
        handler: json_query,
        params: &[StaticParam {
            name: "query-object",
            datatype: ParamDataType::Object,
            desc: "JSON Query Object/Hash",
        }],
    },
];

/// Get the IDL class info from the API call split into parts by ".".
///
/// Also verifies the API name has the correct number of parts.
fn get_idl_class(apiname: &str) -> EgResult<String> {
    let api_parts = apiname.split('.').collect::<Vec<&str>>();

    let len = api_parts.len();
    if !(6..=7).contains(&len) {
        // .atomic
        // Could potentially happen if an IDL class was not correctly
        // encoded in the IDL file.
        Err(format!("Invalid API call: {:?}", api_parts))?;
    }

    let fieldmapper = format!("{}::{}", &api_parts[3], &api_parts[4]);

    for class in idl::parser().classes().values() {
        if let Some(fm) = class.fieldmapper() {
            if fm.eq(fieldmapper.as_str()) {
                return Ok(class.classname().to_string());
            }
        }
    }

    Err(format!("Not a valid IDL class fieldmapper={fieldmapper}").into())
}

// open-ils.rs-store.direct.actor.user.retrieve
pub fn retrieve(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let classname = get_idl_class(method.method())?;

    let pkey = method.param(0); // at least 1 param is guaranteed

    let db = worker.database().clone();
    let translator = Translator::new(db);

    let mut flesh_def = None;
    if let Some(j) = method.params().get(1) {
        flesh_def = Some(FleshDef::from_eg_value(j)?);
    }

    if let Some(obj) = translator.get_idl_object_by_pkey(&classname, pkey, flesh_def)? {
        session.respond(obj)
    } else {
        Ok(())
    }
}

// open-ils.rs-store.direct.actor.user.search
pub fn search(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let classname = get_idl_class(method.method())?;

    let db = worker.database().clone();
    let translator = Translator::new(db);

    let query = method.param(0);
    let mut search = IdlClassSearch::new(&classname);
    search.set_filter(query.clone());

    if let Some(j) = method.params().get(1) {
        search.flesh = Some(FleshDef::from_eg_value(j)?);
    }

    for value in translator.idl_class_search(&search)? {
        session.respond(value)?;
    }

    Ok(())
}

// open-ils.rs-store.direct.actor.user.delete
pub fn delete(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let classname = get_idl_class(method.method())?;

    let pkey = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.delete_idl_object_by_pkey(&classname, pkey)?;
    session.respond(count)
}

// open-ils.rs-store.direct.actor.user.create
pub fn create(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let obj = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.create_idl_object(obj)?;
    session.respond(count)
}

// open-ils.rs-store.direct.actor.user.update
pub fn update(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let obj = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.update_idl_object(obj)?;
    session.respond(count)
}

/// begin, commit, and rollback the transaction on our primary database
/// connection.
///
/// "begin" will return Err() if a transaction is in progress.
/// "commit" will return Err() if no transaction is in progress.
pub fn manage_xact(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let db = worker.database();
    let api = method.method();

    if api.contains(".begin") {
        db.borrow_mut().xact_begin()?;
    } else if api.contains(".rollback") {
        // Avoid warnings/errors on rollback if no transaction
        // is in progress.
        if db.borrow().in_transaction() {
            db.borrow_mut().xact_rollback()?;
        }
    } else if api.contains(".commit") {
        // Returns Errif there is no transaction in progress
        db.borrow_mut().xact_commit()?;
    }

    session.respond(true)
}

// open-ils.rs-store.direct.actor.user.update
pub fn json_query(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let query = method.param(0);

    let db = worker.database().clone();

    let mut jq_compiler = JsonQueryCompiler::new();
    jq_compiler.compile(query)?;

    let sql = jq_compiler
        .query_string()
        .ok_or_else(|| format!("JSON query failed to produce valid SQL: {}", query.dump()))?;

    // Do a little translation dance here to get the param values
    // into a container our DB API can accept.
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let qparams: Vec<String> = jq_compiler
        .query_params()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for p in qparams.iter() {
        params.push(p);
    }

    let query_res = db.borrow_mut().client().query(sql, &params);

    if let Err(ref e) = query_res {
        log::error!("DB Error: {e} query={query} param={params:?}");
        Err("DB query failed. See error logs".to_string())?;
    }

    for row in query_res.unwrap() {
        let mut obj = eg::hash! {};

        for (idx, col) in row.columns().iter().enumerate() {
            obj[col.name()] = Translator::col_value_to_json_value(&row, idx)?;
        }

        session.respond(obj)?;
    }

    Ok(())
}
