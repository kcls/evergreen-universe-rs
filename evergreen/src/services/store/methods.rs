use eg::idldb::{IdlClassSearch, Translator};
use evergreen as eg;
use opensrf::app::ApplicationWorker;
use opensrf::message;
use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::session::ServerSession;
use std::sync::Arc;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[
    StaticMethod {
        name: "transaction.begin",
        desc: "Start a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    StaticMethod {
        name: "transaction.rollback",
        desc: "Rollback a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    StaticMethod {
        name: "transaction.commit",
        desc: "Commit a database transaction",
        param_count: ParamCount::Zero,
        handler: manage_xact,
        params: &[],
    },
    // Stub method for *.create calls.  Not directly published.
    StaticMethod {
        name: "create-stub",
        desc: "Create a new IDL object",
        param_count: ParamCount::Exactly(1),
        handler: create,
        params: &[StaticParam {
            required: true,
            name: "IDL Object",
            datatype: ParamDataType::Object,
            desc: "Object to update",
        }],
    },
    // Stub method for *.retrieve calls. Not directly published.
    StaticMethod {
        name: "retrieve-stub",
        desc: "Retrieve an IDL object by its primary key",
        param_count: ParamCount::Exactly(1),
        handler: retrieve,
        params: &[StaticParam {
            required: true,
            name: "primary-key",
            datatype: ParamDataType::Scalar,
            desc: "Primary Key Value",
        }],
    },
    // Stub method for *.search calls. Not directly published.
    StaticMethod {
        name: "search-stub",
        desc: "search an IDL object by its primary key",
        param_count: ParamCount::Exactly(1),
        handler: search,
        params: &[StaticParam {
            required: true,
            name: "query",
            datatype: ParamDataType::Object,
            desc: "Query Object",
        }],
    },
    // Stub method for *.update calls. Not directly published.
    StaticMethod {
        name: "update-stub",
        desc: "Update an IDL object",
        param_count: ParamCount::Exactly(1),
        handler: update,
        params: &[StaticParam {
            required: true,
            name: "IDL Object",
            datatype: ParamDataType::Object,
            desc: "Object to update",
        }],
    },
    // Stub method for *.delete calls.  Not directly published.
    StaticMethod {
        name: "delete-stub",
        desc: "Delete an IDL object by its primary key",
        param_count: ParamCount::Exactly(1),
        handler: delete,
        params: &[StaticParam {
            required: true,
            name: "primary-key",
            datatype: ParamDataType::Scalar,
            desc: "Primary Key Value",
        }],
    },
];

/// Get the IDL class info from the API call split into parts by ".".
///
/// Also verifies the API name has the correct number of parts.
fn get_idl_class(idl: &Arc<eg::idl::Parser>, apiname: &str) -> Result<String, String> {
    let api_parts = apiname.split(".").collect::<Vec<&str>>();

    if api_parts.len() != 6 {
        // Could potentially happen if an IDL class was not correctly
        // encoded in the IDL file.
        Err(format!("Invalid API call: {:?}", api_parts))?;
    }

    let fieldmapper = format!("{}::{}", &api_parts[3], &api_parts[4]);

    for class in idl.classes().values() {
        if let Some(fm) = class.fieldmapper() {
            if fm.eq(fieldmapper.as_str()) {
                return Ok(class.classname().to_string());
            }
        }
    }

    Err(format!("Not a valid IDL class fieldmapper={fieldmapper}"))
}

// open-ils.rs-store.direct.actor.user.retrieve
pub fn retrieve(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let idl = worker.env().idl().clone();
    let classname = get_idl_class(&idl, method.method())?;

    let pkey = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(idl, db);

    if let Some(obj) = translator.get_idl_object_by_pkey(&classname, pkey)? {
        session.respond(obj)
    } else {
        Ok(())
    }
}

// open-ils.rs-store.direct.actor.user.search
pub fn search(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let idl = worker.env().idl().clone();
    let classname = get_idl_class(&idl, method.method())?;

    let db = worker.database().clone();
    let translator = Translator::new(idl, db);

    let query = method.param(0);
    let mut search = IdlClassSearch::new(&classname);
    search.set_filter(query.clone());

    for value in translator.idl_class_search(&search)? {
        session.respond(value)?;
    }

    Ok(())
}

// open-ils.rs-store.direct.actor.user.delete
pub fn delete(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let idl = worker.env().idl().clone();
    let classname = get_idl_class(&idl, method.method())?;

    let pkey = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(idl, db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.delete_idl_object_by_pkey(&classname, pkey)?;
    session.respond(count)
}

// open-ils.rs-store.direct.actor.user.create
pub fn create(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let idl = worker.env().idl().clone();
    let obj = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(idl, db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.create_idl_object(&obj)?;
    session.respond(count)
}

// open-ils.rs-store.direct.actor.user.update
pub fn update(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let idl = worker.env().idl().clone();
    let obj = method.param(0);

    let db = worker.database().clone();
    let translator = Translator::new(idl, db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.update_idl_object(&obj)?;
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
    method: &message::Method,
) -> Result<(), String> {
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
