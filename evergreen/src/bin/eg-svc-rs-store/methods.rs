//use eg::util;
//use eg::settings::Settings;
//use eg::idldb::{IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
use eg::idldb::Translator;
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
        // Stub method for *.retrieve calls
        // This is not directly published.
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
    StaticMethod {
        // Stub method for *.delete calls
        // This is not directly published.
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

/// Get the IDL class info from the API call split into parts (by ".").
///
/// Also verifies the API name has the correct number of parts.
fn get_idl_class(idl: &Arc<eg::idl::Parser>, apiname: &str) -> Result<String, String> {
    let api_parts = apiname.split(".").collect::<Vec<&str>>();

    // We know api_parts is
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
    let translator = Translator::new(idl.to_owned(), db);

    if let Some(obj) = translator.get_idl_object_by_pkey(&classname, pkey)? {
        session.respond(obj)
    } else {
        Ok(())
    }
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
    let translator = Translator::new(idl.to_owned(), db);

    // This will fail if our database connection is not already
    // inside a transaction.
    let count = translator.delete_idl_object_by_pkey(&classname, pkey)?;
    session.respond(count)
}

// TODO xact api's
