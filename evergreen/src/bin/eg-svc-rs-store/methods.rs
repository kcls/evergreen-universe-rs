//use eg::util;
//use eg::settings::Settings;
//use eg::idldb::{IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
use eg::idldb::Translator;
use evergreen as eg;
use opensrf::app::ApplicationWorker;
use opensrf::message;
//use opensrf::method::{ParamCount, ParamDataType, StaticMethod, StaticParam};
use opensrf::method::StaticMethod;
use opensrf::session::ServerSession;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethod] = &[];

// open-ils.rs-store.direct.actor.user.retrieve
pub fn retrieve(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    let worker = app::RsStoreWorker::downcast(worker)?;
    let apiname = method.method();

    // TODO some of this can be moved to a shared function
    let api_parts = apiname.split(".").collect::<Vec<&str>>();

    if api_parts.len() != 6 {
        // shouldn't happen
        Err(format!("Invalid API call: {}", apiname))?;
    }

    if api_parts[5].ne("retrieve") {
        // shouldn't happen
        Err(format!("Invalid API call: {}", apiname))?;
    }

    let fieldmapper = format!("{}::{}", &api_parts[3], &api_parts[4]);

    let idl = worker.env().idl().clone();
    let db = worker.database().clone();

    let idl_class = match idl
        .classes()
        .values()
        .filter(|c| {
            if let Some(fm) = c.fieldmapper() {
                return fm.eq(fieldmapper.as_str());
            }
            return false;
        })
        .next()
    {
        Some(c) => c,
        None => Err(format!("Not a valid IDL class fieldmapper={fieldmapper}"))?,
    };

    let classname = idl_class.classname().clone(); // mixed mutable borrow
    let pkey = method.param(0);

    let translator = Translator::new(idl.to_owned(), db);

    if let Some(obj) = translator.idl_class_by_pkey(classname, pkey)? {
        session.respond(obj)
    } else {
        Ok(())
    }
}
