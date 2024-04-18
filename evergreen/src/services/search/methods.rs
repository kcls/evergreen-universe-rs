use eg::common::bib;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use evergreen as eg;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "biblio.record.catalog_summary",
        desc: "Catalog Record Summary",
        param_count: ParamCount::Range(2, 3),
        handler: catalog_record_summary,
        params: &[
            StaticParam {
                name: "Org Unit ID",
                datatype: ParamDataType::Number,
                desc: "Context Org Unit",
            },
            StaticParam {
                name: "Record IDs",
                datatype: ParamDataType::Array,
                desc: ""
            },
            StaticParam {
                name: "Options",
                datatype: ParamDataType::Object,
                desc: "Options Hash",
            },
        ],
    },
];

pub fn catalog_record_summary(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    let worker = app::RsSearchWorker::downcast(worker)?;

    let _org_id = method.param(0).int()?;
    let _options = method.params().get(2); // optional

    let mut editor = Editor::new(worker.client());

    for rec_id in method.param(1).members() {
        let rec_id = rec_id.int()?;
        let summary = bib::catalog_record_summary(&mut editor, rec_id)?;

        session.respond(summary.into_value())?;
    }

    Ok(())
}
