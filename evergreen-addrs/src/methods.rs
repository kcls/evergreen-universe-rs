use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;

use serde_json;
use smarty_rust_sdk;
use tokio;

use smarty_rust_sdk::us_street_api::lookup::{Lookup, MatchStrategy};
use smarty_rust_sdk::sdk::batch::Batch;
use smarty_rust_sdk::sdk::options::OptionsBuilder;
use smarty_rust_sdk::sdk::authentication::SecretKeyCredential;
use smarty_rust_sdk::us_street_api::client::USStreetAddressClient;

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
///
/// These will form the basis (and possibly all) of our published methods.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "lookup",
        desc: "Find matching addresses",
        param_count: ParamCount::Exactly(2),
        handler: lookup,
        params: &[
            StaticParam {
                name: "Session Token",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Search",
                datatype: ParamDataType::Object,
                desc: "",
            },
        ],
    },
];

pub fn lookup(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    // Cast our worker instance into something we know how to use.
    let _worker = app::AddrsWorker::downcast(worker)?;

    // Extract the method call parameters.
    // Incorrectly shaped parameters will result in an error
    // response to the caller.
    let sestoken = method.param(0).str()?;
    let search = method.param(1);

    // TODO verify sestoken

    //session.respond(response)

    let lookup2 = Lookup {
        street: "1 Rosedale Street, Baltimore, MD".to_string(),
        max_candidates: 8,
        match_strategy: MatchStrategy::Enhanced,
        ..Default::default()
    };

    let mut batch = Batch::default();
    batch.push(lookup2).unwrap(); // TODO

    let authentication = SecretKeyCredential::new(
        std::env::var("SMARTY_AUTH_ID").expect("Missing SMARTY_AUTH_ID env variable"),
        std::env::var("SMARTY_AUTH_TOKEN").expect("Missing SMARTY_AUTH_TOKEN env variable"),
    );

    let options = OptionsBuilder::new(Some(authentication))
        // The appropriate license values to be used for your subscriptions
        // can be found on the Subscriptions page of the account dashboard.
        // https://www.smartystreets.com/docs/cloud/licensing
        .with_license("us-core-cloud")
        .build();

    let mut client = USStreetAddressClient::new(options).unwrap(); // TODO

    // This little bit of magic allows us to wrap and run an async method
    // so our API method does not have to be async.
    let rt  = tokio::runtime::Runtime::new().unwrap();
    rt.handle().block_on(async {
        client.send(&mut batch).await.unwrap(); // TODO
    });

    for record in batch.records() {
        println!("{}", serde_json::to_string_pretty(&record.results).unwrap() /* TODO */);

        //let s = serde_json::to_string(&record.results).unwrap();
        for result in &record.results {
            let s = serde_json::to_string(&result).unwrap();
            session.respond(EgValue::parse(&s)?)?;
        }
    }

    Ok(())
}
