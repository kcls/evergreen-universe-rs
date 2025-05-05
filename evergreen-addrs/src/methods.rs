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
use smarty_rust_sdk::sdk::options::Options;
use smarty_rust_sdk::sdk::authentication::SecretKeyCredential;
use smarty_rust_sdk::us_street_api::client::USStreetAddressClient;

const MAX_RESULT_CANDIDATES: i64 = 8;
/// Generic error to return to the caller.
const ADDR_LOOKUP_ERROR: &str = "Address lookup error";

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
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
    StaticMethodDef {
        name: "autocomplete",
        desc: "Autocomplete an address",
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


/// Build a set of SDK options with our authentication values.
fn smarty_sdk_options(license: &str) -> EgResult<Options> {

    let auth_id = std::env::var("SMARTY_AUTH_ID").map_err(|_| {
        log::error!("Missing SMARTY_AUTH_ID env var");
        ADDR_LOOKUP_ERROR
    })?;

    let auth_token = std::env::var("SMARTY_AUTH_TOKEN").map_err(|_| {
        log::error!("Missing SMARTY_AUTH_TOKEN env var");
        ADDR_LOOKUP_ERROR
    })?;

    let authentication = SecretKeyCredential::new(auth_id, auth_token);

    let options = OptionsBuilder::new(Some(authentication))
        // The appropriate license values to be used for your subscriptions
        // can be found on the Subscriptions page of the account dashboard.
        // https://www.smartystreets.com/docs/cloud/licensing
        .with_license(license)
        .build();

    Ok(options)
}


/// # Reference
///
/// * <https://docs.rs/smarty-rust-sdk/0.4.4/smarty_rust_sdk/us_street_api/lookup/struct.Lookup.html>
pub fn lookup(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let search = method.param(1);

    // TODO verify sestoken

    let mut lookup = Lookup {
        max_candidates: MAX_RESULT_CANDIDATES,
        match_strategy: MatchStrategy::Enhanced,
        ..Default::default()
    };

    // For now, support and map a specific subset of search options,
    // partly to limit control (e.g. max_candidates) but also to avoid
    // vendor-specific APIs.
    if let Some(street) = search["street"].as_str() {
        lookup.street = street.to_string();
    }
    if let Some(street2) = search["street2"].as_str() {
        lookup.street2 = street2.to_string();
    }
    if let Some(city) = search["city"].as_str() {
        lookup.city = city.to_string();
    }
    if let Some(state) = search["state"].as_str() {
        lookup.state = state.to_string();
    }
    if let Some(zipcode) = search["zipcode"].as_str() {
        lookup.zipcode = zipcode.to_string();
    }
    // could be numeric
    if let Some(zipcode) = search["zipcode"].to_string() {
        lookup.zipcode = zipcode;
    }

    let mut batch = Batch::default();

    if let Err(e) = batch.push(lookup) {
        log::error!("cannot create lookup() batch: {e}");
        return Err(ADDR_LOOKUP_ERROR.into());
    }

    let options = smarty_sdk_options("us-core-cloud")?;

    let client = USStreetAddressClient::new(options).map_err(|e| {
        log::error!("Cannot create USStreetAddressClient: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    let mut send_result = Ok(());

    // Await'ing async methods in a non-async environment is not
    // supported, and Smarty offers no sync variant of their SDK.  Wrap
    // the await in a runtime block_on().
    let rt  = tokio::runtime::Runtime::new().unwrap();
    rt.handle().block_on(async {
        send_result = client.send(&mut batch).await;
    });
    
    send_result.map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for record in batch.records() {
        println!("Got record: {record:?}");

        for result in &record.results {
            // Assumes Smarty returns serde-serializable data.
            let s = serde_json::to_string(&result).unwrap();

            // There's no direct crosswalk from serde json to vanilla
            // json, so do the stringify+parse dance.
            session.respond(EgValue::parse(&s)?)?;
        }
    }

    Ok(())
}
