use eg::EgResult;
use eg::EgValue;
use eg::Editor;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use evergreen as eg;

use serde_json;
use smarty_rust_sdk;
use tokio;

use geo::prelude::Contains;                                                    
use shapefile::record::*;
use std::path::Path;

use smarty_rust_sdk::sdk::authentication::SecretKeyCredential;
use smarty_rust_sdk::sdk::batch::Batch;
use smarty_rust_sdk::sdk::options::Options;
use smarty_rust_sdk::sdk::options::OptionsBuilder;
use smarty_rust_sdk::us_autocomplete_pro_api;
use smarty_rust_sdk::us_autocomplete_pro_api::client::USAutocompleteProClient;
use smarty_rust_sdk::us_street_api;
use smarty_rust_sdk::us_street_api::client::USStreetAddressClient;

const MAX_RESULT_CANDIDATES: i64 = 5;
const MAX_AUTO_RESULTS: i32 = 5;

/// Generic error to return to the caller.
const ADDR_LOOKUP_ERROR: &str = "Address lookup error";

// TODO
const DEFAULT_ADDR_DATA_DIR: &str = "/usr/local/share/evergreen/addrs-data";

// Import our local app module
use crate::app;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "lookup",
        desc: "Get details for the provided address",
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
        desc: "Suggest matches for a partial address",
        param_count: ParamCount::Exactly(2),
        handler: autocomplete,
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
    // 47.54030395464964, -122.05041577546649
    StaticMethodDef {
        name: "home-org",
        desc: "Closest/best org unit to use as the home org based on lat/long",
        param_count: ParamCount::Exactly(3),
        handler: home_org,
        params: &[
            StaticParam {
                name: "Session Token",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Latitude",
                datatype: ParamDataType::Numeric,
                desc: "Numeric value between -90 and 90; e.g. 47.54030395464964",
            },
            StaticParam {
                name: "Longitude",
                datatype: ParamDataType::Numeric,
                desc: "Numeric value between -180 and 180; e.g. -122.05041577546649",
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

/// Find detailed information on a specific address.
///
/// # Reference
///
/// * <https://docs.rs/smarty-rust-sdk/0.4.4/smarty_rust_sdk/us_street_api/index.html>
pub fn lookup(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let search = method.param(1);

    // TODO verify sestoken

    let mut lookup = us_street_api::lookup::Lookup {
        max_candidates: MAX_RESULT_CANDIDATES,
        match_strategy: us_street_api::lookup::MatchStrategy::Enhanced,
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
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.handle().block_on(async {
        send_result = client.send(&mut batch).await;
    });

    send_result.map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for record in batch.records() {
        for result in &record.results {
            // Assumes Smarty returns serde-serializable data.
            let s = serde_json::to_string(&result).unwrap();

            log::debug!("Got lookup result: {s}");

            // There's no direct crosswalk from serde json to vanilla
            // json, so do the stringify+parse dance.
            session.respond(EgValue::parse(&s)?)?;
        }
    }

    Ok(())
}

/// Generate address suggestions from an initial address value.
///
/// # Reference
///
/// * <https://docs.rs/smarty-rust-sdk/0.4.4/smarty_rust_sdk/us_autocomplete_pro_api/index.html>
pub fn autocomplete(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let search = method.param(1);

    // TODO verify sestoken

    let search_str = search["search"]
        .to_string()
        .ok_or("autocomplete 'search' required'")?;

    let mut lookup = us_autocomplete_pro_api::lookup::Lookup {
        search: search_str,
        max_results: MAX_AUTO_RESULTS,
        ..Default::default()
    };

    if let Some(state) = search["state_filter"].as_str() {
        lookup.state_filter = vec![state.to_string()];
    }

    if let Some(state) = search["prefer_state"].as_str() {
        lookup.prefer_state = vec![state.to_string()];
    }

    if let Some(zip) = search["zip_filter"].as_str() {
        lookup.zip_filter = vec![zip.to_string()];
    }

    let options = smarty_sdk_options("us-autocomplete-pro-cloud")?;

    let client = USAutocompleteProClient::new(options).map_err(|e| {
        log::error!("Cannot create USAutocompleteProClient: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    let mut send_result = Ok(());

    // Await'ing async methods in a non-async environment is not
    // supported, and Smarty offers no sync variant of their SDK.  Wrap
    // the await in a runtime block_on().
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.handle().block_on(async {
        send_result = client.send(&mut lookup).await;
    });

    send_result.map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for suggestion in lookup.results.suggestions {
        log::debug!("Got record: {suggestion:?}");

        // Assumes Smarty returns serde-serializable data.
        let s = serde_json::to_string(&suggestion).unwrap();

        // There's no direct crosswalk from serde json to vanilla
        // json, so do the stringify+parse dance.
        session.respond(EgValue::parse(&s)?)?;
    }

    Ok(())
}

/// Find the best/closest home library given the provided lat/long values based
/// on predefined shapefiles.
///
/// TODO configs and file locations
pub fn home_org(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let lat = method.param(1).float()?;
    let long = method.param(2).float()?;
    let mut editor = Editor::new(worker.client());

    let query = eg::hash! {
        "select": {"aou": ["id", "shortname"]},
        "from": {"aou": "aout"},
        "where": {"+aout": {"can_have_users": "t"}}
    };

    let org_list = editor.json_query(query)?;

    let mut response = eg::hash! {
        "home_ou": EgValue::Null,
        "is_reciprocal": false
    };

    for org in org_list {
        let code = org["shortname"].string()?;
        let shapefile = format!("{DEFAULT_ADDR_DATA_DIR}/shapefiles/home-orgs/{code}/{code}.shp");

        if shapefile_contains(&shapefile, lat, long)? {
            response["home_ou"] = org["id"].clone();
            return session.respond(response);
        }
    }

    // TODO scan recip files

    session.respond(response)
}

/// Returns true if the shapefile provided contains the lat/long provided.
fn shapefile_contains(shapefile: &str, lat: f64, long: f64) -> EgResult<bool> {
    log::debug!("Inspecting shapefile {shapefile} for lat={lat} and long={long}");

    if !Path::new(shapefile).exists() {
        // Assumption here is not every branch will have a shapefile in place.
        // If not, avoid returning an error.
        log::debug!("No such shapefile: {shapefile}");

        return Ok(false);
    }

    let mut reader = shapefile::Reader::from_path(&shapefile)
        .map_err(|e| format!("Cannot read shapefile: {shapefile}: {e}"))?;

    let point = Point::new(long, lat); // x, y
                                                                               
    for shape_record in reader.iter_shapes_and_records() {                     
        let (shape, _record) = shape_record
            .map_err(|e| format!("Cannot extract shape/record from {shapefile}: {e}"))?;

        if let Shape::Polygon(poly) = shape {
            let geo_poly: geo::MultiPolygon<f64> = poly.into();
            let geo_point: geo::Point<f64> = point.clone().into();

            if geo_poly.contains(&geo_point) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

