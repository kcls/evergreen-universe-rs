// Debugging Smarty streets:
// OSRF_LOG_LEVEL=trace and run by hand (or change config)
// Get the POST info out of the logs
// Translate that into a curl, e.g.:
//
// curl -X GET
// "https://us-street.api.smarty.com/street-address?auth-id=<AUTHID>&auth-token=<TOKEN>&license=&city=Yakima&zipcode=98903&candidates=5&match=enhanced&format=default" \
//  -H "Content-Type: application/json; charset=utf-8" -v
//
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;

use serde_json;

use crate::smarty::{AutocompleteRequest, LookupRequest, SmartyClient};

const MAX_LOOKUP_RESULTS: i64 = 100;
const DEFAULT_LOOKUP_RESULTS: i64 = 100;
const MAX_AUTOCOMPLETE_RESULTS: i32 = 10;
const DEFAULT_AUTOCOMPLETE_RESULTS: i32 = 10;

/// Generic error to return to the caller.
const ADDR_LOOKUP_ERROR: &str = "Address lookup error";

// TODO: Add to systemd config
const DEFAULT_ADDR_DATA_DIR: &str = "/usr/local/share/evergreen/address-data";

// Import our local modules
use crate::app;
use crate::shapefile_util::shapefile_contains;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "lookup",
        desc: "Get details for the provided address",
        param_count: ParamCount::Range(2, 3),
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
            StaticParam {
                name: "Limit",
                datatype: ParamDataType::Number,
                desc: "Maximum results to return",
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
    StaticMethodDef {
        name: "district-of-residence",
        desc: "Returns the name of the reciprocal library district for the provided lat/long where appropriate",
        param_count: ParamCount::Exactly(3),
        handler: district_of_residence,
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

/// Find detailed information on a specific address.
///
/// # Reference
///
/// * <https://www.smarty.com/docs/apis/us-street-api/reference>
pub fn lookup(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let search = method.param(1);

    let mut candidates = DEFAULT_LOOKUP_RESULTS;
    if let Some(Some(v)) = method.params().get(2).map(|v| v.as_i64()) {
        candidates = std::cmp::min(v, MAX_LOOKUP_RESULTS);
    }

    // TODO verify sestoken

    // For now, support and map a specific subset of search options,
    // partly to limit control (e.g. candidates) but also to avoid
    // vendor-specific APIs.
    let mut req = LookupRequest {
        candidates: Some(candidates as u32),
        match_strategy: Some("enhanced".to_string()),
        ..Default::default()
    };

    if let Some(street) = search["street"].as_str() {
        req.street = street.to_string();
    }
    if let Some(street2) = search["street2"].as_str() {
        req.street2 = Some(street2.to_string());
    }
    if let Some(city) = search["city"].as_str() {
        req.city = Some(city.to_string());
    }
    if let Some(state) = search["state"].as_str() {
        req.state = Some(state.to_string());
    }
    // zipcode could be numeric
    if let Some(zipcode) = search["zipcode"].to_string() {
        req.zipcode = Some(zipcode);
    }

    let client = SmartyClient::from_env().map_err(|e| {
        log::error!("{e}");
        ADDR_LOOKUP_ERROR
    })?;

    let candidates = client.lookup(&req).map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for candidate in &candidates {
        let jv = serde_json::to_value(candidate)
            .map_err(|e| format!("Cannot translate candidate to json value: {e}"))?;

        log::debug!("Got lookup result: {jv}");

        session.respond(EgValue::from_json_value(jv)?)?;
    }

    Ok(())
}

/// Generate address suggestions from an initial address value.
///
/// # Reference
///
/// * <https://www.smarty.com/docs/apis/us-autocomplete-pro-api/reference>
pub fn autocomplete(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let search = method.param(1);

    // TODO verify sestoken

    let search_str = search["search"]
        .to_string()
        .ok_or("autocomplete 'search' required'")?;

    let max_results = std::cmp::min(
        search["limit"].as_i32().unwrap_or(DEFAULT_AUTOCOMPLETE_RESULTS),
        MAX_AUTOCOMPLETE_RESULTS
    );

    let mut req = AutocompleteRequest {
        search: search_str.clone(),
        max_results: Some(max_results as u32),
        ..Default::default()
    };

    if let Some(state) = search["state_filter"].as_str() {
        req.include_only_states = vec![state.to_string()];
    }

    if let Some(state) = search["prefer_state"].as_str() {
        req.prefer_states = vec![state.to_string()];
    }

    if let Some(zip) = search["zip_filter"].as_str() {
        req.include_only_zip_codes = vec![zip.to_string()];
    }

    // Optional secondary (unit/apartment) expansion selector, formatted as
    // "street_line secondary (entries) city state zipcode".
    if let Some(selected) = search["selected"].as_str() {
        req.selected = Some(selected.to_string());
    }

    let client = SmartyClient::from_env().map_err(|e| {
        log::error!("{e}");
        ADDR_LOOKUP_ERROR
    })?;

    let results = client.autocomplete(&req).map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    let mut suggestions = Vec::new();

    for suggestion in &results {
        log::info!("Got record: {suggestion:?}");

        let jv = serde_json::to_value(suggestion)
            .map_err(|e| format!("Cannot translate suggestion to json value: {e}"))?;

        suggestions.push(EgValue::from_json_value(jv)?);
    }

    let mut editor = Editor::new(worker.client());
    append_autocomplete_exceptions(&mut editor, &mut suggestions, &search_str)?;

    suggestions.sort_by_key(|a| a["street_line"].as_str().unwrap_or("").to_string());

    for sug in suggestions.drain(..) {
        session.respond(sug)?;
    }

    Ok(())
}

/// Add addresses from the local address exception table which match
/// to the caller's search string.
fn append_autocomplete_exceptions(
    editor: &mut Editor,
    suggestions: &mut Vec<EgValue>,
    search_str: &str
) -> EgResult<()> {

    let search_normalized = search_str
        .replace(',', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase();

    // This could be more efficient.
    // However we'd have to do an ilike prefix search on the full
    // address string, not just e.g. street1, with handling for empty values, etc.
    // At time of writing, the list of addresses exceptions is very small.
    let exceptions = editor.search_with_ops(
        "cuae",
        eg::hash! {"enabled": "t"},
        eg::hash! {
            "flesh": 1,
            "flesh_fields": {"cuae": ["district_of_residence"]},
        }
    )?;

    for addr in &exceptions {

        let mut street_line = addr["street1"].as_str().unwrap_or("").to_string();
        if let Some(s2) = addr["street2"].as_str() && !s2.is_empty() {
            if !street_line.is_empty() {
                street_line += " ";
            }
            street_line += s2;
        }

        // Fuzzy match the caller's search string.  This is less
        // sophisticated than Smarty's matching.  May need some
        // additional smarts.
        if street_line.to_lowercase().starts_with(&search_normalized) {
            log::info!("Found local address exception search='{search_normalized}' exception='{street_line}'");

            let mut response = eg::hash! {
                "is_exception": true,
                "exception_id": addr.id()?,
                "is_allowed": addr["is_allowed"].boolish(),
                "street_line": street_line,
                "city": addr["city"].as_str().unwrap_or(""),
                "state": addr["state"].as_str().unwrap_or(""),
                "zipcode": addr["post_code"].as_str().unwrap_or(""),
            };

            // Only allowed (i.e. non-blocked) addresses contain the needed
            // values to create an account.
            if addr["is_allowed"].boolish() {
                response["home_ou"] = addr["home_ou"].clone();
                response["district_of_residence"] = addr["district_of_residence"]["name"].clone();
            }

            // If an address provided by the API matches the address exception,
            // remove the API version.
            suggestions.retain(|addr| 
                addr["street_line"].as_str().unwrap_or("").to_lowercase() != street_line.to_lowercase());

            suggestions.push(response);
        }
    }

    Ok(())
}

/// Find the best/closest home library given the provided lat/long values based
/// on predefined shapefiles.
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
        "select": {"aou": ["id", "shortname"], "aouc": ["latitude", "longitude"]},
        "from": {"aou": {"aout": {}, "aouc": {}}},
        "where": {"+aout": {"can_have_users": "t"}}
    };

    let org_list = editor.json_query(query)?;

    for org in &org_list {
        let code = org["shortname"].string()?;
        let shapefile = format!("{DEFAULT_ADDR_DATA_DIR}/shapefiles/home-orgs/{code}/{code}.shp");

        if shapefile_contains(&shapefile, lat, long)? {
            return session.respond(org.id()?);
        }
    }

    // Provided lat/long does not match the direct coverage area of any
    // branch.  Find the closest branch as the crow flies.

    let mut closest: Option<(i64, f64)> = None;

    for org in &org_list {
        let org_id = org.id()?;
        let latitude = org["latitude"].float()?;
        let longitude = org["longitude"].float()?;

        let distance = crow_flies_distance(lat, long, latitude, longitude);

        log::info!(
            "Testing {lat}/{long} values at branch {org_id} => \
             {latitude}/{longitude} (distance {distance:.3}km)"
        );

        if closest.map(|(_, best)| distance < best).unwrap_or(true) {
            closest = Some((org_id, distance));
        }
    }

    if let Some((org_id, _)) = closest {
        return session.respond(org_id);
    }

    Ok(())
}

/// Great-circle distance in kilometers between two lat/long points
/// (haversine formula).
///
/// Used to pick the nearest branch when an address falls outside every
/// branch's coverage shapefile.  Only relative ordering matters here, so
/// the choice of units (km) is arbitrary.
fn crow_flies_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();

    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);

    EARTH_RADIUS_KM * 2.0 * a.sqrt().asin()
}


/// Determine if the provided lat/long exists within any of the library
/// district of residence shapefiles.
pub fn district_of_residence(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let _sestoken = method.param(0).str()?;
    let lat = method.param(1).float()?;
    let long = method.param(2).float()?;

    let districts_dir = format!("{DEFAULT_ADDR_DATA_DIR}/shapefiles/districts");

    let entries = std::fs::read_dir(&districts_dir).map_err(|e| {
        log::error!("Cannot read districts directory {districts_dir}: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            log::error!("Error reading directory entry: {e}");
            ADDR_LOOKUP_ERROR
        })?;

        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("shp") {
            continue;
        }

        let shapefile = path.to_string_lossy().to_string();

        if shapefile_contains(&shapefile, lat, long)? {
            let mut name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();

            // Should never happen but lets avoid chaos
            if name.is_empty() { continue; }

            if &name[0..1] == "_" {
                // Some stat cats start with a space character for sorting.
                // On disk they start with an underscore.
                name = name.replacen('_', " ", 1); 
            }

            return session.respond(name);
        }
    }

    Ok(())
}

