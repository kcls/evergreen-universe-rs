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
use crate::exception_match::{AddressSearch, ExceptionRecord};
use crate::shapefile_util::shapefile_contains;
use crate::turnstile;
use std::collections::HashMap;
// NOTE: the session-token module is referenced via crate::session::* to
// avoid colliding with the ServerSession parameter named `session`.

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[
    StaticMethodDef {
        name: "session.create",
        desc: "Verify a CAPTCHA response and mint a short-lived session token",
        param_count: ParamCount::Range(1, 2),
        handler: session_create,
        params: &[
            StaticParam {
                name: "CAPTCHA Response",
                datatype: ParamDataType::String,
                desc: "Turnstile response token from the browser widget",
            },
            StaticParam {
                name: "Remote IP",
                datatype: ParamDataType::String,
                desc: "Optional client IP address",
            },
        ],
    },
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
    StaticMethodDef {
        name: "exception.matches",
        desc: "Find address-exception rows matching the provided address",
        param_count: ParamCount::Range(2, 3),
        handler: exception_matches,
        params: &[
            StaticParam {
                name: "Session Token",
                datatype: ParamDataType::String,
                desc: "",
            },
            StaticParam {
                name: "Address",
                datatype: ParamDataType::Object,
                desc: "Object with street1, street2, city, state, post_code",
            },
            StaticParam {
                name: "Is Allowed Filter",
                datatype: ParamDataType::Boolish,
                desc: "Filter on is_allowed: unset/null = no filter, true = allowed only, false = blocked only",
            },
        ],
    },
];

/// Verify a CAPTCHA response and mint a short-lived session token.
///
/// The returned token must be supplied as the session-token parameter on
/// subsequent sensitive calls (autocomplete, lookup, home-org,
/// district-of-residence) and on patron registration.
pub fn session_create(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let _worker = app::AddrsWorker::downcast(worker)?;

    let response = method.param(0).str()?;
    let remoteip = method.params().get(1).and_then(|v| v.as_str());

    if !turnstile::verify(response, remoteip)? {
        return Err("CAPTCHA verification failed".into());
    }

    let (token, expires_in) = crate::session::create()?;

    session.respond(eg::hash! {
        "token": token,
        "expires_in": expires_in,
    })?;

    Ok(())
}

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

    let sestoken = method.param(0).str()?;
    crate::session::verify(sestoken)?;
    let search = method.param(1);

    let mut candidates = DEFAULT_LOOKUP_RESULTS;
    if let Some(Some(v)) = method.params().get(2).map(|v| v.as_i64()) {
        candidates = std::cmp::min(v, MAX_LOOKUP_RESULTS);
    }

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
    if let Some(secondary) = search["secondary"].as_str() && !secondary.is_empty() {
        req.secondary = Some(secondary.to_string());
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

    log::info!("lookup() calling smarty with {req:?}");

    let client = SmartyClient::from_env().map_err(|e| {
        log::error!("{e}");
        ADDR_LOOKUP_ERROR
    })?;

    let candidates = client.lookup(&req).map_err(|e| {
        log::error!("Error sending address query: {e}");
        ADDR_LOOKUP_ERROR
    })?;

    for candidate in &candidates {
        // https://www.smarty.com/docs/apis/us-street-api/reference
        let mut is_viable_residential = true;

        let is_viable_mailing = if candidate.metadata.rdi == "Commercial" {
            is_viable_residential = false;
            false
        } else {
            true
        };

        if ["G", "P"].contains(&candidate.metadata.record_type.as_str()) {
            // General Delivery (held at post office) or PO Box.
            is_viable_residential = false;
        }

        if candidate.analysis.dpv_cmra == "Y" {
            // Commercial Mail Receiving Agency
            is_viable_residential = false;
        }

        let mut jv = serde_json::to_value(candidate)
            .map_err(|e| format!("Cannot translate candidate to json value: {e}"))?;

        jv["is_viable_residential"] = is_viable_residential.into();
        jv["is_viable_mailing"] = is_viable_mailing.into();

        if let Some(eh) = &candidate.analysis.enhanced_match
            && eh.contains("postal-match")
            && !eh.contains("unknown-secondary")
            && !eh.contains("missing-secondary") {
            jv["has_valid_secondary"] = true.into();
        }

        log::debug!("Got lookup result: {jv}");

        session.respond(EgValue::from_json_value(jv)?)?;
    }

    Ok(())
}

/// Generate address suggestions from an initial address value.
///
/// # Reference
///
/// * <https://www.smarty.com/docs/apis/us-autocomplete/reference>
pub fn autocomplete(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::AddrsWorker::downcast(worker)?;

    let sestoken = method.param(0).str()?;
    crate::session::verify(sestoken)?;
    let search = method.param(1);

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

    // Optional secondary (unit/apartment) expansion selector: the entry_id
    // of the suggestion to expand (v2).
    if let Some(selected) = search["selected"].as_str() {
        req.selected = Some(selected.to_string());
    }

    // Optional comma-separated list of address types to exclude.
    if let Some(exclude) = search["exclude"].as_str() {
        req.exclude = Some(exclude.to_string());
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
        
        // Fitler any suggestions which represent the office address
        // of multi-unit locations.
        // NOTE: This is hacky and may need some love.
        if search["exclude_ofc"].as_bool().unwrap_or(false) 
            && suggestion.secondary.to_lowercase().contains("ofc") {
            log::info!("Skipping multi-until office address");
            continue;
        }

        let jv = serde_json::to_value(suggestion)
            .map_err(|e| format!("Cannot translate suggestion to json value: {e}"))?;

        suggestions.push(EgValue::from_json_value(jv)?);
    }

    // Append the best-matching local address exception (if any), replacing an
    // API suggestion for the same address.
    let mut editor = Editor::new(worker.client());
    let ex_search = AddressSearch {
        street1: Some(search_str.clone()),
        ..Default::default()
    };
    let ex_result = matching_exceptions(&mut editor, &ex_search, Some(true))?;

    if !ex_result["best"].is_null() {
        let suggestion = exception_to_suggestion(&ex_result["best"]);
        let street_line = suggestion["street_line"].as_str().unwrap_or("");

        suggestions.retain(|s|
            s["street_line"].as_str().unwrap_or("").to_lowercase() != street_line);

        suggestions.push(suggestion);
    }

    suggestions.sort_by_key(|a| a["street_line"].as_str().unwrap_or("").to_string());

    for sug in suggestions.drain(..) {
        session.respond(sug)?;
    }

    Ok(())
}

/// Convert an exception match (as built by `exception_response`) into an
/// autocomplete suggestion.
fn exception_to_suggestion(exc: &EgValue) -> EgValue {
    let street1 = exc["street1"].as_str().unwrap_or("");
    let street2 = exc["street2"].as_str().unwrap_or("");

    let mut street_line = street1.to_string();
    if !street2.is_empty() {
        if !street_line.is_empty() {
            street_line += " ";
        }
        street_line += street2;
    }

    let mut suggestion = eg::hash! {
        "is_exception": true,
        "exception_id": exc["exception_id"].clone(),
        "is_allowed": exc["is_allowed"].clone(),
        "street_line": street_line,
        "secondary": exc["street2"].clone(),
        "city": exc["city"].clone(),
        "state": exc["state"].clone(),
        "zipcode": exc["post_code"].clone(),
        "entries": 0,
    };

    // Allowed exceptions carry the home org / district needed downstream.
    if exc["is_allowed"].boolish() {
        suggestion["home_ou"] = exc["home_ou"].clone();
        suggestion["district_of_residence"] = exc["district_of_residence"].clone();
    }

    suggestion
}

/// Search the local address-exception table (cuae) for rows matching the
/// provided address.  Returns the best match plus any other matches.  The
/// scoring/matching rules live in the exception_match module.
pub fn exception_matches(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::AddrsWorker::downcast(worker)?;

    let sestoken = method.param(0).str()?;
    crate::session::verify(sestoken)?;

    let addr = method.param(1);

    let search = AddressSearch {
        street1: opt_str(&addr["street1"]),
        street2: opt_str(&addr["street2"]),
        city: opt_str(&addr["city"]),
        state: opt_str(&addr["state"]),
        post_code: opt_str(&addr["post_code"]),
    };

    // Optional third param filters on is_allowed: unset/null = no filter,
    // true = allowed only, false = blocked only.
    let allowed = match method.params().get(2) {
        Some(v) if !v.is_null() => Some(v.boolish()),
        _ => None,
    };

    let mut editor = Editor::new(worker.client());

    let result = matching_exceptions(&mut editor, &search, allowed)?;

    session.respond(result)?;

    Ok(())
}

/// Search the cuae table for address-exception rows matching `search` and
/// return an object with the best match and the other matches.  `allowed`
/// filters on is_allowed: None = no filter, Some(true) = allowed only,
/// Some(false) = blocked only.
///
/// This does no session-token verification, so callers that have already
/// verified the token (e.g. autocomplete) can invoke it directly.
pub fn matching_exceptions(
    editor: &mut Editor,
    search: &AddressSearch,
    allowed: Option<bool>,
) -> EgResult<EgValue> {
    let mut query = eg::hash! {"enabled": "t"};
    match allowed {
        Some(true) => query["is_allowed"] = "t".into(),
        Some(false) => query["is_allowed"] = "f".into(),
        None => {}
    }

    let exceptions = editor.search_with_ops(
        "cuae",
        query,
        eg::hash! {
            "flesh": 1,
            "flesh_fields": {"cuae": ["district_of_residence"]},
        },
    )?;

    // Map id -> row and build the pure records passed to the matcher.
    let mut by_id: HashMap<i64, &EgValue> = HashMap::new();
    let mut records: Vec<ExceptionRecord> = Vec::new();

    for exc in &exceptions {
        let id = exc["id"].as_i64().unwrap_or(0);
        by_id.insert(id, exc);
        records.push(ExceptionRecord {
            id,
            street1: opt_str(&exc["street1"]),
            street2: opt_str(&exc["street2"]),
            city: opt_str(&exc["city"]),
            state: opt_str(&exc["state"]),
            post_code: opt_str(&exc["post_code"]),
        });
    }

    let result = crate::exception_match::match_exceptions(search, &records);

    let best = match result.best {
        Some(m) => {
            let exc = *by_id.get(&m.id).ok_or("exception id map error")?;
            exception_response(exc, m.score)?
        }
        None => EgValue::Null,
    };

    let mut others: Vec<EgValue> = Vec::new();
    for m in &result.others {
        let exc = *by_id.get(&m.id).ok_or("exception id map error")?;
        others.push(exception_response(exc, m.score)?);
    }

    Ok(eg::hash! {
        "best": best,
        "matches": others,
    })
}

/// A string field value, or None when null/empty.
fn opt_str(value: &EgValue) -> Option<String> {
    value
        .as_str()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Build the response object for a single matched exception.
fn exception_response(exc: &EgValue, score: i32) -> EgResult<EgValue> {
    let mut resp = eg::hash! {
        "exception_id": exc["id"].clone(),
        "is_allowed": exc["is_allowed"].boolish(),
        "score": score,
        "street1": exc["street1"].clone(),
        "street2": exc["street2"].clone(),
        "city": exc["city"].clone(),
        "state": exc["state"].clone(),
        "post_code": exc["post_code"].clone(),
    };

    // Only allowed exceptions carry the values needed to create an account.
    if exc["is_allowed"].boolish() {
        resp["home_ou"] = exc["home_ou"].clone();
        resp["district_of_residence"] = exc["district_of_residence"]["name"].clone();
    }

    Ok(resp)
}

/// Find the best/closest home library given the provided lat/long values based
/// on predefined shapefiles.
pub fn home_org(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: message::MethodCall,
) -> EgResult<()> {
    let worker = app::AddrsWorker::downcast(worker)?;

    let sestoken = method.param(0).str()?;
    crate::session::verify(sestoken)?;
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

    let sestoken = method.param(0).str()?;
    crate::session::verify(sestoken)?;
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

