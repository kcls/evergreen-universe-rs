//! Minimal synchronous client for the Smarty US address APIs.
//!
//! Replaces the `smarty-rust-sdk`, whose autocomplete support does not
//! expose the `selected` parameter we need for secondary (unit/apartment)
//! expansion.  Only the two endpoints we use are implemented: US
//! Autocomplete Pro and US Street Address.  The field sets are
//! intentionally partial -- add more as needed.
//!
//! All requests are blocking (`reqwest::blocking`), so callers do not need
//! an async runtime.
//!
//! # References
//! * <https://www.smarty.com/docs/apis/us-autocomplete/reference>
//! * <https://www.smarty.com/docs/apis/us-street-api/reference>

use serde::{Deserialize, Serialize};

const AUTOCOMPLETE_URL: &str = "https://us-autocomplete.api.smarty.com/v2/lookup";
const STREET_URL: &str = "https://us-street.api.smarty.com/street-address";

/// Errors surfaced by the Smarty client.
#[derive(Debug)]
pub enum SmartyError {
    /// Missing/invalid configuration (e.g. credentials).
    Config(String),
    /// Network/transport failure talking to Smarty.
    Http(String),
    /// Smarty returned a non-success HTTP status.
    Api { status: u16, body: String },
    /// The response body could not be parsed as expected.
    Parse(String),
}

impl std::fmt::Display for SmartyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmartyError::Config(m) => write!(f, "Smarty config error: {m}"),
            SmartyError::Http(m) => write!(f, "Smarty HTTP error: {m}"),
            SmartyError::Api { status, body } => {
                write!(f, "Smarty API error (status {status}): {body}")
            }
            SmartyError::Parse(m) => write!(f, "Smarty response parse error: {m}"),
        }
    }
}

impl std::error::Error for SmartyError {}

/// A blocking client for the Smarty US address APIs.
pub struct SmartyClient {
    auth_id: String,
    auth_token: String,
    http: reqwest::blocking::Client,
}

impl SmartyClient {
    /// Build a client with explicit secret-key credentials.
    pub fn new(auth_id: String, auth_token: String) -> Result<SmartyClient, SmartyError> {
        let http = reqwest::blocking::Client::builder()
            .build()
            .map_err(|e| SmartyError::Config(e.to_string()))?;

        Ok(SmartyClient { auth_id, auth_token, http })
    }

    /// Build a client from the SMARTY_AUTH_ID / SMARTY_AUTH_TOKEN env vars.
    pub fn from_env() -> Result<SmartyClient, SmartyError> {
        let auth_id = std::env::var("SMARTY_AUTH_ID")
            .map_err(|_| SmartyError::Config("Missing SMARTY_AUTH_ID env var".to_string()))?;

        let auth_token = std::env::var("SMARTY_AUTH_TOKEN")
            .map_err(|_| SmartyError::Config("Missing SMARTY_AUTH_TOKEN env var".to_string()))?;

        SmartyClient::new(auth_id, auth_token)
    }

    /// Suggest matches for a partial address (US Autocomplete Pro).
    pub fn autocomplete(
        &self,
        req: &AutocompleteRequest,
    ) -> Result<Vec<AutocompleteSuggestion>, SmartyError> {
        let mut params: Vec<(&str, String)> = vec![
            ("auth-id", self.auth_id.clone()),
            ("auth-token", self.auth_token.clone()),
            ("search", req.search.clone()),
        ];

        if let Some(max) = req.max_results {
            params.push(("max_results", max.to_string()));
        }

        // Secondary expansion: when set, Smarty returns the individual
        // unit/apartment addresses for the chosen building.
        if let Some(selected) = &req.selected {
            params.push(("selected", selected.clone()));
        }

        // Multi-value filters are semicolon-delimited per the API.
        if !req.include_only_states.is_empty() {
            params.push(("include_only_states", req.include_only_states.join(";")));
        }
        if !req.include_only_zip_codes.is_empty() {
            params.push(("include_only_zip_codes", req.include_only_zip_codes.join(";")));
        }
        if !req.prefer_states.is_empty() {
            params.push(("prefer_states", req.prefer_states.join(";")));
        }

        // Comma-separated list of address types to exclude (e.g.
        // "po-box,commercial").
        if let Some(exclude) = &req.exclude {
            params.push(("exclude", exclude.clone()));
        }

        let response: AutocompleteResponse = self.get_json(AUTOCOMPLETE_URL, &params)?;

        Ok(response.suggestions)
    }

    /// Look up detailed candidate(s) for a specific address (US Street API).
    pub fn lookup(&self, req: &LookupRequest) -> Result<Vec<StreetCandidate>, SmartyError> {
        let mut params: Vec<(&str, String)> = vec![
            ("auth-id", self.auth_id.clone()),
            ("auth-token", self.auth_token.clone()),
            ("street", req.street.clone()),
        ];

        if let Some(v) = &req.secondary {
            params.push(("secondary", v.clone()));
        }
        if let Some(v) = &req.city {
            params.push(("city", v.clone()));
        }
        if let Some(v) = &req.state {
            params.push(("state", v.clone()));
        }
        if let Some(v) = &req.zipcode {
            params.push(("zipcode", v.clone()));
        }
        if let Some(v) = req.candidates {
            params.push(("candidates", v.to_string()));
        }
        if let Some(v) = &req.match_strategy {
            params.push(("match", v.clone()));
        }

        // The US Street API returns a bare JSON array of candidates.
        self.get_json(STREET_URL, &params)
    }

    /// Issue a GET and deserialize the JSON body into `T`.
    fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        params: &[(&str, String)],
    ) -> Result<T, SmartyError> {
        let response = self
            .http
            .get(url)
            .query(params)
            .send()
            .map_err(|e| SmartyError::Http(e.to_string()))?;

        let status = response.status();
        let body = response.text().map_err(|e| SmartyError::Http(e.to_string()))?;

        log::info!("Smarty returned: {body}");

        if !status.is_success() {
            return Err(SmartyError::Api { status: status.as_u16(), body });
        }

        serde_json::from_str(&body).map_err(|e| SmartyError::Parse(format!("{e}; body={body}")))
    }
}

// --- US Autocomplete Pro ----------------------------------------------------

/// Inputs for an autocomplete request.  `search` is required; the rest are
/// optional and omitted from the query when empty/none.
#[derive(Debug, Default)]
pub struct AutocompleteRequest {
    pub search: String,
    pub max_results: Option<u32>,
    /// Secondary expansion selector: the `entry_id` of the suggestion to
    /// expand (v2 API).
    pub selected: Option<String>,
    pub include_only_states: Vec<String>,
    pub include_only_zip_codes: Vec<String>,
    pub prefer_states: Vec<String>,
    /// Comma-separated list of address types to exclude from results
    /// (base-address, commercial, residential, po-box, military).
    pub exclude: Option<String>,
}

/// A single autocomplete suggestion.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AutocompleteSuggestion {
    #[serde(default)]
    pub street_line: String,
    #[serde(default)]
    pub secondary: String,
    #[serde(default)]
    pub city: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub zipcode: String,
    /// Count of secondary (unit/apartment) addresses; > 1 means the
    /// suggestion is an expandable group (see `entry_id`).
    #[serde(default)]
    pub entries: i64,
    /// Identifier of an expandable secondary group (present when entries > 1);
    /// pass it as `AutocompleteRequest::selected` to expand the group (v2).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entry_id: String,
    /// Identifier of a single, non-expandable address (present when the
    /// suggestion is not a group) (v2).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub smarty_key: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutocompleteResponse {
    #[serde(default)]
    suggestions: Vec<AutocompleteSuggestion>,
}

// --- US Street Address ------------------------------------------------------

/// Inputs for a street-address lookup.  `street` is required (it may also
/// carry a full freeform address); the rest are optional.
#[derive(Debug, Default)]
pub struct LookupRequest {
    pub street: String,
    pub secondary: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub zipcode: Option<String>,
    pub candidates: Option<u32>,
    /// Matching strategy: "strict", "invalid", or "enhanced".
    pub match_strategy: Option<String>,
}

/// Parsed address components returned for a candidate.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StreetComponents {
    #[serde(default)]
    pub primary_number: String,
    #[serde(default)]
    pub street_predirection: String,
    #[serde(default)]
    pub street_name: String,
    #[serde(default)]
    pub street_postdirection: String,
    #[serde(default)]
    pub street_suffix: String,
    #[serde(default)]
    pub secondary_designator: String,
    #[serde(default)]
    pub secondary_number: String,
    #[serde(default)]
    pub city_name: String,
    #[serde(default)]
    pub state_abbreviation: String,
    #[serde(default)]
    pub zipcode: String,
    #[serde(default)]
    pub plus4_code: String,
}

/// Candidate metadata, including the geocode we rely on for home-org lookup.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StreetMetadata {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    #[serde(default)]
    pub county_name: String,
    #[serde(default)]
    pub precision: String,
    #[serde(default)]
    pub zip_type: String,
    pub rdi: String,
    pub record_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StreetAnalysis {
	pub dpv_match_code: String,
	pub dpv_footnotes: String,
	pub dpv_cmra: String,
	pub dpv_vacant: String,
	pub dpv_no_stat: String,
	pub active: String,
	pub footnotes: Option<String>,
    pub enhanced_match: Option<String>,
}


/// A single matched candidate for a looked-up address.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StreetCandidate {
    #[serde(default)]
    pub input_index: i64,
    #[serde(default)]
    pub candidate_index: i64,
    #[serde(default)]
    pub delivery_line_1: String,
    #[serde(default)]
    pub last_line: String,
    #[serde(default)]
    pub components: StreetComponents,
    #[serde(default)]
    pub metadata: StreetMetadata,
    pub analysis: StreetAnalysis,
}
