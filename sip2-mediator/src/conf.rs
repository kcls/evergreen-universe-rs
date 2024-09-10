use eg::EgResult;
use evergreen as eg;
use std::fs;
use yaml_rust::YamlLoader;

/// SIP configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub sip_address: String,
    pub sip_port: u16,
    pub max_workers: usize,
    pub min_workers: usize,
    pub min_idle_workers: usize,
    pub ascii: bool,
    pub aliveness_account: Option<String>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            sip_address: String::from("localhost"),
            sip_port: 6001,
            max_workers: 64,
            min_workers: 1,
            min_idle_workers: 1,
            ascii: true,
            aliveness_account: None,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    /// Parse a YAML configuration file.
    pub fn from_yaml(filename: &str) -> EgResult<Self> {
        let mut conf = Config::new();

        let yaml_text = match fs::read_to_string(filename) {
            Ok(y) => y,
            Err(e) => return Err(format!("Error reading SIP config: {e}").into()),
        };

        let yaml_docs = match YamlLoader::load_from_str(&yaml_text) {
            Ok(y) => y,
            Err(e) => return Err(format!("Error reading SIP config: {e}").into()),
        };

        let root = match yaml_docs.first() {
            Some(v) => &v["sip2-mediator"],
            None => return Err("Invalid SIP config".into()),
        };

        if let Some(v) = root["sip-address"].as_str() {
            conf.sip_address = String::from(v);
        };

        if let Some(v) = root["sip-port"].as_i64() {
            conf.sip_port = v as u16;
        }

        if let Some(v) = root["max-workers"].as_i64() {
            conf.max_workers = v as usize;
        }

        if let Some(v) = root["min-workers"].as_i64() {
            conf.min_workers = v as usize;
        }

        if let Some(v) = root["min-idle-workers"].as_i64() {
            conf.min_idle_workers = v as usize;
        }

        if let Some(v) = root["ascii"].as_bool() {
            conf.ascii = v;
        }

        conf.aliveness_account = root["aliveness-account"].as_str().map(|s| s.to_string());

        Ok(conf)
    }
}
