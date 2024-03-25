use eg::EgResult;
use evergreen as eg;
use std::fs;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone)]
pub struct Config {
    pub sip_address: String,
    pub sip_port: u16,
    pub max_clients: usize,
    pub min_workers: usize,
    pub ascii: bool,
}

impl Config {
    pub fn new() -> Config {
        Config {
            sip_address: String::from("localhost"),
            sip_port: 6001,
            max_clients: 64,
            min_workers: 1,
            ascii: true,
        }
    }

    /// Parse a YAML configuration file.
    ///
    /// Panics if the file is not formatted correctly
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

        let root = &yaml_docs[0]["sip2-mediator"];

        if let Some(v) = root["sip-address"].as_str() {
            conf.sip_address = String::from(v);
        };

        if let Some(v) = root["sip-port"].as_i64() {
            conf.sip_port = v as u16;
        }

        if let Some(v) = root["max-clients"].as_i64() {
            conf.max_clients = v as usize;
        }

        if let Some(v) = root["min-workers"].as_i64() {
            conf.min_workers = v as usize;
        }

        if let Some(v) = root["ascii"].as_bool() {
            conf.ascii = v;
        }

        Ok(conf)
    }
}
