use eg::EgResult;
use evergreen as eg;
use std::fs;
use yaml_rust::YamlLoader;

/// Z39 configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub max_workers: usize,
    pub min_workers: usize,
    pub min_idle_workers: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            host: String::from("localhost"),
            port: 2210,
            max_workers: 64,
            min_workers: 1,
            min_idle_workers: 1,
            ascii: true,
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
            Err(e) => return Err(format!("Error reading Z39 config: {e}").into()),
        };

        let yaml_docs = match YamlLoader::load_from_str(&yaml_text) {
            Ok(y) => y,
            Err(e) => return Err(format!("Error reading Z39 config: {e}").into()),
        };

        let root = match yaml_docs.first() {
            Some(v) => &v["z39-server"],
            None => return Err("Invalid Z39 config".into()),
        };

        if let Some(v) = root["host"].as_str() {
            conf.host = String::from(v);
        };

        if let Some(v) = root["port"].as_i64() {
            conf.port = v as u16;
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

        Ok(conf)
    }
}
