use std::fs;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone)]
pub struct Config {
    pub sip_address: String,
    pub sip_port: u16,
    pub http_url: String,
    pub syslog_facility: String,
    pub syslog_level: String,
    pub max_clients: usize,
    pub ascii: bool,
    pub ignore_ssl_errors: bool,
}

impl Config {
    pub fn new() -> Config {
        Config {
            sip_address: String::from("localhost"),
            sip_port: 6001,
            http_url: String::from("http://localhost/sip2-mediator"),
            syslog_facility: String::from("LOCAL0"),
            syslog_level: String::from("INFO"),
            max_clients: 64,
            ascii: true,
            ignore_ssl_errors: false,
        }
    }

    /// Parse a YAML configuration file.
    ///
    /// Panics if the file is not formatted correctly
    pub fn read_yaml(&mut self, filename: &str) {
        let yaml_text =
            fs::read_to_string(filename).expect("Read YAML configuration file to string");

        let yaml_docs =
            YamlLoader::load_from_str(&yaml_text).expect("Parsing configuration file as YAML");

        let root = &yaml_docs[0]["sip2-mediator"];

        if let Some(v) = root["sip-address"].as_str() {
            self.sip_address = String::from(v);
        };

        if let Some(v) = root["sip-port"].as_i64() {
            self.sip_port = v as u16;
        }

        if let Some(v) = root["http-url"].as_str() {
            self.http_url = String::from(v);
        };

        if let Some(v) = root["syslog-facility"].as_str() {
            self.syslog_facility = String::from(v);
        };

        if let Some(v) = root["syslog-level"].as_str() {
            self.syslog_level = String::from(v);
        };

        if let Some(v) = root["max-clients"].as_i64() {
            self.max_clients = v as usize;
        }

        if let Some(v) = root["ascii"].as_bool() {
            self.ascii = v;
        }

        if let Some(v) = root["ignore-ssl-errors"].as_bool() {
            self.ignore_ssl_errors = v;
        }
    }
}
