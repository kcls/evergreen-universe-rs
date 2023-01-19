use std::collections::HashMap;
use std::fs;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone)]
pub struct SipSettings {
    name: String,
    institution: String,
    due_date_use_sip_date_format: bool,
}

impl SipSettings {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn institution(&self) -> &str {
        &self.institution
    }
    pub fn due_date_use_sip_date_format(&self) -> bool {
        self.due_date_use_sip_date_format
    }
}

#[derive(Debug, Clone)]
pub struct SipAccount {
    settings: SipSettings,
    sip_username: String,
    sip_password: String,
    ils_username: String,
    ils_user_id: Option<i64>,
    workstation: Option<String>,
}

impl SipAccount {
    pub fn settings(&self) -> &SipSettings {
        &self.settings
    }
    pub fn sip_username(&self) -> &str {
        &self.sip_username
    }
    pub fn sip_password(&self) -> &str {
        &self.sip_password
    }
    pub fn ils_username(&self) -> &str {
        &self.ils_username
    }
    pub fn ils_user_id(&self) -> Option<i64> {
        self.ils_user_id
    }
    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub sip_address: String,
    pub sip_port: u16,
    pub max_clients: usize,
    pub ascii: bool,
    pub setting_groups: HashMap<String, SipSettings>,
    pub accounts: HashMap<String, SipAccount>,
    pub sc_status_before_login: bool,
    currency: String,
}

impl Config {
    pub fn new() -> Config {
        Config {
            sip_address: String::from("localhost"),
            sip_port: 6001,
            max_clients: 256,
            ascii: true,
            setting_groups: HashMap::new(),
            accounts: HashMap::new(),
            currency: "USD".to_string(),
            sc_status_before_login: false,
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

        let root = &yaml_docs[0];

        if let Some(v) = root["sip-address"].as_str() {
            self.sip_address = String::from(v);
        };

        if let Some(v) = root["sip-port"].as_i64() {
            self.sip_port = v as u16;
        }

        if let Some(v) = root["max-clients"].as_i64() {
            self.max_clients = v as usize;
        }

        if let Some(v) = root["ascii"].as_bool() {
            self.ascii = v;
        }

        if let Some(v) = root["sc-status-before-login"].as_bool() {
            self.sc_status_before_login = v;
        }

        // TODO parse setting groups and accounts

        let grp = SipSettings {
            name: String::from("default"),
            institution: String::from("default"),
            due_date_use_sip_date_format: true,
        };

        // TODO verify settings matches a configured group.
        let acct = SipAccount {
            settings: grp.clone(),
            sip_username: String::from("sip-user"),
            sip_password: String::from("sip-pass"),
            ils_username: String::from("admin"),
            ils_user_id: None,
            workstation: None,
        };

        self.setting_groups.insert(grp.name.to_string(), grp);
        self.accounts.insert(acct.sip_username.to_string(), acct);
    }

    pub fn setting_group(&self, name: &str) -> Option<&SipSettings> {
        self.setting_groups.get(name)
    }
    pub fn get_account(&self, username: &str) -> Option<&SipAccount> {
        self.accounts.get(username)
    }
    pub fn currency(&self) -> &str {
        &self.currency
    }
    pub fn sip_address(&self) -> &str {
        &self.sip_address
    }
    pub fn sip_port(&self) -> u16 {
        self.sip_port
    }
    pub fn max_clients(&self) -> usize {
        self.max_clients
    }
    pub fn ascii(&self) -> bool {
        self.ascii
    }
    pub fn setting_groups(&self) -> &HashMap<String, SipSettings> {
        &self.setting_groups
    }
    pub fn accounts(&self) -> &HashMap<String, SipAccount> {
        &self.accounts
    }
    pub fn sc_status_before_login(&self) -> bool {
        self.sc_status_before_login
    }
}
