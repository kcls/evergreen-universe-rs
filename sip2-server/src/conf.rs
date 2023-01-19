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
    sip_address: String,
    sip_port: u16,
    max_clients: usize,
    ascii: bool,
    setting_groups: HashMap<String, SipSettings>,
    accounts: HashMap<String, SipAccount>,
    sc_status_before_login: bool,
    currency: String,
    source: Option<yaml_rust::Yaml>,
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
            source: None,
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

        self.add_setting_groups(root);
        self.add_accounts(root);

        self.source = Some(root.to_owned());
    }

    fn add_setting_groups(&mut self, root: &yaml_rust::Yaml) {

        if root["setting-groups"].is_array() {
            for group in root["setting-groups"].as_vec().unwrap() {

                let name = group["name"].as_str().unwrap();
                let mut grp = SipSettings {
                    name: name.to_string(),
                    institution: group["institution"].as_str().unwrap().to_string(),
                    due_date_use_sip_date_format: true
                };

                if let Some(b) = group["due-date-use-sip-date-format"].as_bool() {
                    grp.due_date_use_sip_date_format = b;
                }

                log::debug!("Adding setting group '{name}'");
                self.setting_groups.insert(name.to_string(), grp);
            }
        }
    }

    fn add_accounts(&mut self, root: &yaml_rust::Yaml) {

        if root["accounts"].is_array() {
            for account in root["accounts"].as_vec().unwrap() {

                let group_name = account["settings"].as_str().unwrap();
                let sgroup = match self.setting_groups.get(group_name) {
                    Some(s) => s,
                    None => panic!("No such settings group: '{}'", group_name),
                };

                let username = account["sip-username"].as_str().unwrap();

                let mut acct = SipAccount {
                    settings: sgroup.clone(),
                    sip_username: username.to_string(),
                    sip_password: account["sip-password"].as_str().unwrap().to_string(),
                    ils_username: account["ils-username"].as_str().unwrap().to_string(),
                    ils_user_id: None,
                    workstation: None,
                };

                if let Some(ws) = account["workstation"].as_str() {
                    acct.workstation = Some(ws.to_string());
                }

                self.accounts.insert(username.to_string(), acct);
            }
        };
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
