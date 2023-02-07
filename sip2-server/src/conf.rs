use std::collections::HashMap;
use std::fs;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone, PartialEq)]
pub enum Msg64HoldDatatype {
    Barcode,
    Title,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Msg64SummaryDatatype {
    Barcode,
    Title,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AvFormat {
    Legacy,
    SwyerA,
    SwyerB,
    ThreeM,
}

impl From<&str> for AvFormat {
    fn from(s: &str) -> AvFormat {
        match s.to_lowercase().as_str() {
            "eg_legacy" => Self::Legacy,
            "swyer_a" => Self::SwyerA,
            "swyer_b" => Self::SwyerB,
            "3m" => Self::ThreeM,
            _ => panic!("Invalid AV Format: {}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldFilter {
    field_code: String,
    replace_with: Option<String>,
}

impl FieldFilter {
    pub fn field_code(&self) -> &str {
        &self.field_code
    }
    pub fn replace_with(&self) -> Option<&str> {
        self.replace_with.as_deref()
    }
}

/// Named collection of SIP session settings.
#[derive(Debug, Clone)]
pub struct SipSettings {
    name: String,
    institution: String,
    due_date_use_sip_date_format: bool,
    patron_status_permit_all: bool,
    patron_status_permit_loans: bool,
    msg64_hold_items_available: bool,
    checkin_holds_as_transits: bool,
    msg64_hold_datatype: Msg64HoldDatatype,
    msg64_summary_datatype: Msg64SummaryDatatype,
    av_format: AvFormat,
    checkout_override_all: bool,
    checkin_override_all: bool,
    checkout_override: Vec<String>,
    checkin_override: Vec<String>,
    field_filters: Vec<FieldFilter>,
}

impl SipSettings {
    pub fn new(name: &str, institution: &str) -> Self {
        SipSettings {
            name: name.to_string(),
            institution: institution.to_string(),
            due_date_use_sip_date_format: true,
            patron_status_permit_all: false,
            patron_status_permit_loans: false,
            msg64_hold_items_available: false,
            checkin_holds_as_transits: false,
            msg64_hold_datatype: Msg64HoldDatatype::Barcode,
            msg64_summary_datatype: Msg64SummaryDatatype::Barcode,
            av_format: AvFormat::ThreeM,
            checkout_override_all: false,
            checkin_override_all: false,
            checkout_override: Vec::new(),
            checkin_override: Vec::new(),
            field_filters: Vec::new(),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn institution(&self) -> &str {
        &self.institution
    }
    /// Use SIP date format instead of ISO8601 format
    pub fn due_date_use_sip_date_format(&self) -> bool {
        self.due_date_use_sip_date_format
    }
    /// If true patrons are only reported as blocked if the account
    /// is expired.  Fines, overdues, etc. are ignored.
    pub fn patron_status_permit_all(&self) -> bool {
        self.patron_status_permit_all
    }
    /// Like patron_status_permit_all, but only relates to checkouts/renewals.
    pub fn patron_status_permit_loans(&self) -> bool {
        self.patron_status_permit_loans
    }
    /// Limit holds list to available holds
    pub fn msg64_hold_items_available(&self) -> bool {
        self.msg64_hold_items_available
    }
    /// Format items as barcodes or titles
    pub fn msg64_summary_datatype(&self) -> &Msg64SummaryDatatype {
        &self.msg64_summary_datatype
    }
    /// Format holds as item barcodes or titles
    pub fn msg64_hold_datatype(&self) -> &Msg64HoldDatatype {
        &self.msg64_hold_datatype
    }
    /// Format for fine items
    pub fn av_format(&self) -> &AvFormat {
        &self.av_format
    }
    pub fn checkin_holds_as_transits(&self) -> bool {
        self.checkin_holds_as_transits
    }
    pub fn checkout_override_all(&self) -> bool {
        self.checkout_override_all
    }
    pub fn checkin_override_all(&self) -> bool {
        self.checkin_override_all
    }
    pub fn checkout_override(&self) -> &Vec<String> {
        &self.checkout_override
    }
    pub fn checkin_override(&self) -> &Vec<String> {
        &self.checkin_override
    }
    pub fn field_filters(&self) -> &Vec<FieldFilter> {
        &self.field_filters
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
    activity_as: Option<String>,
}

impl SipAccount {
    pub fn new(
        settings: &SipSettings,
        sip_username: &str,
        sip_password: &str,
        ils_username: &str,
    ) -> SipAccount {
        SipAccount {
            settings: settings.clone(),
            sip_username: sip_username.to_string(),
            sip_password: sip_password.to_string(),
            ils_username: ils_username.to_string(),
            ils_user_id: None,
            workstation: None,
            activity_as: None,
        }
    }

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
    pub fn set_workstation(&mut self, workstation: Option<&str>) {
        self.workstation = workstation.map(|s| s.to_string());
    }
    pub fn set_ils_user_id(&mut self, id: i64) {
        self.ils_user_id = Some(id)
    }
    pub fn workstation(&self) -> Option<&str> {
        self.workstation.as_deref()
    }
    pub fn activity_as(&self) -> Option<&str> {
        self.activity_as.as_deref()
    }
}

/// Global SIP configuration.
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
    monitor_enabled: bool,
    monitor_address: Option<String>,
    monitor_port: Option<u16>,
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
            monitor_enabled: false,
            monitor_address: None,
            monitor_port: None,
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

        if let Some(v) = root["monitor-enabled"].as_bool() {
            self.monitor_enabled = v;
        }

        if let Some(v) = root["monitor-address"].as_str() {
            self.monitor_address = Some(v.to_string());
        };

        if let Some(v) = root["monitor-port"].as_i64() {
            if let Ok(port) = v.try_into() {
                self.monitor_port = Some(port);
            }
        };

        self.add_setting_groups(root);
        self.add_accounts(root);

        self.source = Some(root.to_owned());
    }

    fn add_setting_groups(&mut self, root: &yaml_rust::Yaml) {
        if !root["setting-groups"].is_array() {
            return;
        }

        for group in root["setting-groups"].as_vec().unwrap() {
            let name = group["name"].as_str().expect("Setting group name required");

            let inst = group["institution"]
                .as_str()
                .expect("Setting group institution required");

            let mut grp = SipSettings::new(name, inst);

            // Local shorthand for pulling a bool value from the yaml
            // node and applying to a setting value.
            let set_bool = |g: &yaml_rust::Yaml, k: &str, f: &mut bool| {
                if let Some(v) = g[k].as_bool() {
                    *f = v;
                }
            };

            set_bool(
                group,
                "due-date-use-sip-date-format",
                &mut grp.due_date_use_sip_date_format,
            );
            set_bool(
                group,
                "patron-status-permit-all",
                &mut grp.patron_status_permit_all,
            );
            set_bool(
                group,
                "patron-status-permit-loans",
                &mut grp.patron_status_permit_loans,
            );
            set_bool(
                group,
                "msg64-hold-items-available",
                &mut grp.msg64_hold_items_available,
            );
            set_bool(
                group,
                "checkin-holds-as-transits",
                &mut grp.checkin_holds_as_transits,
            );
            set_bool(
                group,
                "checkout-override-all",
                &mut grp.checkout_override_all,
            );
            set_bool(group, "checkin-override-all", &mut grp.checkin_override_all);

            if let Some(s) = group["msg64-hold-datatype"].as_str() {
                if s.to_lowercase().starts_with("t") {
                    grp.msg64_hold_datatype = Msg64HoldDatatype::Title;
                }
            }
            if let Some(s) = group["msg64-summary-datatype"].as_str() {
                if s.to_lowercase().starts_with("t") {
                    grp.msg64_summary_datatype = Msg64SummaryDatatype::Title;
                }
            }
            if let Some(s) = group["av-format"].as_str() {
                grp.av_format = s.into();
            }

            if group["checkin-override"].is_array() {
                for ovride in group["checkin-override"].as_vec().unwrap() {
                    if let Some(code) = ovride.as_str() {
                        grp.checkin_override.push(code.to_string());
                    }
                }
            }

            if group["checkout-override"].is_array() {
                for ovride in group["checkout-override"].as_vec().unwrap() {
                    if let Some(code) = ovride.as_str() {
                        grp.checkout_override.push(code.to_string());
                    }
                }
            }

            if group["field-filters"].is_array() {
                for filter in group["field-filters"].as_vec().unwrap() {
                    if let Some(field) = filter["field-code"].as_str() {
                        let mut mfilter = FieldFilter {
                            field_code: field.to_string(),
                            replace_with: None,
                        };

                        if let Some(rw) = filter["replace-with"].as_str() {
                            mfilter.replace_with = Some(rw.to_string());
                        }

                        grp.field_filters.push(mfilter);
                    }
                }
            }

            log::debug!("Adding setting group '{name}'");
            self.setting_groups.insert(name.to_string(), grp);
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

                let mut acct = SipAccount::new(
                    &sgroup,
                    account["sip-username"].as_str().unwrap(),
                    account["sip-password"].as_str().unwrap(),
                    account["ils-username"].as_str().unwrap(),
                );

                if let Some(ws) = account["workstation"].as_str() {
                    acct.workstation = Some(ws.to_string());
                }
                if let Some(ws) = account["activity-as"].as_str() {
                    acct.activity_as = Some(ws.to_string());
                }

                self.accounts.insert(username.to_string(), acct);
            }
        };
    }

    /// Add a SIP account, replacing any existing account with the same sip_username
    pub fn add_account(&mut self, account: &SipAccount) {
        self.accounts
            .insert(account.sip_username().to_string(), account.clone());
    }
    pub fn remove_account(&mut self, sip_username: &str) -> Option<SipAccount> {
        self.accounts.remove(sip_username)
    }
    pub fn accounts(&self) -> Vec<&SipAccount> {
        self.accounts.values().collect()
    }
    pub fn get_settings(&self, name: &str) -> Option<&SipSettings> {
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
    pub fn sc_status_before_login(&self) -> bool {
        self.sc_status_before_login
    }
    pub fn monitor_enabled(&self) -> bool {
        self.monitor_enabled
    }
    pub fn monitor_address(&self) -> Option<&str> {
        self.monitor_address.as_deref()
    }
    pub fn monitor_port(&self) -> Option<u16> {
        self.monitor_port
    }
}
