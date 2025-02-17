use crate::error::{LocalResult, LocalError};

use std::collections::HashMap;
use std::fs;
use std::sync::OnceLock;
use yaml_rust::Yaml;
use yaml_rust::YamlLoader;

static CONFIG: OnceLock<Config> = OnceLock::new();

const DEFAULT_HOLDINGS_TAG: &str = "852";
const DEFAULT_MAX_BIB_COUNT: u32 = 1000;
const DEFAULT_MAX_ITEM_COUNT: u32 = 1000;

pub fn global() -> &'static Config {
    CONFIG
        .get()
        .unwrap_or_else(|| panic!("No settings have been applied"))
}

/// Entry point for a bibliographic search
#[derive(Debug, Clone, Default)]
pub struct Z39Database {
    is_default: bool,
    name: Option<String>,
    include_holdings: bool,
    default_index: Option<String>,
    bib1_use_map: HashMap<u32, String>,
    max_bib_count: Option<u32>,
    max_item_count: Option<u32>,
    holdings_tag: Option<String>,
    use_elasticsearch: bool,
}

/*
impl Default for Z39Database {
    fn default() -> Self {
        is_default: false,
        name: None
        include_holdings: bool,
        default_index: Option<String>,
        bib1_use_map: HashMap<u32, String>,
        max_bib_count: Option<u32>,
        max_item_count: Option<u32>,
        holdings_tag: Option<String>,
        use_elasticsearch: bool,
    }
}
*/

impl Z39Database {
    pub fn name(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }
    pub fn use_elasticsearch(&self) -> bool {
        self.use_elasticsearch
    }

    /// Set the use_elasticsearch flag.  
    ///
    /// Currently used only in test code.
    #[cfg(test)]
    pub fn set_use_elasticsearch(&mut self, set: bool) {
        self.use_elasticsearch = set;
    }

    pub fn include_holdings(&self) -> bool {
        self.include_holdings
    }

    /// Returns the index name mapped to the bib1 Use attribute numeric
    /// value.
    ///
    /// If no map is found and a default is provided, return that instead.
    pub fn bib1_use_map_index(&self, bib1_value: u32) -> Option<&str> {
        self.bib1_use_map
            .get(&bib1_value)
            .map(|s| s.as_str())
            .or(self.default_index())
    }

    pub fn default_index(&self) -> Option<&str> {
        self.default_index.as_deref()
    }

    pub fn max_bib_count(&self) -> u32 {
        self.max_bib_count.unwrap_or(DEFAULT_MAX_BIB_COUNT)
    }

    pub fn max_item_count(&self) -> u32 {
        self.max_item_count.unwrap_or(DEFAULT_MAX_ITEM_COUNT)
    }

    pub fn holdings_tag(&self) -> &str {
        self.holdings_tag.as_deref().unwrap_or(DEFAULT_HOLDINGS_TAG)
    }
}

/// Z39 configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub filename: String,
    pub bind: String,
    pub max_workers: usize,
    pub min_workers: usize,
    pub min_idle_workers: usize,
    databases: Vec<Z39Database>,
}

impl Config {
    pub fn new(filename: &str) -> Config {
        Config {
            filename: filename.to_string(),
            bind: String::from("localhost:2210"),
            max_workers: 64,
            min_workers: 1,
            min_idle_workers: 1,
            databases: Vec::new(),
        }
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.databases
            .iter()
            .map(|d| d.name())
            .collect::<Vec<&str>>()
    }

    /// Returns the named database, or a default if provided.
    ///
    /// Returns Err if no database can be found.
    pub fn find_database(&self, database_name: Option<&str>) -> LocalResult<&Z39Database> {
        if let Some(name) = database_name {
            if let Some(db) = self.databases.iter().find(|d| d.name() == name) {
                return Ok(db);
            }
        }

        if let Some(db) = self.databases.iter().find(|d| d.is_default) {
            Ok(db)
        } else {
            Err(LocalError::NoSuchDatabase(format!("{}", database_name.unwrap_or(""))))
        }
    }

    pub fn apply(self) {
        if CONFIG.set(self).is_err() {
            panic!("Global Settings already applied");
        }
    }

    /// Parse a YAML configuration file.
    pub fn from_yaml(filename: &str) -> LocalResult<Self> {
        let mut conf = Config::new(filename);

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

        if let Some(v) = root["bind"].as_str() {
            conf.bind = String::from(v);
        };

        if let Some(v) = root["max-workers"].as_i64() {
            conf.max_workers = v as usize;
        }

        if let Some(v) = root["min-workers"].as_i64() {
            conf.min_workers = v as usize;
        }

        if let Some(v) = root["min-idle-workers"].as_i64() {
            conf.min_idle_workers = v as usize;
        }

        let Yaml::Array(ref databases) = root["databases"] else {
            return Ok(conf);
        };

        for db in databases {
            conf.add_database(db)?;
        }

        Ok(conf)
    }

    /// Unpack settings for a single Z39Database in the config.
    fn add_database(&mut self, db: &Yaml) -> LocalResult<()> {
        let name = db["name"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Database name required".to_string())?;

        let max_item_count = db["max-item-count"].as_i64().map(|n| n as u32);
        let max_bib_count = db["max-bib-count"].as_i64().map(|n| n as u32);
        let include_holdings = db["include-holdings"].as_bool().unwrap_or(false);
        let is_default = db["is-default"].as_bool().unwrap_or(false);
        let use_elasticsearch = db["use-elasticsearch"].as_bool().unwrap_or(false);
        let holdings_tag = db["holdings-tag"].as_str().map(|s| s.to_string());

        let default_index = db["default-index"].as_str().map(|s| s.to_string());

        let mut bib1_use_map = HashMap::new();

        if let Yaml::Array(ref maps) = db["bib1-use-map"] {
            for map in maps {
                let attr_num = map["attr"]
                    .as_i64()
                    .ok_or_else(|| format!("Map {map:?} requires an 'attr' value"))?;

                let index = map["index"]
                    .as_str()
                    .ok_or_else(|| format!("Map {map:?} requires an 'index' value"))?;

                bib1_use_map.insert(attr_num as u32, index.to_string());
            }
        }

        let zdb = Z39Database {
            name: Some(name),
            is_default,
            holdings_tag,
            max_item_count,
            max_bib_count,
            include_holdings,
            use_elasticsearch,
            default_index,
            bib1_use_map,
        };

        log::debug!("Adding database {zdb:?}");

        self.databases.push(zdb);

        Ok(())
    }
}
