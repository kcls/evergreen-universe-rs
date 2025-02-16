use eg::EgResult;
use evergreen as eg;
use std::collections::HashMap;
use std::fs;
use std::sync::OnceLock;
use yaml_rust::Yaml;
use yaml_rust::YamlLoader;

static CONFIG: OnceLock<Config> = OnceLock::new();

const DEFAULT_HOLDINGS_TAG: &str = "852";
const DEFAULT_MAX_BIB_COUNT: u32 = 1000;
const DEFAULT_MAX_ITEM_COUNT: u32 = 1000;

/// Default Bib1 Use attribute maps
const BIB1_ATTR_QUERY_MAP: &[(u32, &str)] = &[
    (4, "title"),
    (7, "identifier|isbn"),
    (8, "keyword"),
    (21, "subject"),
    (1003, "author"),
    (1007, "identifier"),
    (1018, "keyword|publisher"),
];

pub fn global() -> &'static Config {
    CONFIG
        .get()
        .unwrap_or_else(|| panic!("No settings have been applied"))
}

/// Entry point for a bibliographic search
#[derive(Debug, Clone)]
pub struct Z39Database {
    name: Option<String>,
    include_holdings: bool,
    // TODO allow for a default index name instead of just a true/false
    bib1_use_keyword_default: bool,
    bib1_use_map: Option<HashMap<u32, String>>,
    max_bib_count: Option<u32>,
    max_item_count: Option<u32>,
    holdings_tag: Option<String>,
    use_elasticsearch: bool,
}

impl Default for Z39Database {
    fn default() -> Self {
        Self {
            name: None,
            include_holdings: false,
            bib1_use_keyword_default: true,
            bib1_use_map: None,
            max_bib_count: None,
            max_item_count: None,
            holdings_tag: None,
            use_elasticsearch: false,
        }
    }
}

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
    /// Returns None if no mapping exists and self.bib1_use_keyword_default is false.
    pub fn bib1_use_map_index(&self, bib1_value: u32) -> Option<&str> {
        if let Some(ref map) = self.bib1_use_map {
            let index = map.get(&bib1_value);
            if index.is_none() && self.bib1_use_keyword_default {
                Some("keyword")
            } else {
                index.map(|i| i.as_str())
            }
        } else {
            BIB1_ATTR_QUERY_MAP
                .iter()
                .find(|(attr, _)| *attr == bib1_value)
                .map(|(_, index)| *index)
        }
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

    // TODO config file
    /// If no database name is provided OR the provided database is
    /// not found, use the default.
    pub use_default_database: bool,

    default_database: Z39Database,
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
            use_default_database: true,
            databases: Vec::new(),
            default_database: Z39Database::default(),
        }
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.databases
            .iter()
            .map(|d| d.name())
            .collect::<Vec<&str>>()
    }

    /// Returns Err if the provided database is not found and this
    /// server instance does not support failling back to the default.
    pub fn find_database(&self, database_name: Option<&str>) -> EgResult<&Z39Database> {
        let mut db = None;
        if let Some(name) = database_name {
            db = self.databases.iter().find(|d| d.name() == name);
        }

        if let Some(d) = db {
            Ok(d)
        } else if self.use_default_database {
            Ok(&self.default_database)
        } else {
            Err(format!("No such database: '{}'", database_name.unwrap_or("")).into())
        }
    }

    pub fn apply(self) {
        if CONFIG.set(self).is_err() {
            panic!("Global Settings already applied");
        }
    }

    /// Parse a YAML configuration file.
    pub fn from_yaml(filename: &str) -> EgResult<Self> {
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
    fn add_database(&mut self, db: &Yaml) -> EgResult<()> {
        let name = db["name"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Database name required".to_string())?;

        let max_item_count = db["max-item-count"].as_i64().map(|n| n as u32);
        let max_bib_count = db["max-bib-count"].as_i64().map(|n| n as u32);
        let include_holdings = db["include-holdings"].as_bool().unwrap_or(false);
        let use_elasticsearch = db["use-elasticsearch"].as_bool().unwrap_or(false);
        let holdings_tag = db["holdings-tag"].as_str().map(|s| s.to_string());

        let bib1_use_keyword_default = db["bib1-use-keyword-default"].as_bool().unwrap_or(false);

        let mut bib1_use_map = None;

        if let Yaml::Array(ref maps) = db["bib1-use-map"] {
            let mut hashmap = HashMap::new();

            for map in maps {
                let attr_num = map["attr"]
                    .as_i64()
                    .ok_or_else(|| format!("Map {map:?} requires an 'attr' value"))?;

                let index = map["index"]
                    .as_str()
                    .ok_or_else(|| format!("Map {map:?} requires an 'index' value"))?;

                hashmap.insert(attr_num as u32, index.to_string());
            }

            bib1_use_map = Some(hashmap);
        }

        let zdb = Z39Database {
            name: Some(name),
            holdings_tag,
            max_item_count,
            max_bib_count,
            include_holdings,
            use_elasticsearch,
            bib1_use_keyword_default,
            bib1_use_map,
        };

        log::debug!("Adding database {zdb:?}");

        self.databases.push(zdb);

        Ok(())
    }
}
