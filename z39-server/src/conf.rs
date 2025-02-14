use eg::EgResult;
use evergreen as eg;
use std::collections::HashMap;
use std::fs;
use std::sync::OnceLock;
use yaml_rust::Yaml;
use yaml_rust::YamlLoader;

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn global() -> &'static Config {
    CONFIG
        .get()
        .unwrap_or_else(|| panic!("No settings have been applied"))
}

/// Entry point for a bibliographic search
#[derive(Debug, Clone)]
pub struct Z39Database {
    pub name: String,
    pub include_holdings: bool,
    pub bib1_use_keyword_default: bool,
    /// Maps Bib1 Use attribute numeric values to search indexes.
    pub bib1_use_map: HashMap<u32, String>,
    pub max_item_count: Option<i64>,
    pub holdings_tag: Option<String>,
}

/// Z39 configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub filename: String,
    pub bind: String,
    pub max_workers: usize,
    pub min_workers: usize,
    pub min_idle_workers: usize,
    pub databases: Vec<Z39Database>,
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

    pub fn find_database(&self, name: &str) -> Option<&Z39Database> {
        self.databases.iter().find(|d| d.name.as_str() == name)
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
            let name = db["name"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| "Database name required".to_string())?;

            let max_item_count = db["max-item-count"].as_i64();
            let include_holdings = db["include-holdings"].as_bool().unwrap_or(false);
            let holdings_tag = db["holdings-tag"].as_str().map(|s| s.to_string());

            let bib1_use_keyword_default =
                db["bib1-use-keyword-default"].as_bool().unwrap_or(false);

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
                name,
                holdings_tag,
                max_item_count,
                include_holdings,
                bib1_use_keyword_default,
                bib1_use_map,
            };

            conf.databases.push(zdb);
        }

        Ok(conf)
    }
}
