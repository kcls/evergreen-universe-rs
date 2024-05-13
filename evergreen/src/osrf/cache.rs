use crate::osrf::sclient::HostSettings;
use crate::EgResult;
use crate::EgValue;
use memcache;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

thread_local! {
    static CACHE_CONNECTIONS: RefCell<HashMap<String, CacheConnection>> = RefCell::new(HashMap::new());
}

const DEFAULT_MAX_CACHE_TIME: u32 = 86400;
const DEFAULT_MAX_CACHE_SIZE: u32 = 100000000; // ~100M
const GLOBAL_CACHE_NAME: &str = "global";
const ANON_CACHE_NAME: &str = "anon";

/*
<cache>
  <global>
    <servers>
      <server>127.0.0.1:11211</server>
    </servers>
    <max_cache_time>86400</max_cache_time>
  </global>
  <anon>
    <servers>
      <server>127.0.0.1:11211</server>
    </servers>
    <max_cache_time>1800</max_cache_time>
    <max_cache_size>102400</max_cache_size>
  </anon>
</cache>
*/

pub struct CacheConnection {
    name: String,
    memcache: memcache::Client,
    max_cache_time: u32,
    max_cache_size: u32,
}

impl fmt::Display for CacheConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cache name={}", self.name)
    }
}

impl CacheConnection {
    /// Store a value in the cache with the provided timeout.
    ///
    /// If the timeout is 0, the default timeout for the connection type is used.
    fn set(&self, key: &str, value: EgValue, mut timeout: u32) -> EgResult<()> {
        let value = value.into_json_value().dump();
        let byte_count = value.as_bytes().len();

        log::debug!("{self} caching {byte_count} bytes at key={key}");

        if byte_count > self.max_cache_size as usize {
            return Err(format!(
                "{self} key={key} exceeds the max size of {}",
                self.max_cache_size
            )
            .into());
        }

        if timeout == 0 {
            timeout = self.max_cache_time;
        }

        self.memcache
            .set(key, &value, timeout)
            .map_err(|e| format!("{self} set key={key} failed: {e}").into())
    }

    fn get(&self, key: &str) -> EgResult<Option<EgValue>> {
        let result: Option<String> = match self.memcache.get(key) {
            Ok(r) => r,
            Err(e) => return Err(format!("{self} get key={key} failed: {e}").into()),
        };

        if let Some(value) = result {
            let obj = json::parse(&value).or_else(|e| {
                Err(format!(
                    "Cached JSON parse failure on key {key}: {e} [{value}]"
                ))
            })?;

            let v = EgValue::try_from(obj)?;

            return Ok(Some(v));
        }

        Ok(None)
    }

    fn del(&self, key: &str) -> EgResult<()> {
        self.memcache
            .delete(key)
            .map(|_| ())
            .map_err(|e| format!("{self} del key={key} failed: {e}").into())
    }
}

pub struct Cache;

impl Cache {
    /// Returns OK if the specified cache type has been initialized, Err otherwise.
    fn verify_cache(cache_name: &str) -> EgResult<()> {
        let mut has = false;
        CACHE_CONNECTIONS.with(|cc| has = cc.borrow().contains_key(cache_name));
        if has {
            Ok(())
        } else {
            Err(format!("No such cache initialized: {cache_name}").into())
        }
    }

    pub fn init_cache(cache_name: &str) -> EgResult<()> {
        if Cache::verify_cache(cache_name).is_ok() {
            log::warn!("Cache {cache_name} is already connected; ignoring");
            return Ok(());
        }

        let conf_key = format!("cache/{}", cache_name);
        let config = HostSettings::get(&conf_key)?;

        let mut servers = Vec::new();
        if let Some(server) = config["servers"]["server"].as_str() {
            servers.push(format!("memcache://{server}"));
        } else {
            for server in config["servers"]["server"].members() {
                servers.push(format!("memcache://{server}"));
            }
        }

        let cache_time = config["max_cache_time"]
            .as_int()
            .map(|n| n as u32)
            .unwrap_or(DEFAULT_MAX_CACHE_TIME);

        let cache_size = config["max_cache_size"]
            .as_int()
            .map(|n| n as u32)
            .unwrap_or(DEFAULT_MAX_CACHE_SIZE);

        log::info!("Connecting to cache servers: {servers:?}");

        let mc = match memcache::connect(servers) {
            Ok(mc) => mc,
            Err(e) => {
                return Err(format!(
                    "Cannot connect to memcache with config: {} : {e}",
                    config.clone().into_json_value().dump()
                )
                .into());
            }
        };

        let cache = CacheConnection {
            name: GLOBAL_CACHE_NAME.to_string(),
            memcache: mc,
            max_cache_time: cache_time,
            max_cache_size: cache_size,
        };

        CACHE_CONNECTIONS.with(|c| c.borrow_mut().insert(GLOBAL_CACHE_NAME.to_string(), cache));

        Ok(())
    }

    /// Remove a thing from the cache.
    pub fn del_from(cache_name: &str, key: &str) -> EgResult<()> {
        Cache::verify_cache(cache_name)?;

        let mut result = Ok(());
        CACHE_CONNECTIONS.with(|c| result = c.borrow().get(cache_name).unwrap().del(key));
        result
    }

    /// Shortcut to remove a value from the "global" cache
    pub fn del_global(key: &str) -> EgResult<()> {
        Cache::del_from(GLOBAL_CACHE_NAME, key)
    }

    /// Shortcut to remove a value from the "anon" cache
    pub fn del_anon(key: &str) -> EgResult<()> {
        Cache::del_from(ANON_CACHE_NAME, key)
    }

    pub fn get(cache_name: &str, key: &str) -> EgResult<Option<EgValue>> {
        Cache::verify_cache(cache_name)?;
        let mut result = Ok(None);
        CACHE_CONNECTIONS.with(|c| result = c.borrow().get(cache_name).unwrap().get(key));
        result
    }

    /// Shortcut to return a value from the "global" cache
    pub fn get_global(key: &str) -> EgResult<Option<EgValue>> {
        Cache::get(GLOBAL_CACHE_NAME, key)
    }

    /// Shortcut to return a value from the "anon" cache
    pub fn get_anon(key: &str) -> EgResult<Option<EgValue>> {
        Cache::get(ANON_CACHE_NAME, key)
    }

    /// Store a value using the specified cache.
    pub fn set(cache_name: &str, key: &str, value: EgValue, timeout: u32) -> EgResult<()> {
        Cache::verify_cache(cache_name)?;

        let mut result = Ok(());
        CACHE_CONNECTIONS
            .with(|c| result = c.borrow().get(cache_name).unwrap().set(key, value, timeout));
        result
    }

    /// Shortcut for storing a value in the "global" cache with the
    /// default timeout.
    pub fn set_global(key: &str, value: EgValue) -> EgResult<()> {
        Cache::set(GLOBAL_CACHE_NAME, key, value, 0)
    }

    /// Shortcut for storing a value in the "global" cache with the
    /// provided timeout.
    pub fn set_global_for(key: &str, value: EgValue, timeout: u32) -> EgResult<()> {
        Cache::set(GLOBAL_CACHE_NAME, key, value, timeout)
    }

    /// Shortcut for storing a value in the "anon" cache with the
    /// default timeout.
    pub fn set_anon(key: &str, value: EgValue) -> EgResult<()> {
        Cache::set(ANON_CACHE_NAME, key, value, 0)
    }

    /// Shortcut for storing a value in the "anon" cache with the
    /// provided timeout.
    pub fn set_anon_for(key: &str, value: EgValue, timeout: u32) -> EgResult<()> {
        Cache::set(ANON_CACHE_NAME, key, value, timeout)
    }
}
