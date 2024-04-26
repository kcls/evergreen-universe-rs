use crate::osrf::sclient::HostSettings;
use crate::EgResult;
use crate::EgValue;
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use std::collections::HashMap;
use std::sync::Arc;

const CACHE_PREFIX: &str = "opensrf:cache";
const DEFAULT_CACHE_TYPE: &str = "global";
const DEFAULT_MAX_CACHE_TIME: i64 = 86400;
const DEFAULT_MAX_CACHE_SIZE: i64 = 100000000; // ~100M

/// Append our cache prefix to whatever key the caller provides.
fn to_key(k: &str) -> String {
    format!("{CACHE_PREFIX}:{k}")
}

/* opensrf.xml
 * TODO: this config format is PoC and will likely change.
<redis-cache>
  <host>127.0.0.1</host>
  <port>6379</port>
  <username>cache</username>
  <password>250f29a2-e2dd-4032-99da-1726bc8d7277</password>
  <cache-types>
    <global>
      <max_cache_time>86400</max_cache_time>
    </global>
    <anon>
      <max_cache_time>1800</max_cache_time>
      <max_cache_size>102400</max_cache_size>
    </anon>
  </cache-types>
</redis-cache>
*/

#[derive(Debug)]
pub struct CacheType {
    name: String,
    max_cache_time: i64,
    max_cache_size: i64,
}

impl CacheType {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
    pub fn max_cache_time(&self) -> i64 {
        self.max_cache_time
    }
    pub fn max_cache_size(&self) -> i64 {
        self.max_cache_size
    }
}

pub struct Cache {
    redis: redis::Connection,
    active_type: Option<String>,
    cache_types: HashMap<String, CacheType>,
}

impl Cache {
    pub fn init(host_settings: Arc<HostSettings>) -> EgResult<Self> {
        let config = host_settings.value("redis-cache");

        if config.is_null() {
            return Err(format!("No Cache configuration for redis").into());
        }

        let err = || format!("Invalid cache config");

        let host = config["host"].as_str().ok_or_else(err)?;
        let port = config["port"].as_u16().ok_or_else(err)?;
        let username = config["username"].as_str().ok_or_else(err)?;
        let password = config["password"].as_str().ok_or_else(err)?;

        let con_info = ConnectionInfo {
            addr: ConnectionAddr::Tcp(host.to_string(), port),
            redis: RedisConnectionInfo {
                db: 0,
                username: Some(username.to_string()),
                password: Some(password.to_string()),
            },
        };

        log::info!("Connecting to Redis cache as host={host} port={port} username={username}");

        let redis = redis::Client::open(con_info)
            .or_else(|e| Err(format!("Error opening Redis connection: {e}")))?;

        let redis = redis
            .get_connection()
            .or_else(|e| Err(format!("Error opening Redis connection: {e}")))?;

        let mut cache = Cache {
            redis,
            active_type: None,
            cache_types: HashMap::new(),
        };

        cache.load_types(config);

        Ok(cache)
    }

    pub fn active_cache(&self) -> EgResult<&CacheType> {
        let name = self.active_type.as_deref().unwrap_or(DEFAULT_CACHE_TYPE);
        self.cache_types
            .get(name)
            .ok_or_else(|| format!("No such cache type: {name}").into())
    }

    pub fn set_active_type(&mut self, ctype: &str) -> EgResult<()> {
        if !self.cache_types.contains_key(ctype) {
            Err(format!("No configuration present for cache type: {ctype}").into())
        } else {
            self.active_type = Some(ctype.to_string());
            Ok(())
        }
    }

    fn load_types(&mut self, config: &EgValue) {
        for (ctype, conf) in config["cache-types"].entries() {
            let max_cache_time = conf["max_cache_time"]
                .as_i64()
                .unwrap_or(DEFAULT_MAX_CACHE_TIME);

            let max_cache_size = conf["max_cache_size"]
                .as_i64()
                .unwrap_or(DEFAULT_MAX_CACHE_SIZE);

            let ct = CacheType {
                name: ctype.to_string(),
                max_cache_time,
                max_cache_size,
            };

            log::info!("Adding cache config: {ct:?}");

            self.cache_types.insert(ctype.to_string(), ct);
        }
    }

    /// Retrieve a JSON thing from the cache.
    pub fn get(&mut self, key: &str) -> EgResult<Option<EgValue>> {
        let key = to_key(key);
        let value: String = match self.redis.get(&key) {
            Ok(v) => v,
            Err(e) => match e.kind() {
                // Returns Nil if no value is present for the key.
                redis::ErrorKind::TypeError => {
                    return Ok(None);
                }

                _ => return Err(format!("get({key}) failed: {e}").into()),
            },
        };

        let obj = json::parse(&value).or_else(|e| {
            Err(format!(
                "Cached JSON parse failure on key {key}: {e} [{value}]"
            ))
        })?;

        let v = EgValue::try_from(obj)?;

        Ok(Some(v))
    }

    /// Store a value using the default max timeout for the cache type.
    pub fn set(&mut self, key: &str, value: EgValue) -> EgResult<()> {
        self.set_for(key, value, self.active_cache()?.max_cache_time())
    }

    /// Store a value in the cache for this amount of time
    pub fn set_for(&mut self, key: &str, value: EgValue, timeout: i64) -> EgResult<()> {
        let key = to_key(key);
        let ctype = self.active_cache()?;
        let max_timeout = ctype.max_cache_time();
        let max_size = ctype.max_cache_size();

        let time = if timeout > max_timeout {
            max_timeout
        } else {
            timeout
        };

        let valstr = value.into_json_value().dump();

        if valstr.bytes().count() > max_size as usize {
            return Err(format!("Cache value too large: bytes={}", valstr.bytes().count()).into());
        }

        let res: Result<(), _> = self.redis.set_ex(&key, valstr, time as usize);

        if let Err(err) = res {
            return Err(format!("set_ex({key}) failed: {err}").into());
        }

        log::debug!("Cached {key} for {time} seconds");

        Ok(())
    }

    /// Remove a thing from the cache.
    pub fn del(&mut self, key: &str) -> EgResult<()> {
        let key = to_key(key);
        let res: Result<(), _> = self.redis.del(&key);

        if let Err(err) = res {
            return Err(format!("del({key}) failed: {err}").into());
        }

        Ok(())
    }
}
