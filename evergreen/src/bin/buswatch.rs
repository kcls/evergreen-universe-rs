//! Watch the message bus for stale messages and apply a TTL value
//! so they may be removed.
use eg::date;
use eg::osrf::bus;
use eg::osrf::conf;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::env;
use std::fmt;
use std::thread;
use std::time::Duration;

// The 'watch' account requires permissions:
// +keys +ttl +expire +llen

/// How often we wake and check for stale keys.
const DEFAULT_WAIT_TIME: u64 = 60; // 1 minute

/// Set the expire time to this many seconds when a stale key is found.
///
/// Redis lists are deleted every time the last value in the list is
/// popped.  If a list key persists for many minutes, it means the list
/// is never fully drained, suggesting the backend responsible for
/// popping values from the list is no longer alive or is under constant
/// load.  Tell keys to delete themselves after
/// this many seconds of being unable to drain the list.
const DEFAULT_KEY_EXPIRE_SECS: u64 = 7200; // 2 hours

struct BusWatch {
    bus: bus::Bus,
    wait_time: u64,
    ttl: u64,
}

impl fmt::Display for BusWatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Buswatch {}", conf::config().client().domain())
    }
}

impl BusWatch {
    pub fn new() -> Self {
        let bus = match bus::Bus::new(conf::config().client()) {
            Ok(b) => b,
            Err(e) => panic!("Cannot connect bus: {}", e),
        };

        let wait_time = DEFAULT_WAIT_TIME;

        BusWatch {
            bus,
            wait_time,
            ttl: DEFAULT_KEY_EXPIRE_SECS,
        }
    }

    pub fn watch(&mut self) -> EgResult<()> {
        let mut obj = eg::hash! {};

        loop {
            thread::sleep(Duration::from_secs(self.wait_time));

            // Check all opensrf keys.
            let keys = self.bus.keys("opensrf:*")?;

            if keys.len() == 0 {
                continue;
            }

            obj["stats"] = EgValue::new_object();

            for key in keys.iter() {
                let l = self.bus.llen(key)?;

                // The list may have cleared in the time between the
                // time we called keys() and llen().
                if l > 0 {
                    obj["stats"][key]["count"] = EgValue::from(l);

                    // Uncomment this chunk to see the next
                    // message in the queue for this key as JSON.

                    if let Ok(list) = self.bus.lrange(key, 0, 1) {
                        if let Some(s) = list.get(0) {
                            obj["stats"][key]["next_value"] = EgValue::from(s.as_str());
                        }
                    }
                }

                let ttl = self.bus.ttl(key)?;

                obj["stats"][key]["ttl"] = EgValue::from(ttl);

                if ttl == -1 {
                    log::warn!("Setting TTL {} for stale key {key}", self.ttl);
                    self.bus.set_key_timeout(key, self.ttl)?;
                }
            }

            obj["time"] = EgValue::from(date::epoch_secs_str());

            log::info!("{}", obj.dump());
        }
    }
}

fn main() {
    eg::init().unwrap();
    let config = conf::config();

    log::info!("Starting buswatch at {}", config.client().domain());

    let mut watcher = BusWatch::new();

    if let Ok(v) = env::var("OSRF_BUSWATCH_TTL") {
        if let Ok(v2) = v.parse::<u64>() {
            watcher.ttl = v2;
        }
    }

    loop {
        if let Err(e) = watcher.watch() {
            log::error!("Buswatch failed; restarting: {e}");
        }
    }
}
