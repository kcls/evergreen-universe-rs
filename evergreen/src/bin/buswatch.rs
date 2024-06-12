//! Watch the message bus for stale messages and apply a TTL value
//! so they may be automatically removed over time.
use eg::osrf::bus;
use eg::osrf::conf;
use eg::EgResult;
use evergreen as eg;
use std::env;
use std::fmt;
use std::thread;
use std::time::Duration;

// If a key exists on the bus for at least DEFAULT_WAIT_TIME seconds,
// apply a time-to-live value of DEFAULT_KEY_EXPIRE_SECS so that it
// may delete itself after it expires.

// The 'watch' account requires permissions: +keys +ttl +expire +llen +lrange

/// How often to wake and scan for keys
const DEFAULT_WAIT_TIME: u64 = 600; // 10 minutes

/// Set the expire time to this many seconds when a stale key is found.
///
/// Redis lists are deleted every time the last value in the list is
/// popped.  If a list key persists, it means the list is never fully
/// drained, suggesting the backend responsible for popping values from
/// the list is no longer alive or is under constant load.  Tell keys to
/// delete themselves after this many seconds of being unable to drain
/// the list.
///
/// Scenarios where a valid message could be deleted under these circumstances:
///
/// 1. Requests are coming into a server at a rate where there
/// is a perpetual backlog for at least DEFAULT_WAIT_TIME +
/// DEFAULT_KEY_EXPIRE_SECS seconds
///
/// 2. A worker/drone receives a request that takes longer than
/// DEFAULT_WAIT_TIME + DEFAULT_KEY_EXPIRE_SECS to process and receives
/// additional requests (from the same client) in the meantime, causing
/// the follow-up requests to linger.
///
const DEFAULT_KEY_EXPIRE_SECS: u64 = 7200; // 2 hours

struct BusWatch {
    bus: bus::Bus,
    wait_time: u64,
    ttl: u64,
    entries: Vec<String>,
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
            entries: Vec::new(),
            ttl: DEFAULT_KEY_EXPIRE_SECS,
        }
    }

    pub fn watch(&mut self) -> EgResult<()> {
        loop {
            for key in self.bus.keys("opensrf:*")?.drain(..) {
                let ttl = self.bus.ttl(&key)?;

                if ttl > -1 {
                    // We only care about keys that don't already have a TTL.
                    continue;
                }

                match self.entries.iter().position(|k| k == &key) {
                    Some(idx) => {
                        // We're already tracking this key, which it means it's
                        // been on the bus for at least self.wait_time seconds.
                        // Give it an expire time.

                        log::warn!("Setting TTL {} for stale key {key}", self.ttl);
                        self.bus.set_key_timeout(&key, self.ttl)?;

                        // Now that it has a timeout, we can stop tracking it.
                        self.entries.remove(idx);

                        // This can fail if the value at key is not a list,
                        // which generally only happens during manual testing.
                        if let Ok(mut list) = self.bus.lrange(&key, 0, 1) {
                            if let Some(value) = list.pop() {
                                log::debug!("Message set to expire: {value}");
                            }
                        }
                    }

                    None => {
                        log::debug!("Tracking new bus key {key}");
                        self.entries.push(key);
                    }
                };
            }

            thread::sleep(Duration::from_secs(self.wait_time));
        }
    }
}

fn main() {
    eg::init().unwrap();
    let config = conf::config();

    log::info!("Starting buswatch at {}", config.client().domain());

    let mut watcher = BusWatch::new();

    if let Ok(v) = env::var("EG_BUSWATCH_TTL") {
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
