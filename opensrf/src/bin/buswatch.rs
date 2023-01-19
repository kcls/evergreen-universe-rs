use chrono::{DateTime, Local};
use getopts;
use opensrf::bus;
use opensrf::conf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const DEFAULT_WAIT_TIME_MILLIS: u64 = 5000;

// Redis lists are deleted every time the last value in the list is
// popped.  If a list key persists for many minutes, it means the list
// is never fully drained, suggesting the backend responsible for
// popping values from the list is no longer alive OR is perpetually
// under excessive load.  Tell keys to delete themselves after
// this many seconds of being unable to drain the list.
const DEFAULT_KEY_EXPIRE_SECS: u64 = 1800; // 30 minutes

struct BusWatch {
    domain: String,
    bus: bus::Bus,
    wait_time: u64,
    ttl: u64,
    _start_time: DateTime<Local>,
}

impl BusWatch {
    pub fn new(config: Arc<conf::Config>, domain: &str) -> Self {
        let mut busconf = match config.get_router_conf(domain) {
            Some(rc) => rc.client().clone(),
            None => panic!("No router config for domain {}", domain),
        };

        // We connect using info on our routers, but we want to login
        // with our own credentials from the main config.client()
        // object, which are subject to command-line username/ password
        // overrides.

        busconf.set_username(config.client().username());
        busconf.set_password(config.client().password());

        let bus = match bus::Bus::new(&busconf) {
            Ok(b) => b,
            Err(e) => panic!("Cannot connect bus: {}", e),
        };

        let wait_time = DEFAULT_WAIT_TIME_MILLIS;

        BusWatch {
            bus,
            wait_time,
            ttl: DEFAULT_KEY_EXPIRE_SECS,
            _start_time: Local::now(),
            domain: domain.to_string(),
        }
    }

    /// Returns true if the caller should start over with a new
    /// buswatcher to recover from a potentially temporary bus
    /// connection error.  False if this is a clean shutdown.
    pub fn watch(&mut self) -> bool {
        let mut obj = json::object! {
            "domain": json::from(self.domain.as_str()),
        };

        loop {
            thread::sleep(Duration::from_millis(self.wait_time));

            // Check all opensrf keys.
            let keys = match self.bus.keys("opensrf:*") {
                Ok(k) => k,
                Err(e) => {
                    log::error!("Error in keys() command: {e}");
                    return true;
                }
            };

            if keys.len() == 0 {
                continue;
            }

            obj["stats"] = json::JsonValue::new_object();

            for key in keys.iter() {
                match self.bus.llen(key) {
                    Ok(l) => {
                        // The list may have cleared in the time between the
                        // time we called keys() and llen().
                        if l > 0 {
                            obj["stats"][key]["count"] = json::from(l);
                            /*
                            // Uncomment this chunk to see the next opensrf
                            // message in the queue for this key as JSON.
                            if let Ok(list) = self.bus.lrange(key, 0, 1) {
                                if let Some(s) = list.get(0) {
                                    obj["stats"][key]["next_value"] = json::from(s.as_str());
                                }
                            }
                            */
                        }
                    }
                    Err(e) => {
                        let err = format!("Error reading LLEN list={key} error={e}");
                        log::error!("{err}");
                        return true;
                    }
                }

                match self.bus.ttl(key) {
                    Ok(ttl) => {
                        obj["stats"][key]["ttl"] = json::from(ttl);
                        if ttl == -1 {
                            log::debug!("Setting TTL for stale key {key}");
                            if let Err(e) = self.bus.set_key_timeout(key, self.ttl) {
                                log::error!("Error with set_key_timeout: {e}");
                                return true;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error with ttl: {e}");
                    }
                }
            }

            obj["time"] = json::from(format!("{}", Local::now().format("%FT%T%z")));

            println!("{}", obj.dump());
        }
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optmulti("d", "domain", "Domain", "DOMAIN");
    ops.optopt("", "ttl", "Time to Live", "TTL");

    let (config, params) = opensrf::init::init_with_options(&mut ops).unwrap();
    let config = config.into_shared();

    let mut domains = params.opt_strs("domain");

    if domains.len() == 0 {
        // Watch all routed domains by default.
        domains = config
            .routers()
            .iter()
            .map(|r| r.client().domain().name().to_string())
            .collect();
        if domains.len() == 0 {
            panic!("Watcher requires at least on domain");
        }
    }

    println!("Starting buswatch for domains: {domains:?}");

    let ttl = match params.opt_str("ttl") {
        Some(t) => match t.parse::<u64>() {
            Ok(t2) => Some(t2),
            Err(e) => panic!("Invalid --ttl value: {}", e),
        },
        None => None,
    };

    // A watcher for each domain runs within its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    for domain in domains.iter() {
        let conf = config.clone();
        let domain = domain.clone();

        threads.push(thread::spawn(move || loop {
            let mut watcher = BusWatch::new(conf.clone(), &domain);
            if let Some(t) = ttl { watcher.ttl = t; }
            if watcher.watch() {
                log::error!("Restarting watcher after exit-on-error");
            } else {
                break;
            }
        }));
    }

    // Wait for threads to complete.
    for thread in threads {
        thread.join().ok();
    }
}
