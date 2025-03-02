use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    events: HashMap<IpAddr, Vec<Instant>>,
    window: Duration,
    max_per_window: u32,
    ip_addr_whitelist: Option<Vec<IpAddr>>,
}

impl RateLimiter {
    pub fn into_shared(self) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(self))
    }

    pub fn new(
        window: Duration,
        max_per_window: u32,
        ip_addr_whitelist: Option<Vec<IpAddr>>,
    ) -> Self {
        Self {
            window,
            max_per_window,
            ip_addr_whitelist,
            events: HashMap::new(),
        }
    }

    /// Returns true if the event should be processed as usual, i.e. has
    /// not exceeded the rate, false otherwise.
    pub fn track_event(&mut self, addr: &IpAddr) -> bool {
        // A max of zero means unlimited
        if self.max_per_window == 0 {
            return true;
        }

        if let Some(ref wl) = self.ip_addr_whitelist {
            if wl.contains(addr) {
                return true;
            }
        }

        // If it's a newly tracked address, add it and move on.
        if !self.events.contains_key(addr) {
            self.events.insert(*addr, vec![Instant::now()]);
            return true;
        };

        let events = self.remove_old_events(addr).unwrap(); // invariant

        // Track the event, regardless of permissibility, since work was
        // done, plus the caller may choose to honor the request after a pause.
        events.push(Instant::now());

        events.len() <= self.max_per_window as usize
    }

    /// Remove events that occurred outside of the window of time we care about.
    fn remove_old_events(&mut self, addr: &IpAddr) -> Option<&mut Vec<Instant>> {
        if let Some(events) = self.events.get_mut(addr) {
            let before = Instant::now() - self.window;
            events.retain(|e| e > &before);
            Some(events)
        } else {
            None
        }
    }

    /// Cycle through all IPs and remove old events, removing IP
    /// entries for addresses which have no remaining events.
    pub fn sync(&mut self) {
        let addrs: Vec<IpAddr> = self.events.keys().copied().collect();

        for addr in addrs.iter() {
            let is_empty = if let Some(events) = self.remove_old_events(addr) {
                events.is_empty()
            } else {
                false
            };

            if is_empty {
                self.events.remove(addr);
            }
        }
    }
}
