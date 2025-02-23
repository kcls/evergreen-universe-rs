use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct AddrLimiter {
    events: HashMap<SocketAddr, Vec<Instant>>,
    window: Duration,
    max_per_window: u32,
}

impl AddrLimiter {
    pub fn into_shared(self) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(self))
    }

    pub fn new(window: Duration, max_per_window: u32) -> Self {
        Self {
            window,
            max_per_window,
            events: HashMap::new(),
        }
    }

    /// Returns true if the current request may continue, false otherwise.
    /// TODO should prob return a LocalResult?
    pub fn event_permitted(&mut self, addr: &SocketAddr) -> bool {
        // A max of zero means unbounded.
        if self.max_per_window == 0 {
            return true;
        }

        let now = Instant::now();

        let Some(events) = self.events.get_mut(addr) else {
            // New IP entry.
            self.events.insert(*addr, vec![now]);
            return true;
        };

        // Destination for retained entries.
        let mut new_events = Vec::new();

        // Start of the window we care about
        let then = now - self.window;

        // Drain and rebuild the events list including only those
        // within the time frame we care about.  This has the necessary
        // side effect of ensuring our events lists do not grow unbound.
        for event_time in events.drain(..) {
            if event_time > then {
                new_events.push(event_time);
            }
        }

        let permitted = if new_events.len() < self.max_per_window as usize {
            new_events.push(now);
            true
        } else {
            // Avoid adding the event if it's not permitted
            false
        };

        // Change the underlying vec our hashtable points to.
        *events = new_events;

        permitted
    }

    pub fn _remove_addr(&mut self, addr: &SocketAddr) {
        self.events.remove(addr);
    }
}
