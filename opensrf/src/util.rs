use json;
use rand::Rng;
use std::time::Instant;

/// Returns a string of random numbers of the requested length
pub fn random_number(size: usize) -> String {
    let mut rng = rand::thread_rng();
    let num: u64 = rng.gen_range(100_000_000_000..1_000_000_000_000);
    format!("{:0width$}", num, width = size)[0..size].to_string()
}

/// Converts a JSON number or string to an isize if possible
pub fn json_isize(value: &json::JsonValue) -> Option<isize> {
    if let Some(i) = value.as_isize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<isize>() {
            return Some(i2);
        }
    };

    None
}

/// Converts a JSON number or string to an usize if possible
pub fn json_usize(value: &json::JsonValue) -> Option<usize> {
    if let Some(i) = value.as_usize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<usize>() {
            return Some(i2);
        }
    };

    None
}

pub struct Timer {
    /// Duration of this timer in seconds.
    /// Timer is "done" once this many seconds have passed
    /// since start_time.
    duration: i32,

    /// Moment this timer starts.
    start_time: Instant,
}

impl Timer {
    pub fn new(duration: i32) -> Timer {
        Timer {
            duration,
            start_time: Instant::now(),
        }
    }

    pub fn reset(&mut self) {
        self.start_time = Instant::now();
    }

    pub fn remaining(&self) -> i32 {
        self.duration - self.start_time.elapsed().as_secs() as i32
    }

    pub fn done(&self) -> bool {
        self.remaining() <= 0
    }
}
