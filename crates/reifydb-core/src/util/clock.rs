// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::{SystemTime, UNIX_EPOCH};

/// A trait for getting the current timestamp, allowing for test injection
pub trait Clock: Send + Sync {
    /// Returns the current timestamp as milliseconds since Unix epoch
    fn now_millis(&self) -> u64;
}

/// System clock implementation that uses actual system time
#[derive(Debug, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before Unix epoch")
            .as_millis() as u64
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        SystemClock
    }
}

/// Fixed clock implementation for testing with deterministic timestamps
#[derive(Debug, Clone)]
pub struct FixedClock {
    timestamp_millis: u64,
}

impl FixedClock {
    pub fn new(timestamp_millis: u64) -> Self {
        Self { timestamp_millis }
    }
}

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        self.timestamp_millis
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_clock() {
        let clock = SystemClock;
        let t1 = clock.now_millis();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = clock.now_millis();
        assert!(t2 >= t1 + 10);
    }

    #[test]
    fn test_fixed_clock() {
        let clock = FixedClock::new(1234567890);
        assert_eq!(clock.now_millis(), 1234567890);
        assert_eq!(clock.now_millis(), 1234567890);
    }
}
