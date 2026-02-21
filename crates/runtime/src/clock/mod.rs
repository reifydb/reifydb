// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Platform-agnostic clock abstraction.
//!
//! Provides a `Clock` enum that can be either real system time or mock time for testing.
//! The clock is shared across all threads within a runtime instance.
//!
//! - **Native**: Uses system time via the time module
//! - **WASM**: Uses JavaScript's Date.now() via the time module

#[cfg(reifydb_target = "native")]
mod native;
#[cfg(reifydb_target = "wasm")]
mod wasm;

#[cfg(reifydb_target = "native")]
pub use native::{Clock, Instant, MockClock};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{Clock, Instant, MockClock};

#[cfg(test)]
mod tests {
	use std::thread;

	use super::*;

	#[test]
	fn test_real_clock() {
		let clock = Clock::Real;
		let t1 = clock.now_millis();
		// Small busy loop to ensure time passes
		let mut sum = 0;
		for i in 0..10000 {
			sum += i;
		}
		let t2 = clock.now_millis();
		assert!(t2 >= t1, "Time should not go backwards");
		let _ = sum;
	}

	#[test]
	fn test_mock_clock_initial() {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock);

		assert_eq!(clock.now_millis(), 1000);
		assert_eq!(clock.now_micros(), 1_000_000);
		assert_eq!(clock.now_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_mock_clock_set() {
		let mock = MockClock::from_millis(0);
		mock.set_millis(5000);

		assert_eq!(mock.now_millis(), 5000);

		mock.set_micros(6_000_000);
		assert_eq!(mock.now_millis(), 6000);
	}

	#[test]
	fn test_mock_clock_advance() {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());

		assert_eq!(clock.now_millis(), 1000);

		mock.advance_millis(500);
		assert_eq!(clock.now_millis(), 1500);

		mock.advance_micros(500_000);
		assert_eq!(clock.now_millis(), 2000);

		mock.advance_nanos(500_000_000);
		assert_eq!(clock.now_millis(), 2500);
	}

	#[cfg(reifydb_target = "native")]
	#[test]
	fn test_mock_clock_thread_safe() {
		let mock = MockClock::from_millis(1000);
		let mock_clone = mock.clone();

		let handle = thread::spawn(move || {
			mock_clone.advance_millis(500);
			mock_clone.now_millis()
		});

		let result = handle.join().unwrap();
		assert_eq!(result, 1500);
		assert_eq!(mock.now_millis(), 1500);
	}

	#[test]
	fn test_clock_default() {
		let clock = Clock::default();
		match clock {
			Clock::Real => {}
			Clock::Mock(_) => panic!("Default should be Real"),
		}
	}

	#[test]
	fn test_nanosecond_precision() {
		let mock = MockClock::new(1_234_567_890_123_456_789);
		let clock = Clock::Mock(mock);

		assert_eq!(clock.now_nanos(), 1_234_567_890_123_456_789);
		assert_eq!(clock.now_micros(), 1_234_567_890_123_456);
		assert_eq!(clock.now_millis(), 1_234_567_890_123);
		assert_eq!(clock.now_secs(), 1_234_567_890);
	}
}
