// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(any(reifydb_target = "host", reifydb_dst))]
mod host;
#[cfg(reifydb_target = "wasi")]
mod wasi;
#[cfg(reifydb_target = "wasm")]
mod wasm;

#[cfg(any(reifydb_target = "host", reifydb_dst))]
pub use host::{Clock, Instant, MockClock};
use reifydb_value::{clock::ClockNow, value::datetime::DateTime};
#[cfg(reifydb_target = "wasi")]
pub use wasi::{Clock, Instant, MockClock};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{Clock, Instant, MockClock};

impl ClockNow for Clock {
	fn now(&self) -> DateTime {
		Clock::now(self)
	}
}

#[cfg(test)]
mod tests {
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	use std::thread;

	use super::*;

	#[test]
	fn test_real_clock() {
		let clock = Clock::Real;
		let t1 = clock.now().to_millis();
		// Busy work rather than a sleep; the assertion only requires the clock not to go
		// backwards, so it holds whether or not a millisecond actually elapsed.
		let mut sum = 0;
		for i in 0..10000 {
			sum += i;
		}
		let t2 = clock.now().to_millis();
		assert!(t2 >= t1, "Time should not go backwards");
		let _ = sum;
	}

	#[test]
	fn test_mock_clock_initial() {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock);

		assert_eq!(clock.now().to_millis(), 1000);
		assert_eq!(clock.now().to_micros(), 1_000_000);
		assert_eq!(clock.now().to_nanos(), 1_000_000_000);
	}

	#[test]
	fn test_mock_clock_set() {
		let mock = MockClock::from_millis(0);
		mock.set_millis(5000);

		assert_eq!(mock.now().to_millis(), 5000);

		mock.set_micros(6_000_000);
		assert_eq!(mock.now().to_millis(), 6000);
	}

	#[test]
	fn test_mock_clock_advance() {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());

		assert_eq!(clock.now().to_millis(), 1000);

		mock.advance_millis(500);
		assert_eq!(clock.now().to_millis(), 1500);

		mock.advance_micros(500_000);
		assert_eq!(clock.now().to_millis(), 2000);

		mock.advance_nanos(500_000_000);
		assert_eq!(clock.now().to_millis(), 2500);
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	#[test]
	fn test_mock_clock_thread_safe() {
		let mock = MockClock::from_millis(1000);
		let mock_clone = mock.clone();

		let handle = thread::spawn(move || {
			mock_clone.advance_millis(500);
			mock_clone.now().to_millis()
		});

		let result = handle.join().unwrap();
		assert_eq!(result, 1500);
		assert_eq!(mock.now().to_millis(), 1500);
	}

	#[test]
	fn test_nanosecond_precision() {
		let mock = MockClock::new(1_234_567_890_123_456_789);
		let clock = Clock::Mock(mock);

		assert_eq!(clock.now().to_nanos(), 1_234_567_890_123_456_789);
		assert_eq!(clock.now().to_micros(), 1_234_567_890_123_456);
		assert_eq!(clock.now().to_millis(), 1_234_567_890_123);
		assert_eq!(clock.now().to_epoch_secs(), 1_234_567_890);
	}
}
