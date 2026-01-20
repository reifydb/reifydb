// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[cfg(debug_assertions)]
use std::cell::RefCell;

#[cfg(feature = "native")]
use reifydb_runtime::time::native::now_nanos as runtime_now_nanos;
#[cfg(feature = "wasm")]
use reifydb_runtime::time::wasm::now_nanos as runtime_now_nanos;

use reifydb_type::value::datetime::DateTime;

#[cfg(debug_assertions)]
thread_local! {
    // Store as nanoseconds internally for maximum precision
    static MOCK_TIME_NANOS: RefCell<Option<u128>> = RefCell::new(None);
}

/// Get current time in nanoseconds since Unix epoch
/// In release builds, this uses platform-specific time source
/// In debug builds, this checks for mock time override
#[inline(always)]
pub fn now_nanos() -> u128 {
	#[cfg(debug_assertions)]
	{
		if let Some(nanos) = MOCK_TIME_NANOS.with(|c| *c.borrow()) {
			return nanos;
		}
	}

	runtime_now_nanos()
}

/// Get current time in microseconds since Unix epoch
#[inline(always)]
pub fn now_micros() -> u64 {
	(now_nanos() / 1_000) as u64
}

/// Get current time in milliseconds since Unix epoch
#[inline(always)]
pub fn now_millis() -> u64 {
	(now_nanos() / 1_000_000) as u64
}

/// Get current time as DateTime
/// Preserves nanosecond precision when available
#[inline(always)]
pub fn now() -> DateTime {
	DateTime::from_timestamp_nanos(now_nanos())
}

// ============================================================================
// Mock Time Functions (only available in debug builds)
// ============================================================================

/// Set mock time to a specific value in nanoseconds
#[cfg(debug_assertions)]
pub fn mock_time_set_nanos(nanos: u128) {
	MOCK_TIME_NANOS.with(|c| {
		*c.borrow_mut() = Some(nanos);
	});
}

/// Set mock time to a specific value in microseconds
#[cfg(debug_assertions)]
pub fn mock_time_set_micros(micros: u64) {
	mock_time_set_nanos(micros as u128 * 1_000);
}

/// Set mock time to a specific value in milliseconds
#[cfg(debug_assertions)]
pub fn mock_time_set_millis(millis: u64) {
	mock_time_set_nanos(millis as u128 * 1_000_000);
}

/// Set mock time to a specific value in milliseconds (convenience alias)
#[cfg(debug_assertions)]
pub fn mock_time_set(millis: u64) {
	mock_time_set_millis(millis);
}

/// Advance mock time by specified nanoseconds
#[cfg(debug_assertions)]
pub fn mock_time_advance_nanos(nanos: u128) {
	MOCK_TIME_NANOS.with(|c| {
		let mut time = c.borrow_mut();
		let current = time.unwrap_or_else(|| now_nanos());
		*time = Some(current + nanos);
	});
}

/// Advance mock time by specified microseconds
#[cfg(debug_assertions)]
pub fn mock_time_advance_micros(micros: u64) {
	mock_time_advance_nanos(micros as u128 * 1_000);
}

/// Advance mock time by specified milliseconds
#[cfg(debug_assertions)]
pub fn mock_time_advance_millis(millis: u64) {
	mock_time_advance_nanos(millis as u128 * 1_000_000);
}

/// Advance mock time by specified milliseconds (convenience alias)
#[cfg(debug_assertions)]
pub fn mock_time_advance(millis: u64) {
	mock_time_advance_millis(millis);
}

/// Clear mock time override
#[cfg(debug_assertions)]
pub fn mock_time_clear() {
	MOCK_TIME_NANOS.with(|c| {
		*c.borrow_mut() = None;
	});
}

/// Get current mock time value in nanoseconds
#[cfg(debug_assertions)]
pub fn mock_time_get_nanos() -> Option<u128> {
	MOCK_TIME_NANOS.with(|c| *c.borrow())
}

/// Get current mock time value in microseconds
#[cfg(debug_assertions)]
pub fn mock_time_get_micros() -> Option<u64> {
	mock_time_get_nanos().map(|n| (n / 1_000) as u64)
}

/// Get current mock time value in milliseconds
#[cfg(debug_assertions)]
pub fn mock_time_get_millis() -> Option<u64> {
	mock_time_get_nanos().map(|n| (n / 1_000_000) as u64)
}

/// Get current mock time value in milliseconds (convenience alias)
#[cfg(debug_assertions)]
pub fn mock_time_get() -> Option<u64> {
	mock_time_get_millis()
}

/// Check if mock time is currently active
#[cfg(debug_assertions)]
pub fn mock_time_is_active() -> bool {
	MOCK_TIME_NANOS.with(|c| c.borrow().is_some())
}

/// RAII guard for scoped mock time
#[cfg(debug_assertions)]
pub struct MockTimeGuard {
	prev: Option<u128>,
}

#[cfg(debug_assertions)]
impl Drop for MockTimeGuard {
	fn drop(&mut self) {
		MOCK_TIME_NANOS.with(|c| {
			*c.borrow_mut() = self.prev;
		});
	}
}

/// Set mock time with RAII guard that restores previous value
#[cfg(debug_assertions)]
pub fn mock_time_scoped_nanos(nanos: u128) -> MockTimeGuard {
	MOCK_TIME_NANOS.with(|c| {
		let prev = *c.borrow();
		*c.borrow_mut() = Some(nanos);
		MockTimeGuard {
			prev,
		}
	})
}

/// Set mock time in microseconds with RAII guard
#[cfg(debug_assertions)]
pub fn mock_time_scoped_micros(micros: u64) -> MockTimeGuard {
	mock_time_scoped_nanos(micros as u128 * 1_000)
}

/// Set mock time in milliseconds with RAII guard
#[cfg(debug_assertions)]
pub fn mock_time_scoped_millis(millis: u64) -> MockTimeGuard {
	mock_time_scoped_nanos(millis as u128 * 1_000_000)
}

/// Set mock time in milliseconds with RAII guard (convenience alias)
#[cfg(debug_assertions)]
pub fn mock_time_scoped(millis: u64) -> MockTimeGuard {
	mock_time_scoped_millis(millis)
}

/// Run a function with mock time in nanoseconds
#[cfg(debug_assertions)]
pub fn mock_time_with_nanos<T>(nanos: u128, f: impl FnOnce() -> T) -> T {
	let _guard = mock_time_scoped_nanos(nanos);
	f()
}

/// Run a function with mock time in microseconds
#[cfg(debug_assertions)]
pub fn mock_time_with_micros<T>(micros: u64, f: impl FnOnce() -> T) -> T {
	let _guard = mock_time_scoped_micros(micros);
	f()
}

/// Run a function with mock time in milliseconds
#[cfg(debug_assertions)]
pub fn mock_time_with_millis<T>(millis: u64, f: impl FnOnce() -> T) -> T {
	let _guard = mock_time_scoped_millis(millis);
	f()
}

/// Run a function with mock time in milliseconds (convenience alias)
#[cfg(debug_assertions)]
pub fn mock_time_with<T>(millis: u64, f: impl FnOnce() -> T) -> T {
	mock_time_with_millis(millis, f)
}

/// Control handle for advancing mock time within a scope
#[cfg(debug_assertions)]
pub struct MockTimeControl;

#[cfg(debug_assertions)]
impl MockTimeControl {
	pub fn advance_nanos(&self, nanos: u128) {
		mock_time_advance_nanos(nanos);
	}

	pub fn advance_micros(&self, micros: u64) {
		mock_time_advance_micros(micros);
	}

	pub fn advance_millis(&self, millis: u64) {
		mock_time_advance_millis(millis);
	}

	pub fn advance(&self, millis: u64) {
		mock_time_advance_millis(millis);
	}

	pub fn set_nanos(&self, nanos: u128) {
		mock_time_set_nanos(nanos);
	}

	pub fn set_micros(&self, micros: u64) {
		mock_time_set_micros(micros);
	}

	pub fn set_millis(&self, millis: u64) {
		mock_time_set_millis(millis);
	}

	pub fn set(&self, millis: u64) {
		mock_time_set_millis(millis);
	}

	pub fn current_nanos(&self) -> u128 {
		mock_time_get_nanos().expect("Mock time should be active")
	}

	pub fn current_micros(&self) -> u64 {
		mock_time_get_micros().expect("Mock time should be active")
	}

	pub fn current_millis(&self) -> u64 {
		mock_time_get_millis().expect("Mock time should be active")
	}

	pub fn current(&self) -> u64 {
		self.current_millis()
	}
}

/// Run a function with mock time that can be controlled
#[cfg(debug_assertions)]
pub fn mock_time_with_control<T>(initial_millis: u64, f: impl FnOnce(&MockTimeControl) -> T) -> T {
	let _guard = mock_time_scoped_millis(initial_millis);
	let control = MockTimeControl;
	f(&control)
}

#[cfg(test)]
pub mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;

    #[test]
	fn test_system_time() {
		mock_time_clear(); // Ensure no mock time is set

		let t1 = now_millis();
		sleep(Duration::from_millis(10));
		let t2 = now_millis();
		assert!(t2 >= t1 + 10);
	}

	#[test]
	fn test_mock_time_set() {
		mock_time_set(1000);
		assert_eq!(now_millis(), 1000);
		assert!(mock_time_is_active());

		mock_time_clear();
		assert!(!mock_time_is_active());
	}

	#[test]
	fn test_mock_time_advance() {
		mock_time_set(1000);
		assert_eq!(now_millis(), 1000);

		mock_time_advance(500);
		assert_eq!(now_millis(), 1500);

		mock_time_advance(250);
		assert_eq!(now_millis(), 1750);

		mock_time_clear();
	}

	#[test]
	fn test_nanosecond_precision() {
		// Set time with nanosecond precision
		mock_time_set_nanos(1_234_567_890_123_456_789);

		assert_eq!(now_nanos(), 1_234_567_890_123_456_789);
		assert_eq!(now_micros(), 1_234_567_890_123_456);
		assert_eq!(now_millis(), 1_234_567_890_123);

		let dt = now();
		assert_eq!(dt.timestamp(), 1_234_567_890);
		assert_eq!(dt.timestamp_nanos() % 1_000_000_000, 123_456_789);

		mock_time_clear();
	}

	#[test]
	fn test_microsecond_precision() {
		mock_time_set_micros(1_234_567_890_123);

		assert_eq!(now_micros(), 1_234_567_890_123);
		assert_eq!(now_millis(), 1_234_567_890);

		mock_time_advance_micros(500);
		assert_eq!(now_micros(), 1_234_567_890_623);

		mock_time_clear();
	}

	#[test]
	fn test_datetime_conversion() {
		mock_time_set_nanos(1_700_000_000_987_654_321);

		let dt = now();
		assert_eq!(dt.timestamp(), 1_700_000_000);
		assert_eq!(dt.timestamp_nanos() % 1_000_000_000, 987_654_321);
		assert_eq!(dt.timestamp_millis(), 1_700_000_000_987);

		mock_time_clear();
	}

	#[test]
	fn test_mock_time_scoped() {
		assert!(!mock_time_is_active());

		{
			let _guard = mock_time_scoped(2000);
			assert_eq!(now_millis(), 2000);
			assert!(mock_time_is_active());
		}

		assert!(!mock_time_is_active());
	}

	#[test]
	fn test_mock_time_with() {
		let result = mock_time_with(3000, || {
			assert_eq!(now_millis(), 3000);
			"test"
		});

		assert_eq!(result, "test");
		assert!(!mock_time_is_active());
	}

	#[test]
	fn test_mock_time_with_control() {
		mock_time_with_control(1000, |control| {
			assert_eq!(control.current(), 1000);

			control.advance(500);
			assert_eq!(now_millis(), 1500);

			control.set(2000);
			assert_eq!(now_millis(), 2000);

			control.advance_micros(500_000);
			assert_eq!(now_millis(), 2500);

			control.advance_nanos(500_000_000);
			assert_eq!(now_millis(), 3000);
		});

		assert!(!mock_time_is_active());
	}

	#[test]
	fn test_parallel_tests_isolated() {
		use std::thread;

		let handle1 = thread::spawn(|| {
			mock_time_with(1000, || {
				assert_eq!(now_millis(), 1000);
			});
		});

		let handle2 = thread::spawn(|| {
			mock_time_with(2000, || {
				assert_eq!(now_millis(), 2000);
			});
		});

		handle1.join().unwrap();
		handle2.join().unwrap();
	}
}
