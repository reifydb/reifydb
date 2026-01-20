// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Platform-agnostic time functions.
//!
//! Provides `now_nanos()` and related time functions that work across both
//! native and WASM platforms:
//! - **Native**: Uses `std::time::SystemTime`
//! - **WASM**: Uses JavaScript's `Date.now()` via wasm-bindgen

#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(test)]
mod tests {
	#[cfg(feature = "native")]
	use super::native::*;
	#[cfg(feature = "wasm")]
	use super::wasm::*;

	#[test]
	fn test_time_functions_return_reasonable_values() {
		let nanos = now_nanos();
		let micros = now_micros();
		let millis = now_millis();
		let secs = now_secs();

		// Check that values are in reasonable ranges (after year 2020)
		assert!(secs > 1_600_000_000);
		assert!(millis > 1_600_000_000_000);
		assert!(micros > 1_600_000_000_000_000);
		assert!(nanos > 1_600_000_000_000_000_000);

		// Check consistency between units
		assert_eq!(micros, (nanos / 1_000) as u64);
		assert_eq!(millis, (nanos / 1_000_000) as u64);
		assert_eq!(secs, (nanos / 1_000_000_000) as u64);
	}

	#[test]
	fn test_time_progresses() {
		let t1 = now_millis();
		// Small busy loop to ensure time passes
		let mut sum = 0;
		for i in 0..1000 {
			sum += i;
		}
		let t2 = now_millis();

		// Time should either stay the same or progress forward
		assert!(t2 >= t1, "Time should not go backwards");
		let _ = sum; // Use sum to prevent optimization
	}
}
