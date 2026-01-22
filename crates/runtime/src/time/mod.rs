// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Platform-agnostic time functions.
//!
//! Provides `now_nanos()` and related time functions that work across both
//! native and WASM platforms:
//! - **Native**: Uses `std::time::SystemTime`
//! - **WASM**: Uses JavaScript's `Date.now()` via wasm-bindgen

use std::time::Duration;

#[cfg(reifydb_target = "native")]
mod native;
#[cfg(reifydb_target = "wasm")]
mod wasm;

// Re-export time functions for external use
#[cfg(reifydb_target = "native")]
pub use native::{now_micros, now_millis, now_nanos, now_secs};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{now_micros, now_millis, now_nanos, now_secs};

cfg_if::cfg_if! {
	if #[cfg(reifydb_target = "native")] {
		type InstantInner = native::InstantInner;
	} else {
		type InstantInner = wasm::InstantInner;
	}
}

/// Platform-agnostic instant for measuring elapsed time.
#[derive(Clone, Copy, Debug)]
pub struct Instant {
	inner: InstantInner,
}

impl Instant {
	/// Creates an Instant representing the current moment in time.
	#[inline]
	pub fn now() -> Self {
		Self {
			inner: InstantInner::now(),
		}
	}

	/// Returns the amount of time elapsed since this instant.
	#[inline]
	pub fn elapsed(&self) -> Duration {
		self.inner.elapsed()
	}

	/// Returns the amount of time elapsed between two instants.
	#[inline]
	pub fn duration_since(&self, earlier: Instant) -> Duration {
		self.inner.duration_since(earlier.inner)
	}
}

#[cfg(test)]
mod tests {
	#[cfg(reifydb_target = "native")]
	use super::native::*;
	#[cfg(reifydb_target = "wasm")]
	use super::wasm::*;

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
