// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM time implementation using JavaScript Date and performance APIs.

use std::time::Duration;

/// Get current time in nanoseconds since Unix epoch.
///
/// Note: Precision is limited to milliseconds on WASM.
#[inline(always)]
pub fn now_nanos() -> u128 {
	// JavaScript Date.now() returns milliseconds since Unix epoch
	let millis = js_sys::Date::now();
	// Convert to nanoseconds (note: precision limited to milliseconds)
	(millis * 1_000_000.0) as u128
}

/// Get current time in microseconds since Unix epoch.
#[inline(always)]
pub fn now_micros() -> u64 {
	(now_nanos() / 1_000) as u64
}

/// Get current time in milliseconds since Unix epoch.
#[inline(always)]
pub fn now_millis() -> u64 {
	(now_nanos() / 1_000_000) as u64
}

/// Get current time in seconds since Unix epoch.
#[inline(always)]
pub fn now_secs() -> u64 {
	(now_nanos() / 1_000_000_000) as u64
}

/// Platform-agnostic instant for measuring elapsed time.
///
/// WASM implementation uses JavaScript's Date.now() for timing.
#[derive(Clone, Copy, Debug)]
pub struct Instant {
	millis: f64,
}

impl Instant {
	/// Creates an Instant representing the current moment in time.
	#[inline]
	pub fn now() -> Self {
		Self {
			millis: js_sys::Date::now(),
		}
	}

	/// Returns the amount of time elapsed since this instant.
	#[inline]
	pub fn elapsed(&self) -> Duration {
		let now = js_sys::Date::now();
		let elapsed_millis = now - self.millis;
		Duration::from_millis(elapsed_millis.max(0.0) as u64)
	}

	/// Returns the amount of time elapsed between two instants.
	#[inline]
	pub fn duration_since(&self, earlier: Instant) -> Duration {
		let elapsed_millis = self.millis - earlier.millis;
		Duration::from_millis(elapsed_millis.max(0.0) as u64)
	}
}
