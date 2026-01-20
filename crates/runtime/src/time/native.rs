// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native time implementation using std::time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Get current time in nanoseconds since Unix epoch.
#[inline(always)]
pub fn now_nanos() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("System time is before Unix epoch")
		.as_nanos()
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
/// Native implementation uses std::time::Instant.
#[derive(Clone, Copy, Debug)]
pub struct Instant {
	inner: std::time::Instant,
}

impl Instant {
	/// Creates an Instant representing the current moment in time.
	#[inline]
	pub fn now() -> Self {
		Self {
			inner: std::time::Instant::now(),
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
