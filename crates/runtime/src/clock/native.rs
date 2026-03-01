// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native clock implementation.

use std::{
	fmt,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

#[inline(always)]
fn platform_now_nanos() -> u128 {
	SystemTime::now().duration_since(UNIX_EPOCH).expect("System time is before Unix epoch").as_nanos()
}

/// A clock that provides time - either real system time or mock time for testing.
#[derive(Clone)]
pub enum Clock {
	/// Real system clock - delegates to platform time
	Real,
	/// Mock clock with controllable time
	Mock(MockClock),
}

impl Clock {
	/// Get current time in nanoseconds since Unix epoch
	pub fn now_nanos(&self) -> u128 {
		match self {
			Clock::Real => platform_now_nanos(),
			Clock::Mock(mock) => mock.now_nanos(),
		}
	}

	/// Get current time in microseconds since Unix epoch
	pub fn now_micros(&self) -> u64 {
		(self.now_nanos() / 1_000) as u64
	}

	/// Get current time in milliseconds since Unix epoch
	pub fn now_millis(&self) -> u64 {
		(self.now_nanos() / 1_000_000) as u64
	}

	/// Get current time in seconds since Unix epoch
	pub fn now_secs(&self) -> u64 {
		(self.now_nanos() / 1_000_000_000) as u64
	}

	pub fn instant(&self) -> Instant {
		match self {
			Clock::Real => Instant {
				inner: InstantInner::Real(time::Instant::now()),
			},
			Clock::Mock(mock) => Instant {
				inner: InstantInner::Mock {
					captured_nanos: mock.now_nanos(),
					clock: mock.clone(),
				},
			},
		}
	}
}

impl Default for Clock {
	fn default() -> Self {
		Clock::Real
	}
}

/// Mock clock with atomic time storage for thread-safe access.
#[derive(Clone)]
pub struct MockClock {
	inner: Arc<MockClockInner>,
}

struct MockClockInner {
	// Split u128 into two u64s for atomic access
	time_high: AtomicU64,
	time_low: AtomicU64,
}

impl MockClock {
	/// Create a new mock clock starting at the given nanoseconds
	pub fn new(initial_nanos: u128) -> Self {
		Self {
			inner: Arc::new(MockClockInner {
				time_high: AtomicU64::new((initial_nanos >> 64) as u64),
				time_low: AtomicU64::new(initial_nanos as u64),
			}),
		}
	}

	/// Create a new mock clock starting at the given milliseconds
	pub fn from_millis(millis: u64) -> Self {
		Self::new(millis as u128 * 1_000_000)
	}

	/// Get current time in nanoseconds
	pub fn now_nanos(&self) -> u128 {
		let high = self.inner.time_high.load(Ordering::Acquire) as u128;
		let low = self.inner.time_low.load(Ordering::Acquire) as u128;
		(high << 64) | low
	}

	/// Get current time in microseconds
	pub fn now_micros(&self) -> u64 {
		(self.now_nanos() / 1_000) as u64
	}

	/// Get current time in milliseconds
	pub fn now_millis(&self) -> u64 {
		(self.now_nanos() / 1_000_000) as u64
	}

	/// Get current time in seconds
	pub fn now_secs(&self) -> u64 {
		(self.now_nanos() / 1_000_000_000) as u64
	}

	/// Set time to specific nanoseconds
	pub fn set_nanos(&self, nanos: u128) {
		self.inner.time_high.store((nanos >> 64) as u64, Ordering::Release);
		self.inner.time_low.store(nanos as u64, Ordering::Release);
	}

	/// Set time to specific microseconds
	pub fn set_micros(&self, micros: u64) {
		self.set_nanos(micros as u128 * 1_000);
	}

	/// Set time to specific milliseconds
	pub fn set_millis(&self, millis: u64) {
		self.set_nanos(millis as u128 * 1_000_000);
	}

	/// Advance time by nanoseconds
	pub fn advance_nanos(&self, nanos: u128) {
		self.set_nanos(self.now_nanos() + nanos);
	}

	/// Advance time by microseconds
	pub fn advance_micros(&self, micros: u64) {
		self.advance_nanos(micros as u128 * 1_000);
	}

	/// Advance time by milliseconds
	pub fn advance_millis(&self, millis: u64) {
		self.advance_nanos(millis as u128 * 1_000_000);
	}
}

#[derive(Clone)]
enum InstantInner {
	Real(time::Instant),
	Mock {
		captured_nanos: u128,
		clock: MockClock,
	},
}

#[derive(Clone)]
pub struct Instant {
	inner: InstantInner,
}

impl Instant {
	#[inline]
	pub fn elapsed(&self) -> Duration {
		match &self.inner {
			InstantInner::Real(instant) => instant.elapsed(),
			InstantInner::Mock {
				captured_nanos,
				clock,
			} => {
				let now = clock.now_nanos();
				let elapsed_nanos = now.saturating_sub(*captured_nanos);
				Duration::from_nanos(elapsed_nanos as u64)
			}
		}
	}

	#[inline]
	pub fn duration_since(&self, earlier: Instant) -> Duration {
		match (&self.inner, &earlier.inner) {
			(InstantInner::Real(this), InstantInner::Real(other)) => this.duration_since(*other),
			(
				InstantInner::Mock {
					captured_nanos: this_nanos,
					..
				},
				InstantInner::Mock {
					captured_nanos: other_nanos,
					..
				},
			) => {
				let elapsed = this_nanos.saturating_sub(*other_nanos);
				Duration::from_nanos(elapsed as u64)
			}
			_ => panic!("Cannot compare instants from different clock types"),
		}
	}
}

impl fmt::Debug for Instant {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.inner {
			InstantInner::Real(instant) => f.debug_tuple("Instant::Real").field(instant).finish(),
			InstantInner::Mock {
				captured_nanos,
				..
			} => f.debug_tuple("Instant::Mock").field(captured_nanos).finish(),
		}
	}
}
