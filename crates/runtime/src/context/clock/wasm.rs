// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp, fmt, ops,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use js_sys::Date;
use web_sys::window;

#[inline(always)]
fn platform_now_nanos() -> u64 {
	let millis = Date::now();
	(millis * 1_000_000.0) as u64
}

fn performance_now_ms() -> f64 {
	window().and_then(|w| w.performance()).map(|p| p.now()).unwrap_or_else(|| Date::now())
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
	pub fn now_nanos(&self) -> u64 {
		match self {
			Clock::Real => platform_now_nanos(),
			Clock::Mock(mock) => mock.now_nanos(),
		}
	}

	/// Get current time in microseconds since Unix epoch
	pub fn now_micros(&self) -> u64 {
		self.now_nanos() / 1_000
	}

	/// Get current time in milliseconds since Unix epoch
	pub fn now_millis(&self) -> u64 {
		self.now_nanos() / 1_000_000
	}

	/// Get current time in seconds since Unix epoch
	pub fn now_secs(&self) -> u64 {
		self.now_nanos() / 1_000_000_000
	}

	pub fn instant(&self) -> Instant {
		match self {
			Clock::Real => Instant {
				inner: InstantInner::Real {
					timestamp_ms: performance_now_ms(),
				},
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
	time_nanos: AtomicU64,
}

impl MockClock {
	/// Create a new mock clock starting at the given nanoseconds
	pub fn new(initial_nanos: u64) -> Self {
		Self {
			inner: Arc::new(MockClockInner {
				time_nanos: AtomicU64::new(initial_nanos),
			}),
		}
	}

	/// Create a new mock clock starting at the given milliseconds
	pub fn from_millis(millis: u64) -> Self {
		Self::new(millis * 1_000_000)
	}

	/// Get current time in nanoseconds
	pub fn now_nanos(&self) -> u64 {
		self.inner.time_nanos.load(Ordering::Acquire)
	}

	/// Get current time in microseconds
	pub fn now_micros(&self) -> u64 {
		self.now_nanos() / 1_000
	}

	/// Get current time in milliseconds
	pub fn now_millis(&self) -> u64 {
		self.now_nanos() / 1_000_000
	}

	/// Get current time in seconds
	pub fn now_secs(&self) -> u64 {
		self.now_nanos() / 1_000_000_000
	}

	/// Set time to specific nanoseconds
	pub fn set_nanos(&self, nanos: u64) {
		self.inner.time_nanos.store(nanos, Ordering::Release);
	}

	/// Set time to specific microseconds
	pub fn set_micros(&self, micros: u64) {
		self.set_nanos(micros * 1_000);
	}

	/// Set time to specific milliseconds
	pub fn set_millis(&self, millis: u64) {
		self.set_nanos(millis * 1_000_000);
	}

	/// Advance time by nanoseconds
	pub fn advance_nanos(&self, nanos: u64) {
		self.set_nanos(self.now_nanos().saturating_add(nanos));
	}

	/// Advance time by microseconds
	pub fn advance_micros(&self, micros: u64) {
		self.advance_nanos(micros * 1_000);
	}

	/// Advance time by milliseconds
	pub fn advance_millis(&self, millis: u64) {
		self.advance_nanos(millis * 1_000_000);
	}
}

#[derive(Clone)]
enum InstantInner {
	Real {
		timestamp_ms: f64,
	},
	Mock {
		captured_nanos: u64,
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
			InstantInner::Real {
				timestamp_ms,
			} => {
				let now = performance_now_ms();
				let elapsed_ms = (now - timestamp_ms).max(0.0);
				let nanos = (elapsed_ms * 1_000_000.0) as u64;
				Duration::from_nanos(nanos)
			}
			InstantInner::Mock {
				captured_nanos,
				clock,
			} => {
				let now = clock.now_nanos();
				let elapsed_nanos = now.saturating_sub(*captured_nanos);
				Duration::from_nanos(elapsed_nanos)
			}
		}
	}

	#[inline]
	pub fn duration_since(&self, earlier: &Instant) -> Duration {
		match (&self.inner, &earlier.inner) {
			(
				InstantInner::Real {
					timestamp_ms: this,
				},
				InstantInner::Real {
					timestamp_ms: other,
				},
			) => {
				let elapsed_ms = (this - other).max(0.0);
				let nanos = (elapsed_ms * 1_000_000.0) as u64;
				Duration::from_nanos(nanos)
			}
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
				Duration::from_nanos(elapsed)
			}
			_ => panic!("Cannot compare instants from different clock types"),
		}
	}
}

impl PartialEq for Instant {
	fn eq(&self, other: &Self) -> bool {
		match (&self.inner, &other.inner) {
			(
				InstantInner::Real {
					timestamp_ms: a,
				},
				InstantInner::Real {
					timestamp_ms: b,
				},
			) => a.to_bits() == b.to_bits(),
			(
				InstantInner::Mock {
					captured_nanos: a,
					..
				},
				InstantInner::Mock {
					captured_nanos: b,
					..
				},
			) => a == b,
			_ => panic!("Cannot compare instants from different clock types"),
		}
	}
}

impl Eq for Instant {}

impl PartialOrd for Instant {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Instant {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		match (&self.inner, &other.inner) {
			(
				InstantInner::Real {
					timestamp_ms: a,
				},
				InstantInner::Real {
					timestamp_ms: b,
				},
			) => a.partial_cmp(b).unwrap_or(cmp::Ordering::Equal),
			(
				InstantInner::Mock {
					captured_nanos: a,
					..
				},
				InstantInner::Mock {
					captured_nanos: b,
					..
				},
			) => a.cmp(b),
			_ => panic!("Cannot compare instants from different clock types"),
		}
	}
}

impl ops::Add<Duration> for Instant {
	type Output = Instant;

	fn add(self, duration: Duration) -> Instant {
		match self.inner {
			InstantInner::Real {
				timestamp_ms,
			} => Instant {
				inner: InstantInner::Real {
					timestamp_ms: timestamp_ms + duration.as_secs_f64() * 1000.0,
				},
			},
			InstantInner::Mock {
				captured_nanos,
				clock,
			} => Instant {
				inner: InstantInner::Mock {
					captured_nanos: captured_nanos.saturating_add(duration.as_nanos() as u64),
					clock,
				},
			},
		}
	}
}

impl ops::Sub for &Instant {
	type Output = Duration;

	fn sub(self, other: &Instant) -> Duration {
		self.duration_since(other)
	}
}

impl fmt::Debug for Instant {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.inner {
			InstantInner::Real {
				timestamp_ms,
			} => f.debug_tuple("Instant::Real").field(timestamp_ms).finish(),
			InstantInner::Mock {
				captured_nanos,
				..
			} => f.debug_tuple("Instant::Mock").field(captured_nanos).finish(),
		}
	}
}
