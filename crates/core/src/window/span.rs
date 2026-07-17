// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::Debug,
	ops::{Add, Rem, Sub},
};

use reifydb_value::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};
use serde::{Deserialize, Serialize};

use crate::util::memory::HeapSize;

pub trait Slot:
	Copy
	+ Ord
	+ Debug
	+ Add<Self::Duration, Output = Self>
	+ Sub<Self, Output = Self::Duration>
	+ Rem<Self::Duration, Output = Self::Duration>
	+ Sub<Self::Duration, Output = Self>
{
	type Duration: Copy + Ord + Debug + IsZero;

	fn order_key(&self) -> u64;

	fn from_order_key(order_key: u64) -> Self;
}

pub trait IsZero {
	fn is_zero(&self) -> bool;
}

impl IsZero for u64 {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == 0
	}
}

impl IsZero for Duration {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Duration::zero()
	}
}

impl IsZero for DateTime {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == DateTime::default()
	}
}

impl IsZero for Date {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Date::default()
	}
}

impl IsZero for Time {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Time::default()
	}
}

impl Slot for u64 {
	type Duration = u64;

	fn order_key(&self) -> u64 {
		*self
	}

	fn from_order_key(order_key: u64) -> Self {
		order_key
	}
}

impl Slot for DateTime {
	type Duration = Duration;

	fn order_key(&self) -> u64 {
		self.to_nanos()
	}

	fn from_order_key(order_key: u64) -> Self {
		DateTime::from_nanos(order_key)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WindowSpan<T> {
	pub start: T,
	pub end: T,
}

impl<T: HeapSize> HeapSize for WindowSpan<T> {
	fn heap_size(&self) -> usize {
		self.start.heap_size() + self.end.heap_size()
	}
}

impl<T> WindowSpan<T>
where
	T: Slot,
{
	#[inline]
	pub fn for_slot(slot: T, duration: T::Duration) -> Self {
		assert!(!duration.is_zero(), "WindowSpan::for_slot: duration must be > 0");
		let start = slot - (slot % duration);
		Self {
			start,
			end: start + duration,
		}
	}

	#[inline]
	pub fn new(start: T, end: T) -> Self {
		assert!(start < end, "WindowSpan::new: start ({start:?}) must be < end ({end:?})");
		Self {
			start,
			end,
		}
	}

	#[inline]
	pub fn duration(&self) -> T::Duration {
		self.end - self.start
	}

	#[inline]
	pub fn contains(&self, slot: T) -> bool {
		slot >= self.start && slot < self.end
	}

	#[inline]
	pub fn next(&self) -> Self {
		let d = self.duration();
		Self {
			start: self.end,
			end: self.end + d,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn for_slot_aligns_to_duration() {
		assert_eq!(WindowSpan::<u64>::for_slot(123, 60), WindowSpan::new(120u64, 180));
		assert_eq!(WindowSpan::<u64>::for_slot(0, 60), WindowSpan::new(0u64, 60));
		assert_eq!(WindowSpan::<u64>::for_slot(60, 60), WindowSpan::new(60u64, 120));
	}

	#[test]
	fn for_slot_aligns_datetime_to_duration() {
		let coord = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let one_second = Duration::from_seconds(1).unwrap();
		let one_minute = Duration::from_seconds(60).unwrap();

		// A sub-minute (1s) window must stay 1s, not round up to a minute.
		let sec = WindowSpan::for_slot(coord, one_second);
		assert_eq!(sec.start, DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap());
		assert_eq!(sec.end, DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 26).unwrap());
		assert_eq!(sec.duration(), one_second);

		// A 1m window aligns the coord down to the minute boundary.
		let min = WindowSpan::for_slot(coord, one_minute);
		assert_eq!(min.start, DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap());
		assert_eq!(min.end, DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap());
		assert!(min.contains(coord));
		assert!(!min.contains(min.end));
	}

	#[test]
	fn contains_is_half_open() {
		let span = WindowSpan::new(100u64, 200);
		assert!(span.contains(100));
		assert!(span.contains(199));
		assert!(!span.contains(200));
		assert!(!span.contains(99));
	}

	#[test]
	fn boundary_slot_belongs_to_next_window() {
		// The recurring off-by-one bug: an event at exactly window_end
		// must NOT be claimed by the current window. Encoded once, here.
		let cur = WindowSpan::<u64>::for_slot(60, 60);
		let nxt = cur.next();
		assert!(!cur.contains(120));
		assert!(nxt.contains(120));
		assert_eq!(nxt, WindowSpan::new(120u64, 180));
	}

	#[test]
	#[should_panic(expected = "duration must be > 0")]
	fn zero_duration_panics() {
		WindowSpan::<u64>::for_slot(10, 0);
	}

	#[test]
	#[should_panic(expected = "must be <")]
	fn empty_span_panics() {
		WindowSpan::new(100u64, 100);
	}

	/// A toy newtype demonstrating that any well-behaved coordinate works,
	/// not just `u64`. This is what a `Slot` or `DateTime` wrapper would do.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
	struct Tick(u64);

	impl Add<u64> for Tick {
		type Output = Tick;
		fn add(self, rhs: u64) -> Tick {
			Tick(self.0 + rhs)
		}
	}
	impl Sub<Tick> for Tick {
		type Output = u64;
		fn sub(self, rhs: Tick) -> u64 {
			self.0 - rhs.0
		}
	}
	impl Sub<u64> for Tick {
		type Output = Tick;
		fn sub(self, rhs: u64) -> Tick {
			Tick(self.0 - rhs)
		}
	}
	impl Rem<u64> for Tick {
		type Output = u64;
		fn rem(self, rhs: u64) -> u64 {
			self.0 % rhs
		}
	}
	impl Slot for Tick {
		type Duration = u64;

		fn order_key(&self) -> u64 {
			self.0
		}

		fn from_order_key(order_key: u64) -> Self {
			Tick(order_key)
		}
	}

	#[test]
	fn newtype_coord_works() {
		let span = WindowSpan::<Tick>::for_slot(Tick(125), 10);
		assert_eq!(span, WindowSpan::new(Tick(120), Tick(130)));
		assert!(span.contains(Tick(120)));
		assert!(!span.contains(Tick(130)));
	}
}
