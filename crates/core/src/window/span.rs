// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::Debug,
	ops::{Add, Rem, Sub},
};

use reifydb_codec::state::ArchiveState;
use reifydb_macro::operator_state;
use reifydb_value::value::{
	date::Date,
	datetime::{ArchivedDateTime, DateTime},
	duration::Duration,
	time::Time,
};
use rkyv::{Archive, seal::Seal};
use serde::{Deserialize, Serialize};

use crate::metrics::heap::HeapSize;

/// The domain a window orders and seals in.
///
/// Implementors pair a coordinate with the ONLY span type that may be subtracted from it. That
/// pairing is the whole point: seal horizons are `watermark - seal_after`, and when both sides were
/// a bare `u64` a millisecond span could be subtracted from a nanosecond coordinate. The result was
/// a horizon a million times too small, which sealed every window but the newest and silently
/// discarded late events and retractions across 33 operators. Expressing the span as an associated
/// type makes that subtraction fail to compile instead.
pub trait WindowCoord: Copy + Ord + Debug {
	type Span: Copy + Debug + IsZero + Default + Send + Sync;

	/// The upper bound of the domain, used as the "everything is behind the frontier" sentinel.
	const MAX: Self;

	fn saturating_sub_span(self, span: Self::Span) -> Self;

	/// Order-preserving `u64` for the persisted expiry index.
	///
	/// This is a STORAGE encoding, not a unit conversion: it is only ever compared against other
	/// values from the same coordinate domain, never against a span. Do not reintroduce span
	/// arithmetic here.
	fn to_order(self) -> u64;

	fn from_order(order: u64) -> Self;

	/// This span expressed in milliseconds, for the host's node-horizon derivation.
	///
	/// `None` when the domain has no wall-clock meaning. A count window seals after N rows, and
	/// there is no elapsed time after which a further row becomes inadmissible - handing the host a
	/// row count where it expects milliseconds would derive a horizon from a number that is not a
	/// duration at all.
	fn span_millis(span: Self::Span) -> Option<u64>;
}

impl WindowCoord for u64 {
	type Span = u64;

	const MAX: Self = u64::MAX;

	fn saturating_sub_span(self, span: u64) -> Self {
		self.saturating_sub(span)
	}

	fn to_order(self) -> u64 {
		self
	}

	fn from_order(order: u64) -> Self {
		order
	}

	fn span_millis(_span: u64) -> Option<u64> {
		None
	}
}

impl WindowCoord for DateTime {
	type Span = Duration;

	const MAX: Self = DateTime::MAX;

	fn saturating_sub_span(self, span: Duration) -> Self {
		self.saturating_sub(span)
	}

	fn to_order(self) -> u64 {
		self.timestamp_millis() as u64
	}

	fn from_order(order: u64) -> Self {
		// Saturate rather than default: an out-of-range key defaulting to the epoch would read as
		// ancient and be reclaimed on the next sweep, which is the opposite of what a far-future
		// coordinate should do.
		DateTime::from_timestamp_millis(order).unwrap_or(DateTime::MAX)
	}

	fn span_millis(span: Duration) -> Option<u64> {
		span.milliseconds().ok().and_then(|ms| u64::try_from(ms).ok())
	}
}

/// The span type that may be subtracted from a slot's coordinate.
pub type SlotSpan<S> = <<S as Slot>::Coord as WindowCoord>::Span;

pub trait Slot:
	Copy
	+ Ord
	+ Debug
	+ Add<Self::Duration, Output = Self>
	+ Sub<Self, Output = Self::Duration>
	+ Rem<Self::Duration, Output = Self::Duration>
	+ Sub<Self::Duration, Output = Self>
	+ ArchiveState
{
	type Duration: Copy + Ord + Debug + IsZero;

	/// The domain this slot orders and seals in.
	///
	/// A time window's coordinate is an instant; a count window's is a row number. Keeping it an
	/// associated type is what lets the seal arithmetic below be checked by the compiler: a
	/// `DateTime` coordinate can only have a `Duration` subtracted from it, so a span expressed in
	/// the wrong unit cannot reach it at all.
	type Coord: WindowCoord;

	fn order_key(&self) -> Self::Coord;

	fn from_order_key(coord: Self::Coord) -> Self;

	fn archived_order_key(archived: &<Self as Archive>::Archived) -> Self::Coord;

	fn seal_write(archived: Seal<'_, <Self as Archive>::Archived>, value: Self) -> bool {
		let _ = (archived, value);
		false
	}
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
	type Coord = u64;

	fn order_key(&self) -> u64 {
		*self
	}

	fn from_order_key(coord: u64) -> Self {
		coord
	}

	fn archived_order_key(archived: &<Self as Archive>::Archived) -> u64 {
		archived.to_native()
	}

	fn seal_write(mut archived: Seal<'_, <Self as Archive>::Archived>, value: Self) -> bool {
		*archived = value.into();
		true
	}
}

impl Slot for DateTime {
	type Duration = Duration;
	type Coord = DateTime;

	fn order_key(&self) -> DateTime {
		<DateTime as WindowCoord>::from_order(self.timestamp_millis() as u64)
	}

	fn from_order_key(coord: DateTime) -> Self {
		coord
	}

	fn archived_order_key(archived: &<Self as Archive>::Archived) -> DateTime {
		DateTime::from_timestamp_millis(archived.timestamp_millis() as u64).unwrap_or_default()
	}

	fn seal_write(archived: Seal<'_, <Self as Archive>::Archived>, value: Self) -> bool {
		ArchivedDateTime::seal_write(archived, value);
		true
	}
}

#[operator_state]
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
	use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

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
	#[derive(
		Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, RkyvArchive, RkyvSerialize, RkyvDeserialize,
	)]
	#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
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
		type Coord = u64;

		fn order_key(&self) -> u64 {
			self.0
		}

		fn from_order_key(coord: u64) -> Self {
			Tick(coord)
		}

		fn archived_order_key(archived: &<Self as RkyvArchive>::Archived) -> u64 {
			archived.0.to_native()
		}
	}

	#[test]
	fn a_time_coordinate_can_only_have_a_duration_subtracted_from_it() {
		// This is the invariant that used to live in a hand-written unit conversion, and the reason it
		// now lives in the type system instead: a seal horizon is watermark - seal_after, and when both
		// sides were a bare u64 nothing stopped a millisecond span reaching a nanosecond coordinate.
		// The result was a horizon a million times too small, which sealed every window but the newest
		// and silently discarded late events and retractions across 33 operators.
		// Pairing the coordinate with its own Span makes the wrong subtraction fail to compile, so what
		// is left to assert is that the pairing computes what it claims.
		let watermark = DateTime::from_timestamp_millis(6_060_000).expect("representable instant");
		let one_minute = Duration::from_seconds(60).expect("representable span");

		assert_eq!(
			watermark.saturating_sub_span(one_minute),
			DateTime::from_timestamp_millis(6_000_000).expect("representable"),
			"a minute behind the watermark is a minute, not a million times less"
		);
		assert_eq!(<DateTime as WindowCoord>::span_millis(one_minute), Some(60_000));
	}

	#[test]
	fn a_count_domain_reports_no_millisecond_span() {
		// A count window seals after N rows. There is no elapsed time after which a further row becomes
		// inadmissible, so handing the host a row count where it expects milliseconds would derive a
		// node horizon from a number that is not a duration at all - the same category error in the
		// opposite direction. None is the honest answer, and the host treats it as "no seal span".
		assert_eq!(<u64 as WindowCoord>::span_millis(100), None);
	}

	#[test]
	fn a_coordinate_survives_the_round_trip_through_its_storage_encoding() {
		// to_order/from_order are the persisted expiry-index encoding. They must be exact inverses:
		// a lossy round trip would move a window's anchor and either seal it early or strand it.
		let coord = DateTime::from_timestamp_millis(1_234_567).expect("representable");
		assert_eq!(<DateTime as WindowCoord>::from_order(coord.to_order()), coord);
		assert_eq!(<u64 as WindowCoord>::from_order(42u64.to_order()), 42);
	}

	#[test]
	fn a_seal_horizon_leaves_the_window_exactly_at_the_boundary_admissible() {
		// The boundary is load-bearing in both directions. A window whose start sits exactly one seal
		// span behind the watermark is still reachable by a late event, so sealing it would discard a
		// legitimate retraction; sealing nothing would let state grow without bound.
		let watermark = DateTime::from_timestamp_millis(6_060_000).expect("representable");
		let horizon = crate::window::engine::seal_horizon(
			watermark,
			Duration::from_seconds(60).expect("representable"),
		);

		let at_boundary = DateTime::from_timestamp_millis(6_000_000).expect("representable");
		let before_boundary = DateTime::from_timestamp_millis(5_999_999).expect("representable");

		assert!(!crate::window::engine::is_sealed(at_boundary, horizon), "the boundary window is still live");
		assert!(crate::window::engine::is_sealed(before_boundary, horizon), "anything older is sealed");
	}

	#[test]
	fn newtype_coord_works() {
		let span = WindowSpan::<Tick>::for_slot(Tick(125), 10);
		assert_eq!(span, WindowSpan::new(Tick(120), Tick(130)));
		assert!(span.contains(Tick(120)));
		assert!(!span.contains(Tick(130)));
	}
}
