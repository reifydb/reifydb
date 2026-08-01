// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::state::ArchiveState;
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::value::{
	date::Date,
	datetime::{ArchivedDateTime, DateTime},
	duration::Duration,
	time::Time,
};
use rkyv::{Archive, munge::munge, seal::Seal};
use serde::{Deserialize, Serialize};

pub trait WindowCoord: Copy + Ord + Debug {
	type Span: Copy + Ord + Debug + IsZero + Default + Send + Sync;

	const MAX: Self;

	fn saturating_sub_span(self, span: Self::Span) -> Self;

	fn checked_sub_span(self, span: Self::Span) -> Option<Self>;

	fn add_span(self, span: Self::Span) -> Self;

	fn floor_to(self, span: Self::Span) -> Self;

	fn span_since(self, earlier: Self) -> Self::Span;

	fn to_order(self) -> u64;

	fn from_order(order: u64) -> Self;

	fn span_millis(span: Self::Span) -> Option<u64>;
}

impl WindowCoord for u64 {
	type Span = u64;

	const MAX: Self = u64::MAX;

	fn saturating_sub_span(self, span: u64) -> Self {
		self.saturating_sub(span)
	}

	fn checked_sub_span(self, span: u64) -> Option<Self> {
		self.checked_sub(span)
	}

	fn add_span(self, span: u64) -> Self {
		self + span
	}

	fn floor_to(self, span: u64) -> Self {
		self - (self % span)
	}

	fn span_since(self, earlier: Self) -> u64 {
		self - earlier
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

	fn checked_sub_span(self, span: Duration) -> Option<Self> {
		self.checked_sub(span)
	}

	fn add_span(self, span: Duration) -> Self {
		self + span
	}

	fn floor_to(self, span: Duration) -> Self {
		self - (self % span)
	}

	fn span_since(self, earlier: Self) -> Duration {
		self - earlier
	}

	fn to_order(self) -> u64 {
		self.timestamp_millis() as u64
	}

	fn from_order(order: u64) -> Self {
		DateTime::from_timestamp_millis(order).unwrap_or(DateTime::MAX)
	}

	fn span_millis(span: Duration) -> Option<u64> {
		span.milliseconds().ok().and_then(|ms| u64::try_from(ms).ok())
	}
}

pub type SlotCoord<S> = <S as Slot>::Coord;

pub trait WindowAnchor: Slot<Coord = Self> + WindowCoord {}

impl<T> WindowAnchor for T where T: Slot<Coord = T> + WindowCoord {}

pub type SlotSpan<S> = <<S as Slot>::Coord as WindowCoord>::Span;

pub trait Slot: Copy + Ord + Debug + ArchiveState {
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

#[operator_state(seal)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Stamped<C, T> {
	pub coord: C,
	pub tie: T,
}

impl<C: HeapSize, T: HeapSize> HeapSize for Stamped<C, T> {
	fn heap_size(&self) -> usize {
		self.coord.heap_size() + self.tie.heap_size()
	}
}

impl<C, T> Stamped<C, T> {
	#[inline]
	pub fn new(coord: C, tie: T) -> Self {
		Self {
			coord,
			tie,
		}
	}
}

impl<C, T> Slot for Stamped<C, T>
where
	C: WindowAnchor,
	T: Slot + Default,
	Self: ArchiveState + Archive<Archived = ArchivedStamped<C, T>>,
{
	type Coord = C;

	fn order_key(&self) -> C {
		self.coord
	}

	fn from_order_key(coord: C) -> Self {
		Self {
			coord,
			tie: T::default(),
		}
	}

	fn archived_order_key(archived: &<Self as Archive>::Archived) -> C {
		C::archived_order_key(&archived.coord)
	}

	fn seal_write(archived: Seal<'_, <Self as Archive>::Archived>, value: Self) -> bool {
		munge!(let ArchivedStamped { coord, tie } = archived);
		let wrote_coord = C::seal_write(coord, value.coord);
		let wrote_tie = T::seal_write(tie, value.tie);
		wrote_coord && wrote_tie
	}
}

impl<C> WindowSpan<C>
where
	C: WindowCoord,
{
	#[inline]
	pub fn for_coord(coord: C, span: C::Span) -> Self {
		assert!(!span.is_zero(), "WindowSpan::for_coord: span must be > 0");
		let start = coord.floor_to(span);
		Self {
			start,
			end: start.add_span(span),
		}
	}

	#[inline]
	pub fn new(start: C, end: C) -> Self {
		assert!(start < end, "WindowSpan::new: start ({start:?}) must be < end ({end:?})");
		Self {
			start,
			end,
		}
	}

	#[inline]
	pub fn duration(&self) -> C::Span {
		self.end.span_since(self.start)
	}

	#[inline]
	pub fn contains(&self, coord: C) -> bool {
		coord >= self.start && coord < self.end
	}

	#[inline]
	pub fn next(&self) -> Self {
		let span = self.duration();
		Self {
			start: self.end,
			end: self.end.add_span(span),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::state::{OperatorState, access_archive, decode_state};
	use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

	use super::*;
	use crate::window::engine::{is_sealed, seal_horizon};

	#[test]
	fn for_coord_aligns_to_span() {
		assert_eq!(WindowSpan::<u64>::for_coord(123, 60), WindowSpan::new(120u64, 180));
		assert_eq!(WindowSpan::<u64>::for_coord(0, 60), WindowSpan::new(0u64, 60));
		assert_eq!(WindowSpan::<u64>::for_coord(60, 60), WindowSpan::new(60u64, 120));
	}

	#[test]
	fn for_coord_aligns_datetime_to_span() {
		let coord = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let one_second = Duration::from_seconds(1).unwrap();
		let one_minute = Duration::from_seconds(60).unwrap();

		// A sub-minute (1s) window must stay 1s, not round up to a minute.
		let sec = WindowSpan::for_coord(coord, one_second);
		assert_eq!(sec.start, DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap());
		assert_eq!(sec.end, DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 26).unwrap());
		assert_eq!(sec.duration(), one_second);

		// A 1m window aligns the coord down to the minute boundary.
		let min = WindowSpan::for_coord(coord, one_minute);
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
	fn boundary_coord_belongs_to_next_window() {
		// An event at exactly window_end must not be claimed by the current window.
		let cur = WindowSpan::<u64>::for_coord(60, 60);
		let nxt = cur.next();
		assert!(!cur.contains(120));
		assert!(nxt.contains(120));
		assert_eq!(nxt, WindowSpan::new(120u64, 180));
	}

	#[test]
	#[should_panic(expected = "span must be > 0")]
	fn zero_duration_panics() {
		WindowSpan::<u64>::for_coord(10, 0);
	}

	#[test]
	#[should_panic(expected = "must be <")]
	fn empty_span_panics() {
		WindowSpan::new(100u64, 100);
	}

	/// A toy newtype demonstrating that any well-behaved domain works, not just `u64` and `DateTime`.
	#[derive(
		Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, RkyvArchive, RkyvSerialize, RkyvDeserialize,
	)]
	#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
	struct Ordinal(u64);

	impl WindowCoord for Ordinal {
		type Span = u64;

		const MAX: Self = Ordinal(u64::MAX);

		fn saturating_sub_span(self, span: u64) -> Self {
			Ordinal(self.0.saturating_sub(span))
		}

		fn checked_sub_span(self, span: u64) -> Option<Self> {
			self.0.checked_sub(span).map(Ordinal)
		}

		fn add_span(self, span: u64) -> Self {
			Ordinal(self.0 + span)
		}

		fn floor_to(self, span: u64) -> Self {
			Ordinal(self.0 - (self.0 % span))
		}

		fn span_since(self, earlier: Self) -> u64 {
			self.0 - earlier.0
		}

		fn to_order(self) -> u64 {
			self.0
		}

		fn from_order(order: u64) -> Self {
			Ordinal(order)
		}

		fn span_millis(_span: u64) -> Option<u64> {
			None
		}
	}

	#[test]
	fn a_time_coordinate_can_only_have_a_duration_subtracted_from_it() {
		// A seal horizon is watermark - seal_after; with both sides a bare u64 nothing stopped a
		// millisecond span reaching a nanosecond coordinate, yielding a horizon a million times too
		// small. Pairing a coordinate with its own Span makes the wrong subtraction fail to compile.
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
		// A count window seals after N rows, with no elapsed time after which a further row becomes
		// inadmissible. Handing the host a row count where it expects milliseconds would derive a
		// operator horizon from something that is not a duration; the host reads none as "no seal span".
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
		let horizon = seal_horizon(watermark, Duration::from_seconds(60).expect("representable"));

		let at_boundary = DateTime::from_timestamp_millis(6_000_000).expect("representable");
		let before_boundary = DateTime::from_timestamp_millis(5_999_999).expect("representable");

		assert!(!is_sealed(at_boundary, horizon), "the boundary window is still live");
		assert!(is_sealed(before_boundary, horizon), "anything older is sealed");
	}

	#[test]
	fn a_custom_domain_works_like_the_built_in_ones() {
		let span = WindowSpan::<Ordinal>::for_coord(Ordinal(125), 10);
		assert_eq!(span, WindowSpan::new(Ordinal(120), Ordinal(130)));
		assert!(span.contains(Ordinal(120)));
		assert!(!span.contains(Ordinal(130)));
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_timestamp_millis(millis).expect("representable instant")
	}

	#[test]
	fn two_events_sharing_a_coordinate_stay_distinct() {
		// Upstream stamps events more coarsely than they arrive, so distinct events routinely land
		// on one coordinate. Keyed by the coordinate alone the later would overwrite the earlier in
		// the slot map and its contribution would vanish from the window with no error anywhere.
		let mut slots = BTreeMap::new();
		slots.insert(Stamped::new(at(1_000), 7u64), "first");
		slots.insert(Stamped::new(at(1_000), 9u64), "second");

		assert_eq!(slots.len(), 2, "a shared coordinate must not collapse two events into one slot");
		assert_eq!(
			slots.values().copied().collect::<Vec<_>>(),
			vec!["first", "second"],
			"within a coordinate the tie-break orders them"
		);
	}

	#[test]
	fn ordering_is_lexicographic_with_the_coordinate_first() {
		// The map walk order IS the replay order for path-dependent accumulators, so a tie-break
		// must never outrank a coordinate or the recurrence reads its inputs out of time order.
		let early_high_tie = Stamped::new(at(1_000), u64::MAX);
		let late_low_tie = Stamped::new(at(2_000), 0u64);

		assert!(early_high_tie < late_low_tie, "the coordinate dominates the tie-break");
	}

	#[test]
	fn a_slot_rebuilt_from_a_bare_coordinate_sorts_at_or_before_every_event_there() {
		// from_order_key reconstructs a slot when only the persisted coordinate survives (the
		// expiry index stores no tie). It must land at the very start of its coordinate, or a
		// range scan anchored on it would skip events sharing that coordinate.
		let rebuilt = <Stamped<DateTime, u64> as Slot>::from_order_key(at(1_000));

		assert_eq!(rebuilt.order_key(), at(1_000), "the coordinate must survive the round trip");
		assert!(rebuilt <= Stamped::new(at(1_000), 0u64));
		assert!(rebuilt < Stamped::new(at(1_000), 1u64));
	}

	#[test]
	fn seal_write_lands_both_halves_in_the_archived_bytes() {
		// The persist path rewrites the archived slot in place, and both halves have to land: a
		// coordinate updated while its tie went stale resumes replay from a slot that never
		// existed. Returning false would drop every bump onto the slow rewrite path instead.
		let initial = Stamped::new(at(1_000), 7u64);
		let mut bytes = initial.encode_state(DateTime::default()).expect("encodes");

		let bumped = Stamped::new(at(2_000), 9u64);
		// SAFETY: bytes were just produced by encode_state for this exact type.
		let seal = unsafe { <Stamped<DateTime, u64>>::archived_seal_trusted(&mut bytes) };
		assert!(
			<Stamped<DateTime, u64> as Slot>::seal_write(seal, bumped),
			"the shared slot must take the in-place path, not fall back"
		);

		let back: Stamped<DateTime, u64> = decode_state(&bytes).expect("decodes");
		assert_eq!(back, bumped, "both the coordinate and the tie-break must be rewritten");

		let archived = access_archive::<Stamped<DateTime, u64>>(&bytes).expect("accessible");
		assert_eq!(<Stamped<DateTime, u64> as Slot>::archived_order_key(archived), bumped.order_key());
	}
}
