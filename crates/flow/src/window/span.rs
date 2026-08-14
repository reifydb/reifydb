// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::row::operator::StateCodec;
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::value::datetime::DateTime;

use crate::operator::state::seal::coord::{Coord, IsZero};

pub type SlotCoord<S> = <S as Slot>::Coord;

pub trait WindowAnchor: Slot<Coord = Self> + Coord {}

impl<T> WindowAnchor for T where T: Slot<Coord = T> + Coord {}

pub type SlotSpan<S> = <<S as Slot>::Coord as Coord>::Span;

pub trait Slot: Copy + Ord + Debug + StateCodec {
	type Coord: Coord;

	fn order_key(&self) -> Self::Coord;

	fn from_order_key(coord: Self::Coord) -> Self;
}

impl Slot for DateTime {
	type Coord = DateTime;

	fn order_key(&self) -> DateTime {
		*self
	}

	fn from_order_key(coord: DateTime) -> Self {
		coord
	}
}

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowSpan<T> {
	pub start: T,
	pub end: T,
}

impl<T: HeapSize> HeapSize for WindowSpan<T> {
	fn heap_size(&self) -> usize {
		self.start.heap_size() + self.end.heap_size()
	}
}

impl<C> WindowSpan<C>
where
	C: Coord,
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
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::duration::Duration,
	};

	use super::*;
	use crate::operator::state::seal::policy::{is_sealed, seal_horizon};

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
		let span = WindowSpan::new(at_millis(100), at_millis(200));
		assert!(span.contains(at_millis(100)));
		assert!(span.contains(at_millis(199)));
		assert!(!span.contains(at_millis(200)));
		assert!(!span.contains(at_millis(99)));
	}

	#[test]
	fn boundary_coord_belongs_to_next_window() {
		// An event at exactly window_end must not be claimed by the current window.
		let cur = WindowSpan::for_coord(at_millis(60), millis(60));
		let nxt = cur.next();
		assert!(!cur.contains(at_millis(120)));
		assert!(nxt.contains(at_millis(120)));
		assert_eq!(nxt, WindowSpan::new(at_millis(120), at_millis(180)));
	}

	#[test]
	#[should_panic(expected = "span must be > 0")]
	fn zero_duration_panics() {
		WindowSpan::for_coord(at_millis(10), Duration::zero());
	}

	#[test]
	#[should_panic(expected = "must be <")]
	fn empty_span_panics() {
		WindowSpan::new(at_millis(100), at_millis(100));
	}

	#[test]
	fn a_time_coordinate_can_only_have_a_duration_subtracted_from_it() {
		// A seal horizon is watermark - seal_after; with both sides a bare u64 nothing stopped a
		// millisecond span reaching a nanosecond coordinate, yielding a horizon a million times too
		// small. Pairing a coordinate with its own Span makes the wrong subtraction fail to compile.
		let watermark = DateTime::from_epoch_millis(6_060_000).expect("representable instant");
		let one_minute = Duration::from_seconds(60).expect("representable span");

		assert_eq!(
			watermark.saturating_sub_span(one_minute),
			DateTime::from_epoch_millis(6_000_000).expect("representable"),
			"a minute behind the watermark is a minute, not a million times less"
		);
		assert_eq!(<DateTime as Coord>::span_millis(one_minute), Some(60_000));
	}

	#[test]
	fn a_coordinate_survives_the_round_trip_through_its_storage_encoding() {
		// to_order/from_order are the persisted expiry-index encoding. They must be exact inverses:
		// a lossy round trip would move a window's anchor and either seal it early or strand it.
		let coord = DateTime::from_epoch_millis(1_234_567).expect("representable");
		assert_eq!(<DateTime as Coord>::from_order(coord.to_order()), coord);
	}

	#[test]
	fn a_seal_horizon_leaves_the_window_exactly_at_the_boundary_admissible() {
		// The boundary is load-bearing in both directions. A window whose start sits exactly one seal
		// span behind the watermark is still reachable by a late event, so sealing it would discard a
		// legitimate retraction; sealing nothing would let state grow without bound.
		let watermark = DateTime::from_epoch_millis(6_060_000).expect("representable");
		let horizon = seal_horizon(watermark, Duration::from_seconds(60).expect("representable"));

		let at_boundary = DateTime::from_epoch_millis(6_000_000).expect("representable");
		let before_boundary = DateTime::from_epoch_millis(5_999_999).expect("representable");

		assert!(!is_sealed(at_boundary, horizon), "the boundary window is still live");
		assert!(is_sealed(before_boundary, horizon), "anything older is sealed");
	}
}
