// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::key::encoded::EncodedKeyBuilder;
use reifydb_core::{
	common::WindowSizeDomain,
	error::CoreError,
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
	state::timer::{StateStore, TimerStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	operator::state::seal::{
		coord::{Coord, IsZero},
		domain::SealDomain,
		ledger::SealLedger,
	},
	window::{settings::WindowSettings, span::Slot},
};

pub trait TimeStamped {
	fn row_time(&self) -> DateTime;
}

impl TimeStamped for DateTime {
	fn row_time(&self) -> DateTime {
		*self
	}
}

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTimeCoord(pub DateTime);

impl DateTimeCoord {
	pub fn of(row: &impl TimeStamped) -> Self {
		Self(row.row_time())
	}
}

impl HeapSize for DateTimeCoord {
	fn heap_size(&self) -> usize {
		0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RowSpan {
	rows: u64,
}

impl RowSpan {
	pub const ZERO: Self = Self {
		rows: 0,
	};

	pub fn of(rows: u64) -> Self {
		Self {
			rows,
		}
	}

	pub fn rows(self) -> u64 {
		self.rows
	}
}

impl IsZero for RowSpan {
	#[inline]
	fn is_zero(&self) -> bool {
		self.rows == 0
	}
}

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct U8Coord(pub u64);

impl U8Coord {
	pub fn from_row_number(row_number: RowNumber) -> Self {
		Self(row_number.0)
	}

	pub fn value(self) -> u64 {
		self.0
	}
}

impl HeapSize for U8Coord {
	fn heap_size(&self) -> usize {
		0
	}
}

impl Coord for U8Coord {
	type Span = RowSpan;

	const MAX: Self = Self(u64::MAX);

	fn saturating_sub_span(self, span: RowSpan) -> Self {
		Self(self.0.saturating_sub(span.rows))
	}

	fn checked_sub_span(self, span: RowSpan) -> Option<Self> {
		self.0.checked_sub(span.rows).map(Self)
	}

	fn add_span(self, span: RowSpan) -> Self {
		Self(self.0 + span.rows)
	}

	fn checked_add_span(self, span: RowSpan) -> Option<Self> {
		self.0.checked_add(span.rows).map(Self)
	}

	fn floor_to(self, span: RowSpan) -> Self {
		Self(self.0 - (self.0 % span.rows))
	}

	fn span_since(self, earlier: Self) -> RowSpan {
		RowSpan {
			rows: self.0 - earlier.0,
		}
	}

	fn to_order(self) -> u64 {
		self.0
	}

	fn from_order(order: u64) -> Self {
		Self(order)
	}

	fn extend_key(self, builder: EncodedKeyBuilder) -> EncodedKeyBuilder {
		builder.u64(self.0)
	}
}

impl SealDomain for U8Coord {
	type SealSpan = RowSpan;

	const SIZE_DOMAIN: WindowSizeDomain = WindowSizeDomain::Slots;

	fn arms_timer() -> bool {
		false
	}

	fn seal_span_of(with: &ApplyWith) -> Result<Option<RowSpan>> {
		let count = with.window_slots()?;
		let Some(lateness) = with.lateness_count()? else {
			return Ok(None);
		};
		Ok(Some(RowSpan::of(count.saturating_add(lateness))))
	}

	fn window_settings_of(with: &ApplyWith) -> Result<WindowSettings<Self>> {
		let Some(kind) = &with.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		Ok(WindowSettings {
			kind: kind.clone(),
			size: Some(RowSpan::of(with.window_slots()?)),
			pane: None,
			lag: None,
			slide: with.window_slide_slots()?.map(RowSpan::of),
			gap: None,
			lateness: RowSpan::of(with.lateness_count()?.unwrap_or(0)),
			immutable: with.immutable_count()?.map(RowSpan::of),
		})
	}

	fn observe(store: &mut (impl StateStore + TimerStore), newest: Self, _seal_span: RowSpan) -> Result<()> {
		SealLedger::observe(store, newest.to_order())?;
		Ok(())
	}

	fn frontier(store: &mut (impl StateStore + TimerStore)) -> Result<Self> {
		Ok(Self::from_order(SealLedger::read_order(store)?.unwrap_or(0)))
	}

	fn horizon(frontier: Self, seal_span: RowSpan) -> Self {
		frontier.saturating_sub_span(seal_span)
	}

	fn rearm_decay(
		_store: &mut (impl StateStore + TimerStore),
		_settings: &WindowSettings<Self>,
		_before: Option<u64>,
		_after: Option<u64>,
	) -> Result<()> {
		Ok(())
	}
}

impl Slot for U8Coord {
	type Coord = U8Coord;

	fn order_key(&self) -> U8Coord {
		*self
	}

	fn from_order_key(coord: U8Coord) -> Self {
		coord
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{key::encoded::EncodedKey, row::operator::state::encode};
	use reifydb_core::{
		common::{WindowKind, WindowSize},
		operator_with::WithSpan,
	};
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::{operator::state::mock::MockStore, window::span::WindowSpan};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	struct Row {
		time: DateTime,
		other_column: DateTime,
	}

	impl TimeStamped for Row {
		fn row_time(&self) -> DateTime {
			self.time
		}
	}

	#[test]
	fn of_reads_the_row_time_and_not_another_column() {
		// Bucketing by another column keys the window on something the substrate can neither see nor seal
		// against.
		let row = Row {
			time: DateTime::from_millis(5_000),
			other_column: DateTime::from_millis(9_999),
		};

		assert_eq!(DateTimeCoord::of(&row).0, DateTime::from_millis(5_000));
		assert_ne!(DateTimeCoord::of(&row).0, row.other_column);
	}

	#[test]
	fn event_coordinates_order_by_instant() {
		// Ordering is what the seal ledger and the seal-span comparison are built
		// on, so it must be the instant's order and nothing else.
		let early = DateTimeCoord::of(&DateTime::from_millis(1));
		let late = DateTimeCoord::of(&DateTime::from_millis(2));

		assert!(early < late);
	}

	#[test]
	fn an_ordinal_encodes_to_the_same_bytes_as_the_bare_count_it_replaced() {
		// A changed persisted layout is silent: stored buffer keys get reinterpreted, not rejected.
		let value = 0x0123_4567_89AB_CDEFu64;

		let wrapped = encode(&U8Coord(value)).expect("encode");
		let bare = encode(&value).expect("encode");

		assert_eq!(wrapped.body(), bare.body(), "the newtype changed the persisted layout");
	}

	#[test]
	fn ordinal_arithmetic_counts_rows() {
		let coord = U8Coord(100);

		assert_eq!(coord.saturating_sub_span(RowSpan::of(64)), U8Coord(36));
		assert_eq!(coord.add_span(RowSpan::of(5)), U8Coord(105));
		assert_eq!(coord.span_since(U8Coord(60)), RowSpan::of(40));
	}

	#[test]
	fn an_ordinal_below_its_own_span_has_no_earlier_coordinate_rather_than_wrapping() {
		// Wrapping below zero lands near u64::MAX and evicts the whole buffer on the first pass.
		let coord = U8Coord(10);

		assert_eq!(coord.checked_sub_span(RowSpan::of(11)), None);
		assert_eq!(coord.checked_sub_span(RowSpan::of(10)), Some(U8Coord(0)));
		assert_eq!(coord.saturating_sub_span(RowSpan::of(11)), U8Coord(0));
	}

	#[test]
	fn a_row_ordinal_seals_without_the_wheel() {
		// a row count fed to the timer wheel lands just past the epoch, fires at once and rearms forever
		assert!(!<U8Coord as SealDomain>::arms_timer());
	}

	#[test]
	fn a_row_window_seals_after_its_count_plus_lateness() {
		// the seal span must hold both the slots the window keeps and the rows it still admits late
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(64),
			}),
			lateness: Some(WithSpan::Count(6)),
			immutable: None,
			retention: None,
		};

		assert_eq!(U8Coord::seal_span_of(&with).unwrap(), Some(RowSpan::of(70)));
	}

	#[test]
	fn a_row_window_without_lateness_never_seals() {
		// with no lateness bound, a row window has nothing to seal against and must never hold state
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(64),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		assert_eq!(U8Coord::seal_span_of(&with).unwrap(), None);
	}

	#[test]
	fn a_row_window_rejects_a_duration_lateness() {
		// a row window counts rows, so a duration lateness has no count to add to its span
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(64),
			}),
			lateness: Some(WithSpan::Duration(secs(30))),
			immutable: None,
			retention: None,
		};

		assert!(U8Coord::seal_span_of(&with).is_err());
	}

	#[test]
	fn an_ordinal_frontier_moves_inline_from_the_batch_and_only_forward() {
		// nothing arms a timer here, so the batch itself must advance the ledger or the horizon never moves
		let mut store = MockStore::default();

		assert_eq!(U8Coord::frontier(&mut store).unwrap(), U8Coord(0));

		U8Coord::observe(&mut store, U8Coord(90), RowSpan::ZERO).unwrap();
		U8Coord::observe(&mut store, U8Coord(30), RowSpan::ZERO).unwrap();

		assert_eq!(U8Coord::frontier(&mut store).unwrap(), U8Coord(90));
		assert_eq!(U8Coord::horizon(U8Coord(90), RowSpan::of(64)), U8Coord(26));
	}

	#[test]
	fn both_ways_to_build_a_u8_coord_produce_the_same_value() {
		// A row number and a bare value must land in one domain type, or each source would need its own driver.
		let minted = U8Coord(7);
		let from_row = U8Coord::from_row_number(RowNumber(7));

		assert_eq!(minted, from_row);
		assert_eq!(minted.value(), 7);
	}

	#[test]
	fn the_ordinal_settings_read_counts_and_default_lateness_to_zero_rows() {
		// a slot-domain window is sized in rows; an omitted lateness must be zero rows and an omitted
		// immutable none, or a plain window would silently start refusing retractions
		let declared = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: Some(WithSpan::Count(4)),
			immutable: Some(WithSpan::Count(2)),
			retention: None,
		};
		let omitted = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let declared = U8Coord::window_settings_of(&declared).unwrap();
		let omitted = U8Coord::window_settings_of(&omitted).unwrap();

		assert_eq!(declared.size, Some(RowSpan::of(10)));
		assert_eq!(declared.lateness, RowSpan::of(4));
		assert_eq!(declared.immutable, Some(RowSpan::of(2)));
		assert_eq!(declared.pane, None);
		assert_eq!(omitted.lateness, RowSpan::ZERO);
		assert_eq!(omitted.immutable, None);
	}

	#[test]
	fn the_ordinal_settings_refuse_a_missing_window_and_a_duration_size() {
		// a duration has no row count, so the ordinal domain must fail loud rather than guess a size
		let duration = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		assert!(U8Coord::window_settings_of(&ApplyWith::default()).is_err());
		assert!(U8Coord::window_settings_of(&duration).is_err());
	}

	#[test]
	fn the_ordinal_settings_carry_the_slide_of_a_sliding_window() {
		// a slide that came back as none would make create panic on every slot sliding view
		let sliding = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Count(10),
				slide: WindowSize::Count(4),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let settings = U8Coord::window_settings_of(&sliding).unwrap();

		assert_eq!(settings.size, Some(RowSpan::of(10)));
		assert_eq!(settings.slide, Some(RowSpan::of(4)));
	}

	#[test]
	fn a_datetime_coord_span_encodes_to_the_same_bytes_as_the_raw_datetime_span_it_replaced() {
		// A changed persisted layout is silent: every stored window span would be reinterpreted, not rejected.
		let (start, end) = (DateTime::from_millis(1_000), DateTime::from_millis(2_000));

		let wrapped = encode(&WindowSpan {
			start: DateTimeCoord(start),
			end: DateTimeCoord(end),
		})
		.expect("encode");
		let bare = encode(&WindowSpan {
			start,
			end,
		})
		.expect("encode");

		assert_eq!(wrapped.body(), bare.body(), "the newtype changed the persisted span layout");
	}

	#[test]
	fn a_datetime_coord_writes_the_same_key_bytes_as_the_raw_datetime() {
		// A different key encoding orphans every stored window, because lookups miss the old keys.
		let at = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();

		let key = DateTimeCoord(at).extend_key(EncodedKey::builder().u32(7u32)).build();

		assert_eq!(key, EncodedKey::builder().u32(7u32).datetime(&at).build());
	}

	#[test]
	fn a_u8_coord_writes_the_same_key_bytes_as_the_bare_u64() {
		// A different key encoding orphans every stored slot window, because lookups miss the old keys.
		let key = U8Coord(42).extend_key(EncodedKey::builder().u32(7u32)).build();

		assert_eq!(key, EncodedKey::builder().u32(7u32).u64(42u64).build());
	}

	#[test]
	fn floor_to_aligns_onto_span_boundaries() {
		// An unaligned start would give rows of one window different spans and split its totals.
		assert_eq!(U8Coord(0).floor_to(RowSpan::of(4)), U8Coord(0));
		assert_eq!(U8Coord(3).floor_to(RowSpan::of(4)), U8Coord(0));
		assert_eq!(U8Coord(4).floor_to(RowSpan::of(4)), U8Coord(4));
		assert_eq!(U8Coord(7).floor_to(RowSpan::of(4)), U8Coord(4));
		assert_eq!(U8Coord(9).floor_to(RowSpan::of(1)), U8Coord(9));
	}

	#[test]
	fn max_is_the_largest_u8_coord() {
		// A smaller MAX would leave real slots above the bound that scans treat as the end.
		assert_eq!(<U8Coord as Coord>::MAX, U8Coord(u64::MAX));
		assert!(U8Coord(u64::MAX - 1) < <U8Coord as Coord>::MAX);
	}

	#[test]
	fn the_order_is_the_value_itself() {
		// Index keys persist through this round trip, so anything but the identity reorders them.
		for value in [0u64, 1, 281_474_976_710_656, u64::MAX] {
			assert_eq!(U8Coord(value).to_order(), value);
			assert_eq!(<U8Coord as Coord>::from_order(value), U8Coord(value));
		}
	}

	#[test]
	fn a_u8_coord_is_its_own_order_key() {
		// A non-identity order key would desync the window index from the coord the driver routes on.
		let coord = U8Coord(7);

		assert_eq!(coord.order_key(), coord);
		assert_eq!(U8Coord::from_order_key(coord), coord);
	}

	#[test]
	fn the_horizon_trails_the_frontier_by_the_lag() {
		// A horizon too close to the frontier seals windows that may still take late rows.
		assert_eq!(<U8Coord as SealDomain>::horizon(U8Coord(100), RowSpan::of(32)), U8Coord(68));
		assert_eq!(<U8Coord as SealDomain>::horizon(U8Coord(100), RowSpan::ZERO), U8Coord(100));
	}

	#[test]
	fn the_horizon_clamps_at_genesis() {
		// Wrapping below zero would land near u64::MAX and declare every window sealed.
		assert_eq!(<U8Coord as SealDomain>::horizon(U8Coord(5), RowSpan::of(32)), U8Coord(0));
	}

	#[test]
	fn an_empty_ledger_reads_as_genesis() {
		// A frontier above zero on a new operator would seal early slots before any row arrived.
		let mut store = MockStore::default();

		assert_eq!(<U8Coord as SealDomain>::frontier(&mut store).unwrap(), U8Coord(0));
	}

	#[test]
	fn observing_a_u8_coord_never_arms_a_timer() {
		// A slot read as an instant lands just past the epoch, fires at once and rearms forever.
		let mut store = MockStore::recording_timers();

		<U8Coord as SealDomain>::observe(&mut store, U8Coord(900), RowSpan::of(32)).unwrap();

		assert!(store.timers().is_empty());
	}

	#[test]
	fn a_u8_coord_heap_size_is_zero() {
		// A non-zero size would inflate the operator memory metrics for a value stored inline.
		assert_eq!(U8Coord(u64::MAX).heap_size(), 0);
	}

	#[test]
	fn a_u8_coord_seal_span_needs_a_window() {
		// Without a window there is no slot count to seal against, so this must fail loud.
		assert!(<U8Coord as SealDomain>::seal_span_of(&ApplyWith::default()).is_err());
	}

	#[test]
	fn a_u8_coord_seal_span_refuses_a_duration_size() {
		// A duration read as a slot count would size the seal span in the wrong unit.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			lateness: Some(WithSpan::Count(6)),
			immutable: None,
			retention: None,
		};

		assert!(<U8Coord as SealDomain>::seal_span_of(&with).is_err());
	}

	#[test]
	fn a_u8_coord_is_sized_in_slots() {
		// A coord that claimed time would have its slot windows checked as durations.
		assert_eq!(<U8Coord as SealDomain>::SIZE_DOMAIN, WindowSizeDomain::Slots);
	}
}
