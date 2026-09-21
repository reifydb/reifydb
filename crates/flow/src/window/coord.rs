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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventCoord(DateTime);

impl EventCoord {
	pub fn of(row: &impl TimeStamped) -> Self {
		Self(row.row_time())
	}

	pub fn at(self) -> DateTime {
		self.0
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
pub struct OrdinalCoord {
	ordinal: u64,
}

impl OrdinalCoord {
	pub fn from_arrival_counter(ordinal: u64) -> Self {
		Self {
			ordinal,
		}
	}

	pub fn from_row_number(row_number: RowNumber) -> Self {
		Self {
			ordinal: row_number.0,
		}
	}

	pub fn value(self) -> u64 {
		self.ordinal
	}
}

impl HeapSize for OrdinalCoord {
	fn heap_size(&self) -> usize {
		0
	}
}

impl Coord for OrdinalCoord {
	type Span = RowSpan;

	const MAX: Self = Self {
		ordinal: u64::MAX,
	};

	fn saturating_sub_span(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal.saturating_sub(span.rows),
		}
	}

	fn checked_sub_span(self, span: RowSpan) -> Option<Self> {
		self.ordinal.checked_sub(span.rows).map(|ordinal| Self {
			ordinal,
		})
	}

	fn add_span(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal + span.rows,
		}
	}

	fn floor_to(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal - (self.ordinal % span.rows),
		}
	}

	fn span_since(self, earlier: Self) -> RowSpan {
		RowSpan {
			rows: self.ordinal - earlier.ordinal,
		}
	}

	fn to_order(self) -> u64 {
		self.ordinal
	}

	fn from_order(order: u64) -> Self {
		Self {
			ordinal: order,
		}
	}

	fn extend_key(self, builder: EncodedKeyBuilder) -> EncodedKeyBuilder {
		builder.u64(self.ordinal)
	}
}

impl SealDomain for OrdinalCoord {
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
			size: RowSpan::of(with.window_slots()?),
			pane: None,
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
}

impl Slot for OrdinalCoord {
	type Coord = OrdinalCoord;

	fn order_key(&self) -> OrdinalCoord {
		*self
	}

	fn from_order_key(coord: OrdinalCoord) -> Self {
		coord
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::encode;
	use reifydb_core::{
		common::{WindowKind, WindowSize},
		operator_with::WithSpan,
	};
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::operator::state::mock::MockStore;

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
	fn an_event_coordinate_can_only_come_from_the_row_time() {
		// `of` is the only constructor, so a coordinate cannot come from a data column, a config
		// value or a clock read. The row's second DateTime is what an operator would be tempted to
		// bucket by, keying the window on something the substrate can neither see nor seal against.
		let row = Row {
			time: DateTime::from_millis(5_000),
			other_column: DateTime::from_millis(9_999),
		};

		assert_eq!(EventCoord::of(&row).at(), DateTime::from_millis(5_000));
		assert_ne!(EventCoord::of(&row).at(), row.other_column);
	}

	#[test]
	fn event_coordinates_order_by_instant() {
		// Ordering is what the seal ledger and the admissible-span comparison are built
		// on, so it must be the instant's order and nothing else.
		let early = EventCoord::of(&DateTime::from_millis(1));
		let late = EventCoord::of(&DateTime::from_millis(2));

		assert!(early < late);
	}

	#[test]
	fn an_ordinal_encodes_to_the_same_bytes_as_the_bare_count_it_replaced() {
		// A changed persisted layout is silent: stored buffer keys get reinterpreted, not rejected.
		let value = 0x0123_4567_89AB_CDEFu64;

		let wrapped = encode(&OrdinalCoord::from_arrival_counter(value)).expect("encode");
		let bare = encode(&value).expect("encode");

		assert_eq!(wrapped.body(), bare.body(), "the newtype changed the persisted layout");
	}

	#[test]
	fn ordinal_arithmetic_counts_rows() {
		let coord = OrdinalCoord::from_arrival_counter(100);

		assert_eq!(coord.saturating_sub_span(RowSpan::of(64)), OrdinalCoord::from_arrival_counter(36));
		assert_eq!(coord.add_span(RowSpan::of(5)), OrdinalCoord::from_arrival_counter(105));
		assert_eq!(coord.span_since(OrdinalCoord::from_arrival_counter(60)), RowSpan::of(40));
	}

	#[test]
	fn an_ordinal_below_its_own_span_has_no_earlier_coordinate_rather_than_wrapping() {
		// Wrapping below zero lands near u64::MAX and evicts the whole buffer on the first pass.
		let coord = OrdinalCoord::from_arrival_counter(10);

		assert_eq!(coord.checked_sub_span(RowSpan::of(11)), None);
		assert_eq!(coord.checked_sub_span(RowSpan::of(10)), Some(OrdinalCoord::from_arrival_counter(0)));
		assert_eq!(coord.saturating_sub_span(RowSpan::of(11)), OrdinalCoord::from_arrival_counter(0));
	}

	#[test]
	fn a_row_ordinal_seals_without_the_wheel() {
		// a row count fed to the timer wheel lands just past the epoch, fires at once and rearms forever
		assert!(!<OrdinalCoord as SealDomain>::arms_timer());
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
		};

		assert_eq!(OrdinalCoord::seal_span_of(&with).unwrap(), Some(RowSpan::of(70)));
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
		};

		assert_eq!(OrdinalCoord::seal_span_of(&with).unwrap(), None);
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
		};

		assert!(OrdinalCoord::seal_span_of(&with).is_err());
	}

	#[test]
	fn an_ordinal_frontier_moves_inline_from_the_batch_and_only_forward() {
		// nothing arms a timer here, so the batch itself must advance the ledger or the horizon never moves
		let mut store = MockStore::default();

		assert_eq!(OrdinalCoord::frontier(&mut store).unwrap(), OrdinalCoord::from_arrival_counter(0));

		OrdinalCoord::observe(&mut store, OrdinalCoord::from_arrival_counter(90), RowSpan::ZERO).unwrap();
		OrdinalCoord::observe(&mut store, OrdinalCoord::from_arrival_counter(30), RowSpan::ZERO).unwrap();

		assert_eq!(OrdinalCoord::frontier(&mut store).unwrap(), OrdinalCoord::from_arrival_counter(90));
		assert_eq!(
			OrdinalCoord::horizon(OrdinalCoord::from_arrival_counter(90), RowSpan::of(64)),
			OrdinalCoord::from_arrival_counter(26)
		);
	}

	#[test]
	fn both_ordinal_sources_produce_the_same_domain() {
		// An ordinal can be minted from a per-group arrival counter or from a RowNumber, and both
		// must land in one domain type or each count kind would need its own driver.
		let minted = OrdinalCoord::from_arrival_counter(7);
		let from_row = OrdinalCoord::from_row_number(RowNumber(7));

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
		};
		let omitted = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
		};

		let declared = OrdinalCoord::window_settings_of(&declared).unwrap();
		let omitted = OrdinalCoord::window_settings_of(&omitted).unwrap();

		assert_eq!(declared.size, RowSpan::of(10));
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
		};

		assert!(OrdinalCoord::window_settings_of(&ApplyWith::default()).is_err());
		assert!(OrdinalCoord::window_settings_of(&duration).is_err());
	}
}
