// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{key::operator_state::GroupId, state::timer::StateStore};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::window::{
	coord::{EventCoord, OrdinalCoord, TimeStamped},
	meta::WindowMeta,
};

pub struct Mint<'a> {
	meta: &'a mut WindowMeta,
}

impl<'a> Mint<'a> {
	pub fn new(meta: &'a mut WindowMeta) -> Self {
		Self {
			meta,
		}
	}

	pub fn event(row: &impl TimeStamped) -> EventCoord {
		EventCoord::of(row)
	}

	pub fn ordinal(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<OrdinalCoord> {
		Ok(OrdinalCoord::from_arrival_counter(self.meta.get_and_increment_count(store, group)?))
	}

	pub fn membership(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		self.meta.lookup_row_index(store, group, row_number)
	}

	pub fn record_membership(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		self.meta.store_row_index(store, group, row_number, window_id)
	}

	pub fn drop_membership(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		self.meta.drop_row_index(store, group, row_number)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::operator::state::mock::MockStore;

	const GROUP: GroupId = GroupId(42);
	const OTHER: GroupId = GroupId(43);

	fn meta() -> WindowMeta {
		WindowMeta::new()
	}

	#[test]
	fn the_arrival_counter_starts_at_zero_and_never_repeats_within_a_group() {
		// The ordinal IS the coordinate for a count window, so a repeat aliases two rows onto one
		// slot and a skip leaves a hole the sweep never reaches. The counter is read-then-increment,
		// so starting at 1 would shift every window boundary by one row.
		let mut meta = meta();
		let mut mint = Mint::new(&mut meta);
		let mut store = MockStore::default();

		let minted: Vec<u64> =
			(0..4).map(|_| mint.ordinal(&mut store, GROUP).unwrap().value()).collect::<Vec<_>>();

		assert_eq!(minted, vec![0, 1, 2, 3]);
	}

	#[test]
	fn each_group_counts_independently() {
		// A count window holds the last N rows per group. One shared counter would let a busy group
		// shove a quiet group's rows across a boundary they never crossed.
		let mut meta = meta();
		let mut mint = Mint::new(&mut meta);
		let mut store = MockStore::default();

		mint.ordinal(&mut store, GROUP).unwrap();
		mint.ordinal(&mut store, GROUP).unwrap();

		assert_eq!(mint.ordinal(&mut store, OTHER).unwrap().value(), 0);
		assert_eq!(mint.ordinal(&mut store, GROUP).unwrap().value(), 2);
	}

	#[test]
	fn an_event_coordinate_is_minted_from_the_row_and_never_from_the_counter() {
		// A time window's coordinate comes from the row's own instant, never the arrival counter one
		// call away. The signatures enforce it: `event` cannot see the store, `ordinal` cannot see a row.
		let row = DateTime::from_millis(5_000);

		assert_eq!(Mint::event(&row).at(), DateTime::from_millis(5_000));
	}

	#[test]
	fn a_row_records_every_window_it_joined_and_never_the_same_one_twice() {
		// Sliding windows overlap, so one row joins several and retraction must find all of them. A
		// duplicated id subtracts the row's contribution twice from one window.
		let mut meta = meta();
		let mut mint = Mint::new(&mut meta);
		let mut store = MockStore::default();

		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();
		mint.record_membership(&mut store, GROUP, RowNumber(7), 200).unwrap();
		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();

		assert_eq!(mint.membership(&mut store, GROUP, RowNumber(7)).unwrap(), vec![100, 200]);
	}

	#[test]
	fn a_row_that_joined_no_window_reports_an_empty_membership() {
		// Retraction runs for every removed row, including rows the gate refused. Defaulting to some
		// window would retract a contribution the row never made.
		let mut meta = meta();
		let mut mint = Mint::new(&mut meta);
		let mut store = MockStore::default();

		assert!(mint.membership(&mut store, GROUP, RowNumber(7)).unwrap().is_empty());
	}

	#[test]
	fn membership_is_scoped_to_its_group() {
		// Row numbers are unique per source, not per group, so two groups routinely see the same
		// RowNumber. One shared list would retract a row from a group it never entered.
		let mut meta = meta();
		let mut mint = Mint::new(&mut meta);
		let mut store = MockStore::default();

		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();

		assert!(mint.membership(&mut store, OTHER, RowNumber(7)).unwrap().is_empty());
	}
}
