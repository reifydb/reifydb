// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{key::operator_state::GroupId, state::store::StateStore};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::window::{
	aux::WindowAux,
	coord::{EventCoord, OrdinalCoord, TimeStamped},
};

pub struct Mint<'a> {
	aux: &'a mut WindowAux,
}

impl<'a> Mint<'a> {
	pub fn new(aux: &'a mut WindowAux) -> Self {
		Self {
			aux,
		}
	}

	pub fn event(row: &impl TimeStamped) -> EventCoord {
		EventCoord::of(row)
	}

	pub fn ordinal<S: StateStore>(&mut self, store: &mut S, group: GroupId) -> Result<OrdinalCoord> {
		Ok(OrdinalCoord::from_arrival_counter(self.aux.get_and_increment_count(store, group)?))
	}

	pub fn membership<S: StateStore>(
		&mut self,
		store: &mut S,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		self.aux.lookup_row_index(store, group, row_number)
	}

	pub fn record_membership<S: StateStore>(
		&mut self,
		store: &mut S,
		group: GroupId,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		self.aux.store_row_index(store, group, row_number, window_id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::state::budget::OperatorStateBudgetHandle;
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::window::engine::test_support::MockStore;

	const GROUP: GroupId = GroupId(42);
	const OTHER: GroupId = GroupId(43);

	fn aux() -> WindowAux {
		WindowAux::new(OperatorStateBudgetHandle::default())
	}

	#[test]
	fn the_arrival_counter_starts_at_zero_and_never_repeats_within_a_group() {
		// Intent: the ordinal IS the coordinate for a count window, so a repeat aliases two
		// rows onto one slot and a skip leaves a hole the sweep never reaches. The counter is
		// read-then-increment, so the FIRST row must mint 0 - starting at 1 shifts every
		// window boundary by one row for the life of the operator.
		// Mutation: return the post-increment value and the first row lands in the second slot.
		let mut aux = aux();
		let mut mint = Mint::new(&mut aux);
		let mut store = MockStore::default();

		let minted: Vec<u64> =
			(0..4).map(|_| mint.ordinal(&mut store, GROUP).unwrap().value()).collect::<Vec<_>>();

		assert_eq!(minted, vec![0, 1, 2, 3]);
	}

	#[test]
	fn each_group_counts_independently() {
		// Intent: a count window holds the last N rows PER GROUP. One shared counter would make
		// a group's window boundary depend on traffic in every other group, so a busy group
		// would shove a quiet group's rows across a boundary they never crossed.
		// Mutation: drop the group from the count key and the second group starts at 2.
		let mut aux = aux();
		let mut mint = Mint::new(&mut aux);
		let mut store = MockStore::default();

		mint.ordinal(&mut store, GROUP).unwrap();
		mint.ordinal(&mut store, GROUP).unwrap();

		assert_eq!(mint.ordinal(&mut store, OTHER).unwrap().value(), 0);
		assert_eq!(mint.ordinal(&mut store, GROUP).unwrap().value(), 2);
	}

	#[test]
	fn an_event_coordinate_is_minted_from_the_row_and_never_from_the_counter() {
		// Intent: the two minting paths must not be interchangeable. This is D3 at the shell
		// boundary: a time window's coordinate comes from the row's own instant, and the
		// arrival counter - which exists and is one call away - must never reach it. The
		// signatures are what enforce it; `event` cannot see the store and `ordinal` cannot see
		// a row.
		let row = DateTime::from_millis(5_000);

		assert_eq!(Mint::event(&row).at(), DateTime::from_millis(5_000));
	}

	#[test]
	fn a_row_records_every_window_it_joined_and_never_the_same_one_twice() {
		// Intent: sliding windows overlap, so one row contributes to several windows and the
		// retraction path has to find all of them. A duplicated id makes the retraction subtract
		// the row's contribution twice from one window, which silently corrupts the aggregate
		// in a direction no assertion downstream would attribute back to here.
		// Mutation: drop the contains() guard in store_row_index and a re-delivered row doubles
		// its own retraction.
		let mut aux = aux();
		let mut mint = Mint::new(&mut aux);
		let mut store = MockStore::default();

		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();
		mint.record_membership(&mut store, GROUP, RowNumber(7), 200).unwrap();
		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();

		assert_eq!(mint.membership(&mut store, GROUP, RowNumber(7)).unwrap(), vec![100, 200]);
	}

	#[test]
	fn a_row_that_joined_no_window_reports_an_empty_membership() {
		// Retraction runs for every removed row, including rows the gate refused. An unknown row
		// must answer "no windows" rather than defaulting to some window, or a refused row would
		// retract a contribution it never made.
		let mut aux = aux();
		let mut mint = Mint::new(&mut aux);
		let mut store = MockStore::default();

		assert!(mint.membership(&mut store, GROUP, RowNumber(7)).unwrap().is_empty());
	}

	#[test]
	fn membership_is_scoped_to_its_group() {
		// Row numbers are unique per source, not per group, so two groups routinely see the same
		// RowNumber. Sharing one membership list would retract a row from windows in a group it
		// never entered.
		let mut aux = aux();
		let mut mint = Mint::new(&mut aux);
		let mut store = MockStore::default();

		mint.record_membership(&mut store, GROUP, RowNumber(7), 100).unwrap();

		assert!(mint.membership(&mut store, OTHER, RowNumber(7)).unwrap().is_empty());
	}
}
