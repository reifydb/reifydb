// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::{
		decode_u64, encode_u64,
		encoded::{EncodedKeyRange, IntoEncodedKey},
	},
	row::operator::{OperatorState, decode},
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	state::store::StateStore,
};
use reifydb_value::Result;
use tracing::instrument;

/// The due-ordered expiry index lives in the root group so a group's entries survive the phase-1 range
/// delete and drain on their own.
pub(crate) fn expiry_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::EXPIRY)
}

pub(crate) fn expiry_key<G>(expiry: u64, group: &G, suffix: &[u8]) -> GroupStateKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	let group = group.into_encoded_key();
	let group = group.as_ref();
	let mut tail = Vec::with_capacity(8 + group.len() + suffix.len());
	tail.extend_from_slice(&encode_u64(expiry));
	tail.extend_from_slice(group);
	tail.extend_from_slice(suffix);
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::EXPIRY, tail)
}

fn due_range(threshold: u64) -> EncodedKeyRange {
	let start = OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::EXPIRY, encode_u64(threshold));
	EncodedKeyRange::new(Bound::Included(start.as_encoded().clone()), expiry_range().end)
}

pub(crate) fn expiry_set<E: OperatorState>(store: &mut dyn StateStore, key: GroupStateKey, entry: E) -> Result<()> {
	store.state_set(&key, entry.encode_state(store.written_at())?)
}

pub(crate) fn expiry_drop(store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
	store.state_remove(key)
}

#[instrument(name = "flow::seal::expiry_due", level = "debug", skip_all)]
pub(crate) fn expiry_due<E: OperatorState>(
	store: &mut dyn StateStore,
	threshold: u64,
	limit: usize,
) -> Result<Vec<(GroupStateKey, E)>> {
	let mut out = Vec::new();
	store.state_range_visit(due_range(threshold), Some(limit), &mut |key, payload| {
		out.push((key, decode::<E>(&payload)?));
		Ok(())
	})?;
	Ok(out)
}

#[instrument(name = "flow::seal::expiry_earliest", level = "debug", skip_all)]
pub(crate) fn expiry_earliest(store: &mut dyn StateStore) -> Result<Option<u64>> {
	Ok(store.state_last(expiry_range())?.and_then(|(key, _)| {
		let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
		suffix.get(..8).map(|bytes| decode_u64(bytes.try_into().expect("eight expiry bytes")))
	}))
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator_state::GroupStateKey;
	use reifydb_macro::operator_state;

	use super::{expiry_drop, expiry_due, expiry_earliest, expiry_key, expiry_set};
	use crate::state::mock::MockStore;

	#[operator_state]
	#[derive(Clone, Debug, PartialEq)]
	struct Entry {
		row: u64,
	}

	fn key(expiry: u64, group: u32) -> GroupStateKey {
		expiry_key(expiry, &group, &[])
	}

	#[test]
	fn due_serves_only_entries_at_or_below_the_threshold_newest_first() {
		// Newest-due-first is the order the expire_batch cap relies on to defer the oldest backlog.
		let mut store = MockStore::default();

		for (expiry, row) in [(10u64, 1u64), (20, 2), (30, 3)] {
			expiry_set(
				&mut store,
				key(expiry, expiry as u32),
				Entry {
					row,
				},
			)
			.unwrap();
		}

		let due = expiry_due::<Entry>(&mut store, 20, 16).unwrap();
		let rows: Vec<u64> = due.iter().map(|(_, e)| e.row).collect();
		assert_eq!(rows, vec![2, 1], "expiry 30 is not yet due; 20 (newest due) precedes 10");
	}

	#[test]
	fn a_reader_that_wrote_nothing_still_sees_what_an_earlier_writer_persisted() {
		// A restarted engine must expire the windows its predecessor armed, or they never expire.
		let mut store = MockStore::default();
		expiry_set(
			&mut store,
			key(10, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		let due = expiry_due::<Entry>(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1, "the persisted entry must be visible to a reader that never wrote");
		assert_eq!(
			due[0].1,
			Entry {
				row: 1
			}
		);
	}

	#[test]
	fn a_dropped_key_leaves_only_the_surviving_entry() {
		// A later due must observe the net result of set and drop, never a stale or doubled view.
		let mut store = MockStore::default();
		expiry_set(
			&mut store,
			key(10, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();
		expiry_set(
			&mut store,
			key(20, 2),
			Entry {
				row: 2,
			},
		)
		.unwrap();
		expiry_drop(&mut store, &key(10, 1)).unwrap();

		let due = expiry_due::<Entry>(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1);
		assert_eq!(due[0].1.row, 2, "only the surviving entry may remain");
	}

	#[test]
	fn due_respects_the_batch_limit() {
		// Without the cap a due burst drains in one tick and stalls the flow actor.
		let mut store = MockStore::default();
		for expiry in 1u64..=5 {
			expiry_set(
				&mut store,
				key(expiry, expiry as u32),
				Entry {
					row: expiry,
				},
			)
			.unwrap();
		}
		let due = expiry_due::<Entry>(&mut store, 100, 2).unwrap();
		assert_eq!(due.len(), 2, "one call serves at most `limit` entries");
	}

	#[test]
	fn earliest_reports_the_soonest_expiry_not_the_latest() {
		// The inverted key order puts the soonest expiry last; reading the first entry would arm the seal timer
		// for the furthest window.
		let mut store = MockStore::default();
		for expiry in [30u64, 10, 20] {
			expiry_set(
				&mut store,
				key(expiry, expiry as u32),
				Entry {
					row: expiry,
				},
			)
			.unwrap();
		}
		assert_eq!(expiry_earliest(&mut store).unwrap(), Some(10));
	}

	#[test]
	fn earliest_of_an_empty_index_is_none() {
		// An operator with no armed window must report nothing, or the seal timer fires on garbage.
		let mut store = MockStore::default();
		assert_eq!(expiry_earliest(&mut store).unwrap(), None);
	}
}
