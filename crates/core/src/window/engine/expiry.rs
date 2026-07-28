// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem::size_of};

use reifydb_codec::{
	key::{encode_u64, encoded::EncodedKeyRange},
	state::{OperatorState, decode_state},
};
use reifydb_value::{Result, byte_size::ByteSize, count::Count};

use crate::{
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
	metrics::heap::StateMemory,
	state::store::StateStore,
	window::engine::expiry_range,
};

fn expiry_all_range() -> EncodedKeyRange {
	expiry_range()
}

fn due_start(threshold: u64) -> StateKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::EXPIRY, encode_u64(threshold))
}

pub(crate) struct ExpiryIndex<E> {
	entries: Option<BTreeMap<StateKey, E>>,
	bytes: u64,
}

impl<E: OperatorState + Clone> ExpiryIndex<E> {
	pub(crate) fn new() -> Self {
		Self {
			entries: None,
			bytes: 0,
		}
	}

	fn hydrate(&mut self, store: &mut impl StateStore) -> Result<&mut BTreeMap<StateKey, E>> {
		if self.entries.is_none() {
			let mut map = BTreeMap::new();
			let mut bytes = 0u64;
			store.state_range_visit(expiry_all_range(), None, &mut |key, payload| {
				bytes += entry_bytes::<E>(&key);
				map.insert(key, decode_state::<E>(&payload)?);
				Ok(())
			})?;
			self.entries = Some(map);
			self.bytes = bytes;
		}
		Ok(self.entries.as_mut().expect("hydrated above"))
	}

	pub(crate) fn set(&mut self, store: &mut impl StateStore, key: StateKey, entry: E) -> Result<()> {
		store.state_set(&key, entry.encode_state(store.clock_now())?)?;
		if let Some(map) = self.entries.as_mut() {
			let added = entry_bytes::<E>(&key);
			if map.insert(key, entry).is_none() {
				self.bytes += added;
			}
		}
		Ok(())
	}

	pub(crate) fn drop_key(&mut self, store: &mut impl StateStore, key: &StateKey) -> Result<()> {
		store.state_remove(key)?;
		if let Some(map) = self.entries.as_mut()
			&& map.remove(key).is_some()
		{
			self.bytes = self.bytes.saturating_sub(entry_bytes::<E>(key));
		}
		Ok(())
	}

	pub(crate) fn due(
		&mut self,
		store: &mut impl StateStore,
		threshold: u64,
		limit: usize,
	) -> Result<Vec<(StateKey, E)>> {
		let map = self.hydrate(store)?;
		Ok(map.range(due_start(threshold)..).take(limit).map(|(k, e)| (k.clone(), e.clone())).collect())
	}

	pub(crate) fn approximate_memory(&self) -> StateMemory {
		let entries = self.entries.as_ref().map_or(0, |map| map.len());
		StateMemory::new(Count::new(entries as u64), ByteSize::from_bytes(self.bytes))
	}
}

fn entry_bytes<E>(key: &StateKey) -> u64 {
	(key.as_slice().len() + size_of::<E>()) as u64
}

#[cfg(test)]
mod tests {
	use reifydb_macro::operator_state;

	use super::ExpiryIndex;
	use crate::{
		key::operator_state::StateKey,
		window::engine::{expiry_key, test_support::MockStore},
	};

	#[operator_state]
	#[derive(Clone, Debug, PartialEq)]
	struct Entry {
		row: u64,
	}

	fn key(expiry: u64, group: u32) -> StateKey {
		expiry_key(expiry, &group, &[])
	}

	#[test]
	fn due_serves_only_entries_at_or_below_the_threshold_newest_first() {
		// The index key encodes the expiry inverted (encode_u64 = !v big-endian), so
		// byte order is descending by expiry: due(threshold) must yield exactly the
		// entries with expiry <= threshold, newest first - the order the engines'
		// expire_batch cap relies on to defer the oldest backlog.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::<Entry>::new();

		for (expiry, row) in [(10u64, 1u64), (20, 2), (30, 3)] {
			index.set(
				&mut store,
				key(expiry, expiry as u32),
				Entry {
					row,
				},
			)
			.unwrap();
		}

		let due = index.due(&mut store, 20, 16).unwrap();
		let rows: Vec<u64> = due.iter().map(|(_, e)| e.row).collect();
		assert_eq!(rows, vec![2, 1], "expiry 30 is not yet due; 20 (newest due) precedes 10");
	}

	#[test]
	fn hydration_rebuilds_the_mirror_from_persisted_entries() {
		// A restarted engine starts with an empty mirror; the first due() must see
		// entries persisted by the previous incarnation, or windows would silently
		// never expire after a restart.
		let mut store = MockStore::default();
		let mut first = ExpiryIndex::<Entry>::new();
		first.set(
			&mut store,
			key(10, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		let mut restarted = ExpiryIndex::<Entry>::new();
		let due = restarted.due(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1, "the persisted entry must be visible after rehydration");
		assert_eq!(
			due[0].1,
			Entry {
				row: 1
			}
		);
	}

	#[test]
	fn mutations_before_hydration_reach_the_store_and_survive_hydration() {
		// set/remove before the first due() write through to the store without a
		// mirror; hydration must then observe the net result, not a stale or doubled
		// view.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::<Entry>::new();
		index.set(
			&mut store,
			key(10, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();
		index.set(
			&mut store,
			key(20, 2),
			Entry {
				row: 2,
			},
		)
		.unwrap();
		index.drop_key(&mut store, &key(10, 1)).unwrap();

		let due = index.due(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1);
		assert_eq!(due[0].1.row, 2, "only the surviving entry may remain after hydration");
	}

	#[test]
	fn due_respects_the_batch_limit() {
		// expire_batch bounds tick work; a limit-less due() would drain a due burst in
		// one tick and stall the flow actor.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::<Entry>::new();
		for expiry in 1u64..=5 {
			index.set(
				&mut store,
				key(expiry, expiry as u32),
				Entry {
					row: expiry,
				},
			)
			.unwrap();
		}
		let due = index.due(&mut store, 100, 2).unwrap();
		assert_eq!(due.len(), 2, "one call serves at most `limit` entries");
	}
}
