// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::{
		decode_u64, encode_u64,
		encoded::{EncodedKeyRange, IntoEncodedKey},
	},
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range},
	state::timer::StateStore,
};
use reifydb_value::Result;
use tracing::instrument;

pub(crate) fn expiry_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, KeyspaceId::EXPIRY)
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
	OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::EXPIRY, tail)
}

fn due_range(threshold: u64) -> EncodedKeyRange {
	let start = OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::EXPIRY, encode_u64(threshold));
	EncodedKeyRange::new(Bound::Included(start.as_encoded().clone()), expiry_range().end)
}

pub(crate) fn expiry_set<E: OperatorState>(store: &mut dyn StateStore, key: GroupStateKey, entry: E) -> Result<()> {
	store.state_set(&key, entry.encode_state()?)
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

fn expiry_of(key: &GroupStateKey) -> u64 {
	let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_bytes()).expect("expiry key must decode");
	let bytes = suffix.get(..8).expect("expiry key carries eight threshold bytes");
	decode_u64(bytes.try_into().expect("eight expiry bytes"))
}

struct PendingScan {
	capped: bool,
}

#[derive(Default)]
pub(crate) struct ExpiryIndex {
	earliest: Option<u64>,
	inserted: Option<u64>,
	pending: Option<PendingScan>,
}

impl ExpiryIndex {
	pub(crate) fn set<E: OperatorState>(
		&mut self,
		store: &mut dyn StateStore,
		key: GroupStateKey,
		entry: E,
	) -> Result<()> {
		let expiry = expiry_of(&key);
		self.inserted = Some(self.inserted.map_or(expiry, |seen| seen.min(expiry)));
		self.earliest = self.earliest.map(|earliest| earliest.min(expiry));
		expiry_set(store, key, entry)
	}

	pub(crate) fn due<E: OperatorState>(
		&mut self,
		store: &mut dyn StateStore,
		threshold: u64,
		limit: usize,
	) -> Result<Vec<(GroupStateKey, E)>> {
		self.pending = None;
		if self.earliest.is_some_and(|earliest| threshold < earliest) {
			return Ok(Vec::new());
		}
		self.inserted = None;
		let due = expiry_due(store, threshold, limit)?;
		self.pending = Some(PendingScan {
			capped: due.len() >= limit,
		});
		Ok(due)
	}

	pub(crate) fn settle(&mut self, store: &mut dyn StateStore) -> Result<()> {
		let Some(scan) = self.pending.take() else {
			return Ok(());
		};
		if scan.capped {
			return Ok(());
		}
		let grounded = expiry_earliest(store)?.unwrap_or(u64::MAX);
		self.earliest = Some(self.inserted.map_or(grounded, |seen| grounded.min(seen)));
		Ok(())
	}

	pub(crate) fn earliest(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		let earliest = expiry_earliest(store)?;
		self.earliest = Some(earliest.unwrap_or(u64::MAX));
		Ok(earliest)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator_state::GroupStateKey;
	use reifydb_macro::operator_state;

	use super::{ExpiryIndex, expiry_drop, expiry_due, expiry_earliest, expiry_key, expiry_set};
	use crate::operator::state::mock::MockStore;

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

	#[test]
	fn a_fresh_index_scans_rather_than_trusting_an_unset_watermark() {
		// An operator rebuilt after a restart or a retried commit inherits no bound; if an unset
		// watermark gated the scan, every window a predecessor armed would never expire.
		let mut store = MockStore::default();
		expiry_set(
			&mut store,
			key(10, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		let mut index = ExpiryIndex::default();
		let due = index.due::<Entry>(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1, "an index with no watermark must reach the store");
	}

	#[test]
	fn an_uncapped_scan_raises_the_watermark_so_a_lower_threshold_never_reaches_the_store() {
		// The whole point of the gate: once a scan proved nothing is due at or below its threshold,
		// a lower threshold must not pay for another range scan. The planted entry is invisible to
		// the index, so finding it would prove the store was read.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::default();

		assert!(index.due::<Entry>(&mut store, 100, 16).unwrap().is_empty());
		index.settle(&mut store).unwrap();

		expiry_set(
			&mut store,
			key(50, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		let due = index.due::<Entry>(&mut store, 100, 16).unwrap();
		assert!(due.is_empty(), "a threshold below the raised watermark must skip the range scan entirely");
	}

	#[test]
	fn a_set_below_the_watermark_lowers_it_so_the_next_scan_finds_the_entry() {
		// An insert can only bring the true earliest forward. A watermark that ignored the insert
		// would sit above it and the window would silently never expire.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::default();

		assert!(index.due::<Entry>(&mut store, 100, 16).unwrap().is_empty());
		index.settle(&mut store).unwrap();

		index.set(
			&mut store,
			key(50, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		let due = index.due::<Entry>(&mut store, 60, 16).unwrap();
		assert_eq!(due.len(), 1, "the entry armed at 50 must be reachable at threshold 60");
		assert_eq!(due[0].1.row, 1);
	}

	#[test]
	fn an_entry_armed_during_a_scan_survives_that_scan_raising_the_watermark() {
		// A rolling expire re-arms surviving buffers inside the same scan. The raise must be capped
		// by what was armed, or the re-armed entry is sealed behind a watermark above it.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::default();

		assert!(index.due::<Entry>(&mut store, 100, 16).unwrap().is_empty());
		index.set(
			&mut store,
			key(50, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();
		index.settle(&mut store).unwrap();

		let due = index.due::<Entry>(&mut store, 60, 16).unwrap();
		assert_eq!(due.len(), 1, "the raise must not climb past an entry armed during the scan");
		assert_eq!(due[0].1.row, 1);
	}

	#[test]
	fn a_capped_scan_leaves_the_watermark_below_its_own_threshold() {
		// A scan that hit the batch cap left entries at or below its threshold behind. Raising past
		// them would strand the deferred backlog forever.
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

		let mut index = ExpiryIndex::default();
		assert_eq!(index.due::<Entry>(&mut store, 100, 2).unwrap().len(), 2);
		index.settle(&mut store).unwrap();

		let due = index.due::<Entry>(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 5, "nothing was dropped, so the same threshold must still see every entry");
	}

	#[test]
	fn a_capped_drain_reaches_every_entry_across_successive_scans() {
		// The tumbling and rolling expire loops drain in batches; if a capped batch could raise the
		// watermark the tail of the backlog would never be handed back and those windows never seal.
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

		let mut index = ExpiryIndex::default();
		let mut drained: Vec<u64> = Vec::new();
		for _ in 0..4 {
			let due = index.due::<Entry>(&mut store, 100, 2).unwrap();
			for (index_key, entry) in due {
				expiry_drop(&mut store, &index_key).unwrap();
				drained.push(entry.row);
			}
			index.settle(&mut store).unwrap();
		}

		drained.sort_unstable();
		assert_eq!(drained, vec![1, 2, 3, 4, 5], "every armed entry must be handed back exactly once");
		assert_eq!(expiry_earliest(&mut store).unwrap(), None, "the index must be empty once drained");
	}

	#[test]
	fn reading_the_earliest_expiry_tightens_the_watermark_to_what_the_index_holds() {
		// The rolling face reads the exact earliest to arm its timer; recording it keeps the gate
		// tight, and an empty index must gate every threshold rather than leave the bound unset.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::default();
		assert_eq!(index.earliest(&mut store).unwrap(), None);

		expiry_set(
			&mut store,
			key(50, 1),
			Entry {
				row: 1,
			},
		)
		.unwrap();

		assert!(
			index.due::<Entry>(&mut store, 100, 16).unwrap().is_empty(),
			"an index read as empty must gate until something is armed through it"
		);

		assert_eq!(index.earliest(&mut store).unwrap(), Some(50));
		assert_eq!(
			index.due::<Entry>(&mut store, 100, 16).unwrap().len(),
			1,
			"the exact read must lower the bound back onto the entry the store holds"
		);
	}
}
