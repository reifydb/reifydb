// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, ops::Bound};

use reifydb_codec::{
	key::encoded::EncodedKeyRange,
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	key::{
		operator::{
			keyspace::expiry::{Expiry, ExpiryKey, TumblingExpiry, TumblingExpiryKey},
			state::{GroupId, GroupStateKey, OperatorStateKey, keyspace_inner_range},
			traits::Keyspace,
		},
		typed::{Key, direction::Desc},
	},
	state::{
		timer::StateStore,
		typed::{SuffixBytes, TypedStateStore, typed_key},
	},
};
use reifydb_value::{Result, util::hash::Hash128};
use tracing::instrument;

pub(crate) fn expiry_range<K: Keyspace>() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, K::ID)
}

pub(crate) fn rolling_expiry_key(threshold: u64, owner: Hash128) -> GroupStateKey {
	typed_key::<Expiry>(GroupId::ROOT, &ExpiryKey {
		threshold: Desc(threshold),
		owner: Desc(owner),
	})
}

pub(crate) fn tumbling_expiry_key(threshold: u64, owner: Hash128, window_start: u64) -> GroupStateKey {
	typed_key::<TumblingExpiry>(GroupId::ROOT, &TumblingExpiryKey {
		threshold: Desc(threshold),
		owner: Desc(owner),
		window_start: Desc(window_start),
	})
}

pub(crate) trait ExpirySuffix: SuffixBytes {
	fn at_threshold(threshold: u64) -> Self;

	fn threshold(&self) -> u64;
}

impl ExpirySuffix for ExpiryKey {
	fn at_threshold(threshold: u64) -> Self {
		Self {
			threshold: Desc(threshold),
			owner: Key::low(),
		}
	}

	fn threshold(&self) -> u64 {
		self.threshold.0
	}
}

impl ExpirySuffix for TumblingExpiryKey {
	fn at_threshold(threshold: u64) -> Self {
		Self {
			threshold: Desc(threshold),
			owner: Key::low(),
			window_start: Key::low(),
		}
	}

	fn threshold(&self) -> u64 {
		self.threshold.0
	}
}

pub(crate) fn expiry_set<E: OperatorState>(store: &mut dyn StateStore, key: GroupStateKey, entry: E) -> Result<()> {
	store.state_set(&key, entry.encode_state()?)
}

pub(crate) fn expiry_drop(store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
	store.state_remove(key)
}

#[instrument(name = "flow::seal::expiry_due", level = "debug", skip_all)]
pub(crate) fn expiry_due<K, E>(store: &mut dyn StateStore, threshold: u64, limit: usize) -> Result<Vec<(GroupStateKey, E)>>
where
	K: Keyspace,
	K::Suffix: ExpirySuffix,
	E: OperatorState,
{
	let from = K::Suffix::at_threshold(threshold);
	let page = store.state_scan_in::<K>(GroupId::ROOT, Bound::Included(&from), Some(limit))?;
	let mut out = Vec::with_capacity(page.len());
	for (suffix, payload) in page {
		out.push((typed_key::<K>(GroupId::ROOT, &suffix), decode::<E>(&payload)?));
	}
	Ok(out)
}

#[instrument(name = "flow::seal::expiry_earliest", level = "debug", skip_all)]
pub(crate) fn expiry_earliest<K>(store: &mut dyn StateStore) -> Result<Option<u64>>
where
	K: Keyspace,
	K::Suffix: ExpirySuffix,
{
	let Some((key, _)) = store.state_last(expiry_range::<K>())? else {
		return Ok(None);
	};
	let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_bytes()).expect("an expiry key must decode");
	Ok(K::Suffix::from_suffix_bytes(&suffix).map(|suffix| suffix.threshold()))
}

fn expiry_of<K>(key: &GroupStateKey) -> u64
where
	K: Keyspace,
	K::Suffix: ExpirySuffix,
{
	let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_bytes()).expect("expiry key must decode");
	K::Suffix::from_suffix_bytes(&suffix).expect("an expiry key must carry every column its keyspace declares").threshold()
}

struct PendingScan {
	capped: bool,
}

pub(crate) struct ExpiryIndex<K: Keyspace>
where
	K::Suffix: ExpirySuffix,
{
	earliest: Option<u64>,
	inserted: Option<u64>,
	pending: Option<PendingScan>,
	keyspace: PhantomData<K>,
}

impl<K: Keyspace> Default for ExpiryIndex<K>
where
	K::Suffix: ExpirySuffix,
{
	fn default() -> Self {
		Self {
			earliest: None,
			inserted: None,
			pending: None,
			keyspace: PhantomData,
		}
	}
}

impl<K: Keyspace> ExpiryIndex<K>
where
	K::Suffix: ExpirySuffix,
{
	pub(crate) fn set<E: OperatorState>(
		&mut self,
		store: &mut dyn StateStore,
		key: GroupStateKey,
		entry: E,
	) -> Result<()> {
		let expiry = expiry_of::<K>(&key);
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
		let due = expiry_due::<K, E>(store, threshold, limit)?;
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
		let grounded = expiry_earliest::<K>(store)?.unwrap_or(u64::MAX);
		self.earliest = Some(self.inserted.map_or(grounded, |seen| grounded.min(seen)));
		Ok(())
	}

	pub(crate) fn earliest(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		let earliest = expiry_earliest::<K>(store)?;
		self.earliest = Some(earliest.unwrap_or(u64::MAX));
		Ok(earliest)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator::{keyspace::expiry::Expiry, state::GroupStateKey};
	use reifydb_macro::operator_state;

	use super::{ExpiryIndex, expiry_drop, expiry_due, expiry_earliest, expiry_set, rolling_expiry_key};
	use crate::{operator::state::mock::MockStore, window::engine::group_hash};

	#[operator_state]
	#[derive(Clone, Debug, PartialEq)]
	struct Entry {
		row: u64,
	}

	fn key(expiry: u64, group: u32) -> GroupStateKey {
		// Hashes the group the way the engines do, so these keys are the ones the index really holds.
		rolling_expiry_key(expiry, group_hash(&group).unwrap())
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

		let due = expiry_due::<Expiry, Entry>(&mut store, 20, 16).unwrap();
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

		let due = expiry_due::<Expiry, Entry>(&mut store, 100, 16).unwrap();
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

		let due = expiry_due::<Expiry, Entry>(&mut store, 100, 16).unwrap();
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
		let due = expiry_due::<Expiry, Entry>(&mut store, 100, 2).unwrap();
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
		assert_eq!(expiry_earliest::<Expiry>(&mut store).unwrap(), Some(10));
	}

	#[test]
	fn earliest_of_an_empty_index_is_none() {
		// An operator with no armed window must report nothing, or the seal timer fires on garbage.
		let mut store = MockStore::default();
		assert_eq!(expiry_earliest::<Expiry>(&mut store).unwrap(), None);
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

		let mut index = ExpiryIndex::<Expiry>::default();
		let due = index.due::<Entry>(&mut store, 100, 16).unwrap();
		assert_eq!(due.len(), 1, "an index with no watermark must reach the store");
	}

	#[test]
	fn an_uncapped_scan_raises_the_watermark_so_a_lower_threshold_never_reaches_the_store() {
		// The whole point of the gate: once a scan proved nothing is due at or below its threshold,
		// a lower threshold must not pay for another range scan. The planted entry is invisible to
		// the index, so finding it would prove the store was read.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::<Expiry>::default();

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
		let mut index = ExpiryIndex::<Expiry>::default();

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
		let mut index = ExpiryIndex::<Expiry>::default();

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

		let mut index = ExpiryIndex::<Expiry>::default();
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

		let mut index = ExpiryIndex::<Expiry>::default();
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
		assert_eq!(
			expiry_earliest::<Expiry>(&mut store).unwrap(),
			None,
			"the index must be empty once drained"
		);
	}

	#[test]
	fn reading_the_earliest_expiry_tightens_the_watermark_to_what_the_index_holds() {
		// The rolling face reads the exact earliest to arm its timer; recording it keeps the gate
		// tight, and an empty index must gate every threshold rather than leave the bound unset.
		let mut store = MockStore::default();
		let mut index = ExpiryIndex::<Expiry>::default();
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
