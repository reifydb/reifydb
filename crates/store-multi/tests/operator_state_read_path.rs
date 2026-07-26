// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state read semantics through the multi store.
//!
//! History, because this file used to assert the exact opposite and the reversal was
//! deliberate. Production flow workloads probe operator state keys that do not exist yet
//! (new window buckets, missing join partners) on nearly every apply. Absence was once
//! provable only by the persistent SQLite tier, which put ~1,000 synchronous SQLite reads
//! per second on the serialized flow actor (jupiter incident, 2026-07-04). The fix at the
//! time was in this layer: the first persistent fall-through for an operator node bulk
//! loaded the node's whole page into the read buffer and marked it range-complete, giving
//! the read buffer absence authority for that node.
//!
//! That fix is gone. It cached decoded-adjacent bytes a second time (the operator state
//! cache above it held the same data), it bounded itself by a hard-coded page cap rather
//! than by a real byte budget, and its bytes were invisible to the memory accounting this
//! redesign exists to make total. Operator state residency now lives in exactly one place:
//! the byte-bounded authority cache in `reifydb_core::state::cache::StateCache`, owned by
//! sub-flow and charged against `OperatorStateBudget`. The store is no longer a cache for
//! operator state; it is the store of record beneath that cache.
//!
//! So the jupiter property (steady-state operator applies do not hit SQLite) has not been
//! abandoned, it has moved up a layer and is tested there. What this file pins now is the
//! contract the layer below must honour for that to be safe:
//!
//!   1. operator reads never populate the read tier (no second copy, no unaccounted bytes)
//!   2. the store never claims absence from memory, so a cold read is always truthful
//!   3. MVCC, tombstone, and drop/purge semantics for operator keys are unchanged
//!
//! Several tests write or delete rows in the SQLite tier directly, bypassing the store, to
//! prove which tier answered a read. That bypass violates the tier mirror on purpose; each
//! such test states what it proves.

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, object::ObjectId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
	},
	key::{flow_node_state::FlowNodeStateKey, row::RowKey},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore, tier::TierStorage};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn state_key(node: u64, suffix: &str) -> EncodedKey {
	FlowNodeStateKey::encoded(node, suffix.as_bytes().to_vec())
}

fn internal_key(node: u64, suffix: &str) -> EncodedKey {
	FlowNodeStateKey::encoded(node, suffix.as_bytes().to_vec())
}

fn persistent_only_set(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	let persistent = store.persistent().expect("persistent tier configured");
	let table = classify_key(k);
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(table).or_default().push((k.clone(), Some(CowVec::new(value.as_bytes().to_vec()))));
	persistent.set(CommitVersion(version), batches).unwrap();
}

fn persistent_only_delete(store: &StandardMultiStore, k: &EncodedKey) {
	let persistent = store.persistent().expect("persistent tier configured");
	let deleted = persistent.delete_keys(classify_key(k), std::slice::from_ref(k)).unwrap();
	assert_eq!(deleted, 1, "bypass delete must remove exactly the targeted row");
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.row.to_vec())
}

fn range_keys(store: &StandardMultiStore, range: EncodedKeyRange, read: u64) -> Vec<EncodedKey> {
	store.range(
		range,
		MultiVersionScope::AsOf {
			read: CommitVersion(read),
		},
		64,
	)
	.map(|r| r.unwrap().key)
	.collect()
}

/// Entries resident in the read buffer, summed across every shard and domain. These tests
/// touch operator keys only, so any nonzero count means operator bytes leaked into the
/// read tier.
fn read_tier_entries(store: &StandardMultiStore) -> usize {
	let shards = store.read_buffer_shard_metrics();
	assert!(
		!shards.is_empty(),
		"the read tier must be configured for these assertions to mean anything; an unconfigured tier \
		 would report zero entries vacuously"
	);
	shards.iter().map(|m| m.state.entries).sum()
}

#[test]
fn operator_reads_never_populate_the_read_tier() {
	// The double-cache this redesign removes. Every path that could back-populate the read
	// buffer is exercised (point read through to persistent, commit write-through, batched
	// get_many, range scan) and the read tier must stay empty of operator entries
	// throughout. If any of these regains a back-populate, operator bytes get cached twice
	// and the second copy is invisible to OperatorStateBudget.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	let k2 = state_key(7, "b");

	persistent_only_set(&store, &k1, 5, "v5-a");
	assert_eq!(read_tier_entries(&store), 0, "seeding persistence must not touch the read tier");

	assert_eq!(get(&store, &k1, 9).as_deref(), Some(b"v5-a".as_slice()), "a cold read must reach persistence");
	assert_eq!(read_tier_entries(&store), 0, "a persistent fall-through must not back-populate the read tier");

	assert_eq!(get(&store, &state_key(7, "missing"), 9), None);
	assert_eq!(read_tier_entries(&store), 0, "an absent-key probe must not warm anything");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k2.clone(),
			row: EncodedRow(CowVec::new(b"v8-b".to_vec())),
		}],
		CommitVersion(8),
	)
	.unwrap();
	assert_eq!(read_tier_entries(&store), 0, "an operator commit must not write through into the read tier");

	let found = store.get_many(&[k1.clone(), k2.clone()], CommitVersion(9)).unwrap();
	assert_eq!(found.len(), 2, "get_many must still resolve both rows through commit and persistent tiers");
	assert_eq!(read_tier_entries(&store), 0, "batched reads must not back-populate the read tier");

	let scanned = range_keys(&store, FlowNodeStateKey::node_range(7.into()), 9);
	assert_eq!(scanned.len(), 2, "the range scan must see both rows");
	assert_eq!(read_tier_entries(&store), 0, "range scans must not back-populate the read tier");
}

#[test]
fn source_reads_still_populate_the_read_tier() {
	// Negative control for the test above. Without this, a read tier that had been broken
	// outright (or a metric that never moves) would let every zero-entry assertion pass
	// vacuously. Source rows must still be cached on a persistent fall-through: the
	// excision was surgical to the operator domain, not a removal of read caching.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let object = ObjectId::Table(TableId(1));
	let row = RowKey::encoded(object, 1);

	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(classify_key(&row)).or_default().push((row.clone(), Some(CowVec::new(b"src".to_vec()))));
	store.persistent().expect("persistent tier configured").set(CommitVersion(5), batches).unwrap();

	assert_eq!(read_tier_entries(&store), 0, "nothing is resident before the first read");
	assert_eq!(get(&store, &row, 9).as_deref(), Some(b"src".as_slice()));
	assert_eq!(
		read_tier_entries(&store),
		1,
		"a source row must still be back-populated into the read tier, proving the entry counter moves \
		 and that only the operator domain was excised"
	);
}

#[test]
fn operator_absence_is_never_claimed_from_memory() {
	// The inverse of the deleted completeness contract. The store must have no absence
	// authority for operator state: a row smuggled into SQLite behind its back must become
	// visible on the very next read. This is what makes it safe for the authority cache
	// above to treat a store miss as truth rather than as a possibly stale cache answer.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	persistent_only_set(&store, &k1, 5, "v5-a");

	assert_eq!(get(&store, &state_key(7, "probe"), 9), None, "the key genuinely does not exist yet");
	assert_eq!(get(&store, &k1, 9).as_deref(), Some(b"v5-a".as_slice()));

	let smuggled = state_key(7, "smuggled");
	persistent_only_set(&store, &smuggled, 5, "hidden");

	assert_eq!(
		get(&store, &smuggled, 9).as_deref(),
		Some(b"hidden".as_slice()),
		"an earlier absence probe must not have granted the store authority to deny this row"
	);
}

#[test]
fn operator_range_scan_reads_through_to_persistence() {
	// The inverse of the deleted serve-from-the-complete-page contract. Bypass-deleting a
	// row from SQLite must make it disappear from the scan, proving the scan consults
	// persistence every time rather than replaying a memory-resident page.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let ka = internal_key(9, "exp-a");
	let kb = internal_key(9, "exp-b");
	persistent_only_set(&store, &ka, 5, "a");
	persistent_only_set(&store, &kb, 5, "b");

	let before = range_keys(&store, FlowNodeStateKey::node_range(9.into()), 9);
	assert!(before.contains(&ka) && before.contains(&kb), "both internal rows must be scanned initially");

	persistent_only_delete(&store, &kb);

	let after = range_keys(&store, FlowNodeStateKey::node_range(9.into()), 9);
	assert!(after.contains(&ka), "the surviving row must still be scanned");
	assert!(
		!after.contains(&kb),
		"the scan must re-read persistence rather than serve a cached page (the row was bypass-deleted)"
	);
}

#[test]
fn operator_removal_removes_only_the_removed_key() {
	// TTL eviction removes operator keys continuously (join left TTL, window expiry). A removal
	// must take out exactly its own key and leave the node's other state readable.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	let k2 = state_key(7, "b");
	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k1.clone())], CommitVersion(8)).unwrap();

	assert_eq!(get(&store, &k1, 9), None, "the removed key must read as gone");
	assert_eq!(get(&store, &k2, 9).as_deref(), Some(b"v5-b".as_slice()), "a sibling key must be untouched");
}

#[test]
fn commit_tombstone_shadows_only_readers_above_it() {
	// An Unset commits a tombstone. Readers at or above the tombstone version see the
	// deletion; readers pinned below it must still resolve the persisted row, because a
	// deferred flow dispatch can be pinned below a tick that just deleted the key.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	persistent_only_set(&store, &k1, 5, "v5-a");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::remove_announced(k1.clone(), EncodedRow(CowVec::new(b"v5-a".to_vec())))],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k1, 9), None, "reader above the tombstone sees the deletion");
	assert_eq!(
		get(&store, &k1, 5).as_deref(),
		Some(b"v5-a".as_slice()),
		"reader below the tombstone must fall through and see the persisted row"
	);
}

#[test]
fn reader_pinned_between_versions_sees_the_older_row() {
	// The production interleaving: a deferred dispatch pinned at v10 reads a key the tick
	// just rewrote at v20. The read must resolve the v5 row that was visible at v10, not
	// the newer commit and not nothing. Previously the read buffer's previous-version slot
	// answered this; now it is the commit tier missing on version and the persistent tier
	// answering, which must produce the identical result.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = state_key(7, "hot");
	persistent_only_set(&store, &k, 5, "old");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(20),
	)
	.unwrap();

	assert_eq!(
		get(&store, &k, 10).as_deref(),
		Some(b"old".as_slice()),
		"a reader pinned between the persisted and the freshly committed version must see the older row"
	);
	assert_eq!(get(&store, &k, 25).as_deref(), Some(b"new".as_slice()), "a reader above sees the new row");
	assert_eq!(get(&store, &k, 4), None, "a reader below both versions must see nothing");
}

#[test]
fn range_scan_pinned_below_a_fresh_commit_yields_the_older_visible_row() {
	// The scan counterpart of the point read above. A scan pinned below a fresh commit
	// must still yield the key, resolved at the older visible version. Dropping the key
	// from the scan because only a newer version is in hand would silently shrink join
	// probe and window expiry scans.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let ka = internal_key(9, "exp-a");
	persistent_only_set(&store, &ka, 4, "old");

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: ka.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(20),
	)
	.unwrap();

	let keys = range_keys(&store, FlowNodeStateKey::node_range(9.into()), 10);
	assert!(keys.contains(&ka), "a scan below the newer commit must yield the older visible row");
}

fn persistent_row(store: &StandardMultiStore, k: &EncodedKey) -> Option<(u64, Vec<u8>)> {
	let persistent = store.persistent().expect("persistent tier configured");
	match persistent.get(classify_key(k), k.as_ref(), CommitVersion(u64::MAX)).unwrap() {
		reifydb_store_multi::tier::VersionedGetResult::Value {
			value,
			version,
		} => Some((version.0, value.to_vec())),
		_ => None,
	}
}

#[test]
fn operator_removal_leaves_the_persisted_row_for_the_reaper_not_the_commit() {
	// The commit writes a tombstone and stops; it does not reach into SQLite. The prior version
	// legitimately stays on disk as history below the tombstone, and collecting it is the tombstone
	// reaper's job. This matters most on operator state, which is the highest-churn removal path in
	// the system (join left TTL, window expiry): if the commit ever started deleting synchronously,
	// eviction throughput would collapse into the write connection.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = state_key(7, "reaped");
	persistent_only_set(&store, &k, 5, "v5");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(8)).unwrap();

	assert_eq!(
		persistent_row(&store, &k),
		Some((5, b"v5".to_vec())),
		"the commit must not delete from SQLite; the prior version stays as history below the tombstone"
	);
	assert_eq!(get(&store, &k, 9), None, "and the key is invisible above the tombstone regardless");
}

#[test]
fn operator_removal_then_recreate_resolves_per_read_version() {
	// The production interleaving the retired purge-race test was guarding, minus the purge: a key
	// removed at v8 and rewritten at v10 must give a reader pinned between the two the deletion, and
	// a reader above the recreate the new value. The retired version could only assert this while a
	// RAM mask and a deferred purge were both in flight; now it is plain MVCC resolution and the
	// recreate is in no danger from a background sweep bounded by the wrong version.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = state_key(7, "recreated");
	persistent_only_set(&store, &k, 5, "old");

	MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(k.clone())], CommitVersion(8)).unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(10),
	)
	.unwrap();

	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"old".as_slice()), "below the tombstone: the original row");
	assert_eq!(get(&store, &k, 9), None, "between removal and recreate: gone");
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()), "above the recreate: the new row");
}
