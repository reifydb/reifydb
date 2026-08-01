// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state read semantics through the multi store. Residency lives solely in the byte-bounded
//! authority cache above this layer, so the store must never populate the read tier with operator bytes
//! and never claim absence from memory. Some tests write SQLite directly to prove which tier answered.

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
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

fn read_tier_entries(store: &StandardMultiStore) -> usize {
	// These tests touch operator keys only, so any nonzero count means operator bytes leaked in here.
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
	// Every path that could back-populate the read buffer is exercised; a regression here caches
	// operator bytes twice, and the second copy is invisible to OperatorStateBudget.
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
	// Negative control: without a path that still populates the read tier, a broken tier or a dead
	// entry metric would let every zero-entry assertion above pass vacuously.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let storage = StorageId::Table(TableId(1));
	let row = RowKey::encoded(storage, 1);

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
	// The store holds no absence authority for operator state, which is what makes it safe for the
	// authority cache above to treat a store miss as truth rather than a possibly stale answer.
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
	// Bypass-deleting a row from SQLite must make it vanish from the scan, proving the scan consults
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
	// A deferred dispatch pinned at v10 reads a key the tick just rewrote at v20; it must resolve the
	// row visible at v10, not the newer commit and not nothing.
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
	// Dropping a key from a scan because only a newer version is in hand would silently shrink join
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
	// The commit writes a tombstone and stops; collecting the prior on-disk version is the reaper's
	// job. A synchronous delete here would collapse operator eviction throughput into the write
	// connection, and operator state is the highest-churn removal path in the system.
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
	// A key removed at v8 and rewritten at v10 must give a reader pinned between the two the deletion
	// and a reader above the recreate the new value - plain MVCC resolution, no purge involved.
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
