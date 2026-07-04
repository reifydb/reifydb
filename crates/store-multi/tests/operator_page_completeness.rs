// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator page completeness in the read buffer tier.
//!
//! Production flow workloads probe operator state keys that do not exist yet (new window
//! buckets, missing join partners) on nearly every apply. Absence used to be provable only
//! by the persistent SQLite tier, which put ~1,000 synchronous SQLite reads per second on
//! the serialized flow actor (jupiter incident, 2026-07-04). These tests pin the fix: the
//! first persistent fall-through for an operator node bulk-loads the node's page and marks
//! it range-complete, after which point reads, absence probes, and range scans are served
//! from memory, and TTL drops keep the page complete instead of resetting it.
//!
//! Several tests write or delete rows in the SQLite tier directly, bypassing the store, to
//! prove that a read is answered by the page and never falls through to SQLite. That
//! bypass violates the tier mirror on purpose; each such test states what it proves.

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
	key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore, tier::TierStorage};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn state_key(node: u64, suffix: &str) -> EncodedKey {
	FlowNodeStateKey::encoded(node, suffix.as_bytes().to_vec())
}

fn internal_key(node: u64, suffix: &str) -> EncodedKey {
	FlowNodeInternalStateKey::encoded(node, suffix.as_bytes().to_vec())
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

#[test]
fn first_operator_miss_warms_the_node_page_and_serves_from_memory() {
	// The first persistent fall-through for a node must load the whole node page, after
	// which values AND absence are served from memory. Proven by deleting the SQLite rows
	// behind the store's back: reads keep working, so nothing falls through anymore.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	let k2 = state_key(7, "b");

	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	// Absent-key probe: triggers the warm, returns nothing.
	assert_eq!(get(&store, &state_key(7, "missing"), 9), None);

	persistent_only_delete(&store, &k1);
	persistent_only_delete(&store, &k2);

	assert_eq!(
		get(&store, &k1, 9).as_deref(),
		Some(b"v5-a".as_slice()),
		"a value read after the warm must be served from the page, not SQLite (row was bypass-deleted)"
	);
	assert_eq!(get(&store, &k2, 9).as_deref(), Some(b"v5-b".as_slice()));
}

#[test]
fn absence_is_served_from_the_complete_page_without_consulting_sqlite() {
	// Once a node page is complete, a key missing from the page is definitively absent.
	// Proven by inserting a row into SQLite behind the store's back: the store must keep
	// answering "no row" because the complete page is authoritative for this node.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	persistent_only_set(&store, &k1, 5, "v5-a");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);
	assert_eq!(get(&store, &k1, 9).as_deref(), Some(b"v5-a".as_slice()));

	let smuggled = state_key(7, "smuggled");
	persistent_only_set(&store, &smuggled, 5, "hidden");

	assert_eq!(
		get(&store, &smuggled, 9),
		None,
		"an absence probe on a complete page must not fall through to SQLite"
	);
}

#[test]
fn operator_drop_keeps_the_node_page_complete() {
	// TTL eviction drops operator keys continuously (join left TTL, window expiry). A drop
	// deletes the row from SQLite and must remove only that entry from the page, keeping
	// completeness, otherwise every eviction pass would reset the cache and reintroduce
	// the per-apply SQLite probes the completeness fix exists to remove.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	let k2 = state_key(7, "b");
	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Drop {
			key: k1.clone(),
		}],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k1, 9), None, "the dropped key must read as gone");
	assert_eq!(get(&store, &k2, 9).as_deref(), Some(b"v5-b".as_slice()));

	let smuggled = state_key(7, "smuggled");
	persistent_only_set(&store, &smuggled, 5, "hidden");
	assert_eq!(
		get(&store, &smuggled, 9),
		None,
		"the page must still be complete after the drop: absence must not consult SQLite"
	);
}

#[test]
fn commit_tombstone_preserves_completeness_and_older_snapshots_fall_through() {
	// An Unset commits a tombstone. The page must record the tombstone (not evict the
	// entry), staying complete, while readers below the tombstone version still fall
	// through to SQLite and see the old row.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	persistent_only_set(&store, &k1, 5, "v5-a");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Unset {
			key: k1.clone(),
			row: EncodedRow(CowVec::new(b"v5-a".to_vec())),
		}],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k1, 9), None, "reader above the tombstone sees the deletion");
	assert_eq!(
		get(&store, &k1, 5).as_deref(),
		Some(b"v5-a".as_slice()),
		"reader below the tombstone must fall through and see the persisted row"
	);

	let smuggled = state_key(7, "smuggled");
	persistent_only_set(&store, &smuggled, 5, "hidden");
	assert_eq!(get(&store, &smuggled, 9), None, "tombstoning must not reset completeness");
}

#[test]
fn operator_range_scan_is_served_from_the_complete_page() {
	// Join probes and window expiry scans are prefix ranges over internal state. Once the
	// internal page is complete they must be answered from memory. Proven by bypass-deleting
	// a row from SQLite: the scan still returns it.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let ka = internal_key(9, "exp-a");
	let kb = internal_key(9, "exp-b");
	persistent_only_set(&store, &ka, 5, "a");
	persistent_only_set(&store, &kb, 5, "b");

	assert_eq!(get(&store, &internal_key(9, "warm-trigger"), 9), None);

	persistent_only_delete(&store, &kb);

	let keys = range_keys(&store, FlowNodeInternalStateKey::node_range(9.into()), 9);
	assert!(keys.contains(&ka), "range scan must include the first internal row");
	assert!(
		keys.contains(&kb),
		"range scan must be served from the complete page (the row was bypass-deleted from SQLite)"
	);
}

#[test]
fn operator_range_scan_falls_back_when_a_cached_entry_is_newer_than_the_scope() {
	// The page holds only the latest version per key. A scan pinned below a fresh commit
	// must not trust the page for that key, because SQLite may still hold an older visible
	// version. The serve must fall back to SQLite instead of silently omitting the key.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let ka = internal_key(9, "exp-a");
	persistent_only_set(&store, &ka, 4, "old");

	assert_eq!(get(&store, &internal_key(9, "warm-trigger"), 9), None);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: ka.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(20),
	)
	.unwrap();

	let keys = range_keys(&store, FlowNodeInternalStateKey::node_range(9.into()), 10);
	assert!(
		keys.contains(&ka),
		"a scan below the cached version must fall back to SQLite and yield the older visible row"
	);
}

#[test]
fn oversized_node_page_never_marks_complete_and_keeps_falling_through() {
	// The warm cap guards RSS: a node whose persisted state exceeds the cap (e.g. a
	// future join shipped without a TTL again) must not be mirrored into memory and must
	// not gain absence authority. Proven by smuggling a row into SQLite after the warm
	// attempt: the store must still find it, i.e. reads keep falling through.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let persistent = store.persistent().expect("persistent tier configured");

	let mut entries: Vec<(EncodedKey, Option<CowVec<u8>>)> = Vec::new();
	for i in 0..131_073u32 {
		entries.push((state_key(3, &format!("k{i:06}")), Some(CowVec::new(b"x".to_vec()))));
	}
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.insert(classify_key(&state_key(3, "k000000")), entries);
	persistent.set(CommitVersion(2), batches).unwrap();

	assert_eq!(get(&store, &state_key(3, "zz-missing"), 9), None);

	let smuggled = state_key(3, "zz-smuggled");
	persistent_only_set(&store, &smuggled, 5, "hidden");
	assert_eq!(
		get(&store, &smuggled, 9).as_deref(),
		Some(b"hidden".as_slice()),
		"an oversized node must keep reading through to SQLite instead of claiming absence"
	);
}

#[test]
fn pinned_reader_between_versions_is_served_from_the_previous_slot() {
	// The production residual: a deferred dispatch pinned at v10 reads a key the
	// tick just rewrote at v20. The commit-time write-through supersedes the cached
	// entry in place, keeping v5 in the previous slot, so the read is answered from
	// memory. Proven by bypass-deleting the SQLite row: without the previous slot
	// this read has nowhere to fall through to and would come back empty.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = state_key(7, "hot");
	persistent_only_set(&store, &k, 5, "old");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);
	assert_eq!(get(&store, &k, 9).as_deref(), Some(b"old".as_slice()));

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(20),
	)
	.unwrap();

	persistent_only_delete(&store, &k);

	assert_eq!(
		get(&store, &k, 10).as_deref(),
		Some(b"old".as_slice()),
		"a reader pinned between the persisted and the freshly committed version must be served the previous slot from memory"
	);
	assert_eq!(get(&store, &k, 25).as_deref(), Some(b"new".as_slice()));
	assert_eq!(get(&store, &k, 4), None, "a reader below both versions must see nothing");
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

fn wait_until_persistent_gone(store: &StandardMultiStore, k: &EncodedKey) {
	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
	while persistent_row(store, k).is_some() {
		assert!(
			std::time::Instant::now() < deadline,
			"the drop actor did not purge the persisted row within the deadline"
		);
		std::thread::sleep(std::time::Duration::from_millis(10));
	}
}

#[test]
fn operator_drop_masks_immediately_and_purges_persistence_in_the_background() {
	// A drop leaves the commit buffer clean at once and masks the stale persisted
	// row with a read-cache tombstone, so readers at or above the drop version see
	// the key gone from the moment the commit returns while the commit path itself
	// performs no SQLite work. The drop actor purges the row within its flush
	// interval. Completeness survives both the mask and the purge (smuggle proof).
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k1 = state_key(7, "a");
	let k2 = state_key(7, "b");
	persistent_only_set(&store, &k1, 5, "v5-a");
	persistent_only_set(&store, &k2, 5, "v5-b");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Drop {
			key: k1.clone(),
		}],
		CommitVersion(8),
	)
	.unwrap();

	assert_eq!(get(&store, &k1, 9), None, "the dropped key must read as gone immediately after commit");
	assert!(
		persistent_row(&store, &k1).is_some(),
		"the commit path must not touch SQLite; the stale row is masked, not deleted"
	);

	wait_until_persistent_gone(&store, &k1);

	assert_eq!(get(&store, &k1, 9), None);
	assert_eq!(get(&store, &k2, 9).as_deref(), Some(b"v5-b".as_slice()));

	let smuggled = state_key(7, "smuggled-purge");
	persistent_only_set(&store, &smuggled, 5, "hidden");
	assert_eq!(get(&store, &smuggled, 9), None, "the page must still be complete after the purge");
}

#[test]
fn drop_then_recreate_survives_the_background_purge() {
	// The race the version guard exists for: a key is dropped at v8, recreated at
	// v10, and the recreated row reaches SQLite (direct write standing in for the
	// flush) before the deferred purge runs. The purge is bounded by the drop
	// version and must leave the newer row alone; the mask tombstone lives on in
	// the previous slot, so a reader pinned between drop and recreate still sees
	// the key gone. The sentinel key shares the drop commit, so its disappearance
	// proves the purge batch containing both keys has been processed.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = state_key(7, "recreated");
	let sentinel = state_key(7, "sentinel");
	persistent_only_set(&store, &k, 5, "old");
	persistent_only_set(&store, &sentinel, 5, "old");

	assert_eq!(get(&store, &state_key(7, "warm-trigger"), 9), None);

	MultiVersionCommit::commit(
		&store,
		cow_vec![
			Delta::Drop {
				key: k.clone(),
			},
			Delta::Drop {
				key: sentinel.clone(),
			}
		],
		CommitVersion(8),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(b"new".to_vec())),
		}],
		CommitVersion(10),
	)
	.unwrap();

	assert_eq!(get(&store, &k, 9), None, "a reader pinned between drop and recreate sees the mask");
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()));

	persistent_only_set(&store, &k, 10, "new");

	wait_until_persistent_gone(&store, &sentinel);

	assert_eq!(
		persistent_row(&store, &k),
		Some((10, b"new".to_vec())),
		"the deferred purge must not remove a row newer than the drop version"
	);
	assert_eq!(get(&store, &k, 15).as_deref(), Some(b"new".as_slice()));
}

#[test]
fn tombstone_purge_is_bounded_by_the_drop_version() {
	// The purge primitive itself: deleting through the drop version must leave a
	// newer row alone (a tombstone flushed while a later recreate is already
	// persisted must not destroy the recreate).
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let persistent = store.persistent().expect("persistent tier configured");
	let k = state_key(7, "guarded");
	persistent_only_set(&store, &k, 10, "newer");

	let purged = persistent
		.delete_keys_through(classify_key(&k), std::slice::from_ref(&(k.clone(), CommitVersion(8))))
		.unwrap();
	assert_eq!(purged, 0, "a row newer than the purge bound must survive");
	assert_eq!(persistent_row(&store, &k), Some((10, b"newer".to_vec())));

	let purged = persistent
		.delete_keys_through(classify_key(&k), std::slice::from_ref(&(k.clone(), CommitVersion(10))))
		.unwrap();
	assert_eq!(purged, 1, "a row at the purge bound is dropped state and must go");
	assert_eq!(persistent_row(&store, &k), None);
}

#[test]
fn warmed_node_survives_a_full_sqlite_blackout() {
	// The performance contract behind the completeness and two-version cache work: once a
	// node's page is warm and the flush has settled, steady-state reads must not depend on
	// SQLite at all. Wiping the entire persistent operator table behind the store's back
	// makes any reintroduced per-read fall-through return wrong answers (absent rows)
	// instead of silently regressing latency.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let table = classify_key(&state_key(7, "a"));

	for (suffix, version) in [("a", 5u64), ("b", 6), ("c", 7)] {
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: state_key(7, suffix),
				row: EncodedRow(CowVec::new(format!("val-{suffix}").into_bytes())),
			}],
			CommitVersion(version),
		)
		.unwrap();
	}

	store.flush_all_blocking();

	assert_eq!(get(&store, &state_key(7, "missing"), 9), None);
	for suffix in ["a", "b", "c"] {
		assert_eq!(
			get(&store, &state_key(7, suffix), 9).as_deref(),
			Some(format!("val-{suffix}").as_bytes()),
			"warm-up read must serve the committed value"
		);
	}

	store.persistent().expect("persistent tier configured").clear_table(table).unwrap();

	for suffix in ["a", "b", "c"] {
		assert_eq!(
			get(&store, &state_key(7, suffix), 9).as_deref(),
			Some(format!("val-{suffix}").as_bytes()),
			"point reads must be served from memory during the blackout"
		);
	}
	assert_eq!(get(&store, &state_key(7, "missing"), 9), None, "absence must be served from memory");

	let keys: Vec<EncodedKey> = ["a", "b", "missing", "c"].iter().map(|suffix| state_key(7, suffix)).collect();
	let many = store.get_many(&keys, CommitVersion(9)).unwrap();
	assert_eq!(many.len(), 3, "get_many must serve all live rows from memory during the blackout");

	let scanned: Vec<EncodedKey> = store
		.range(
			FlowNodeStateKey::node_range(7.into()),
			reifydb_store_multi::MultiVersionScope::AsOf {
				read: CommitVersion(9),
			},
			16,
		)
		.map(|r| r.unwrap().key)
		.collect();
	assert_eq!(scanned.len(), 3, "range scans must be served from the complete page during the blackout");
}
