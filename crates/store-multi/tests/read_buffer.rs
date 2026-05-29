// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! MVCC correctness for the read buffer tier wired into the tiered store.
//!
//! The read buffer only ever holds the latest committed value per key and only sits between the commit buffer and
//! the persistent tier on point reads. These tests pin the three invariants that keep it from violating snapshot
//! isolation: a hit serves the right value, a new commit invalidates the stale entry, and capacity eviction only
//! ever forces a (correct) read-through, never a wrong answer.

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

fn commit(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

/// Write a value ONLY to the persistent tier so it is cold from the commit buffer's perspective. A point read
/// then misses the buffer, misses the empty cache, hits persistent, and populates the cache.
fn persistent_only_set(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	let persistent = store.persistent().expect("persistent tier configured");
	let table = classify_key(k);
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(table).or_default().push((k.clone(), Some(CowVec::new(value.as_bytes().to_vec()))));
	use reifydb_store_multi::tier::TierStorage;
	persistent.set(CommitVersion(version), batches).unwrap();
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.row.to_vec())
}

#[test]
fn cache_serves_cold_persistent_value_after_first_read_populates_it() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("cold");

	persistent_only_set(&store, &k, 5, "v5");

	// First read: buffer miss -> cache miss -> persistent hit (populates cache).
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
	// Second read at the same snapshot: must still be v5 (now served from the cache).
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
	// A reader at a higher snapshot also resolves to v5: no newer version exists for this key.
	assert_eq!(get(&store, &k, 9).as_deref(), Some(b"v5".as_slice()));
}

#[test]
fn cache_miss_below_stored_version_does_not_leak_a_newer_value() {
	// The persistent current is v5. A reader whose snapshot is below 5 must NOT see v5. The cache must
	// decline to serve (stored_version > requested), and persistent's version guard returns NotFound too.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("k");
	persistent_only_set(&store, &k, 5, "v5");

	// Prime the cache at v5.
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));

	// A snapshot predating the commit must see nothing.
	assert_eq!(get(&store, &k, 4), None, "snapshot below the committed version must not observe it");
}

#[test]
fn commit_invalidates_a_stale_cached_value() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("k");

	persistent_only_set(&store, &k, 5, "v5");
	// Populate the cache with v5.
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));

	// A new commit at v8 lands in the buffer and must invalidate the cached v5.
	commit(&store, &k, 8, "v8");

	// Reader at v8 sees the new value from the buffer.
	assert_eq!(get(&store, &k, 8).as_deref(), Some(b"v8".as_slice()));
	// Reader at v5 still correctly resolves to v5 (older snapshot, value unchanged at that point).
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
	// Reader at v7 (between the two commits) must resolve to v5, not v8.
	assert_eq!(get(&store, &k, 7).as_deref(), Some(b"v5".as_slice()));
}

#[test]
fn buffer_shadows_cache_for_freshly_committed_keys() {
	// A key written through the normal commit path lives in the buffer; the buffer is consulted first, so
	// the cache can never shadow a fresher buffered value.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("k");

	commit(&store, &k, 3, "v3");
	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()));

	commit(&store, &k, 6, "v6");
	assert_eq!(get(&store, &k, 6).as_deref(), Some(b"v6".as_slice()));
	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()));
}

/// Drain a forward range scan into (key, value) pairs at the given snapshot.
fn scan(store: &StandardMultiStore, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	store.range(
		EncodedKeyRange::all(),
		MultiVersionScope::AsOf {
			read: CommitVersion(version),
		},
		1024,
	)
	.collect::<Result<Vec<_>, _>>()
	.unwrap()
	.into_iter()
	.map(|r| (r.key.to_vec(), r.row.to_vec()))
	.collect()
}

#[test]
fn range_scan_does_not_consult_the_read_tier() {
	// The read tier is a POINT-read cache only. Range scans merge the commit and persistent tiers directly; a
	// value that lives ONLY in the read tier (never written to persistent) must be invisible to a scan. If a
	// scan ever consulted the read tier, capacity eviction of a cached entry would silently change scan
	// results, so the cache must be strictly bypassed here.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("only_in_cache");

	// Populate the persistent tier and prime the read cache via a point read.
	persistent_only_set(&store, &k, 5, "v5");
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "point read populates the cache");

	// A scan must see the persistent-backed value (it is in persistent), proving scans reach persistent.
	let scanned = scan(&store, 5);
	assert!(
		scanned.iter().any(|(kk, vv)| kk == k.as_ref() && vv == b"v5"),
		"a persistent-backed key must appear in a range scan"
	);

	// Now invalidate persistent's authority by removing the key from persistent but leaving a cache entry.
	// A subsequent scan must NOT surface the stale cached value, because scans never read the cache.
	let persistent = store.persistent().unwrap();
	let table = classify_key(&k);
	persistent.delete_keys(table, std::slice::from_ref(&k)).unwrap();
	// The cache still holds v5 from the earlier point read; prove it.
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "cache still answers point reads");

	let scanned_after = scan(&store, 5);
	assert!(
		!scanned_after.iter().any(|(kk, _)| kk == k.as_ref()),
		"a value present only in the read cache must never appear in a range scan"
	);
}

#[test]
fn capacity_eviction_of_a_cache_entry_never_changes_a_read_result() {
	// Shrinking the read buffer to capacity 1 forces eviction of all but one cached entry. Every key must
	// still read correctly afterwards: an evicted entry simply falls through to persistent. Capacity is a
	// RAM/CPU trade and must never affect correctness (parity with a never-cached store).
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let keys = ["a", "b", "c", "d"];
	for (i, name) in keys.iter().enumerate() {
		persistent_only_set(&store, &key(name), 5, &format!("val{i}"));
		assert_eq!(get(&store, &key(name), 5).as_deref(), Some(format!("val{i}").as_bytes()));
	}

	// Force eviction down to a single slot.
	store.configure_read_buffer_capacity(1);

	for (i, name) in keys.iter().enumerate() {
		assert_eq!(
			get(&store, &key(name), 5).as_deref(),
			Some(format!("val{i}").as_bytes()),
			"every key must still read correctly after the read buffer is shrunk and entries evicted"
		);
	}
}
