// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! MVCC correctness for the read buffer tier inside the tiered store. An entry holds the latest committed
//! `(version, value)` plus the one version it superseded, so a hit must never serve a value the requested
//! snapshot cannot see, and a newer commit must invalidate the entry rather than shadow it.

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore, tier::read::ReadBufferConfig};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn commit(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			bytes: EncodedBytes(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

fn persistent_only_set(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	// Writing only to the persistent tier leaves the key cold, so the next point read has to fall through.
	let persistent = store.persistent().expect("persistent tier configured");
	let table = classify_key(k);
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(table).or_default().push((k.clone(), Some(CowVec::new(value.as_bytes().to_vec()))));
	use reifydb_store_multi::tier::TierStorage;
	persistent.set(CommitVersion(version), batches).unwrap();
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.bytes.to_vec())
}

#[test]
fn cache_serves_cold_persistent_value_after_first_read_populates_it() {
	// The first read populates the cache from persistent; with no newer version for the key, every later
	// read at any snapshot at or above 5 must still resolve to v5.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("cold");

	persistent_only_set(&store, &k, 5, "v5");

	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
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

	assert_eq!(get(&store, &k, 4), None, "snapshot below the committed version must not observe it");
}

#[test]
fn commit_invalidates_a_stale_cached_value() {
	// A newer commit must invalidate the cached value while older snapshots, including one between the
	// two commits, still resolve to v5 rather than the new v8.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("k");

	persistent_only_set(&store, &k, 5, "v5");
	// Populate the cache with v5.
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));

	commit(&store, &k, 8, "v8");

	assert_eq!(get(&store, &k, 8).as_deref(), Some(b"v8".as_slice()));
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()));
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
	.map(|r| (r.key.to_vec(), r.bytes.to_vec()))
	.collect()
}

#[test]
fn range_scan_does_not_consult_the_read_tier() {
	// A point read never marks its bucket range-complete, and only range-complete buckets may serve a
	// scan; otherwise capacity eviction of a point-cached entry would silently change scan results.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let k = key("only_in_cache");

	persistent_only_set(&store, &k, 5, "v5");
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "point read populates the cache");

	let scanned = scan(&store, 5);
	assert!(
		scanned.iter().any(|(kk, vv)| kk == k.as_ref() && vv == b"v5"),
		"a persistent-backed key must appear in a range scan"
	);

	// Deleting from persistent leaves the value only in the cache, isolating what a scan can reach.
	let persistent = store.persistent().unwrap();
	let table = classify_key(&k);
	persistent.delete_keys(table, std::slice::from_ref(&k)).unwrap();
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "cache still answers point reads");

	let scanned_after = scan(&store, 5);
	assert!(
		!scanned_after.iter().any(|(kk, _)| kk == k.as_ref()),
		"a value present only in the read cache must never appear in a range scan"
	);
}

#[test]
fn capacity_eviction_of_a_cache_entry_never_changes_a_read_result() {
	// Capacity is a resident-page cap and a RAM trade only: a cap below the pages touched evicts on every read, and
	// every key must still resolve.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite_read(ReadBufferConfig {
		resident_pages: 1,
		shards: 1,
		..ReadBufferConfig::default()
	});
	let keys = ["a", "b", "c", "d"];
	for (i, name) in keys.iter().enumerate() {
		persistent_only_set(&store, &key(name), 5, &format!("val{i}"));
		assert_eq!(get(&store, &key(name), 5).as_deref(), Some(format!("val{i}").as_bytes()));
	}

	for (i, name) in keys.iter().enumerate() {
		assert_eq!(
			get(&store, &key(name), 5).as_deref(),
			Some(format!("val{i}").as_bytes()),
			"every key must still read correctly once cache entries have been evicted for capacity"
		);
	}
}
