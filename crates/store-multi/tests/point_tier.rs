// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The point tier as the store actually reaches it. A read the tier answers must be counted as a hit, one
//! it answers from the displaced version must be counted apart, and one below every cached version must
//! read as a miss rather than as a hit the cache could not honour.

use std::collections::HashMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionGet, classify_key},
	},
	key::{EncodableKey, row::RowKey},
};
use reifydb_store_multi::{
	store::StandardMultiStore,
	tier::{TierStorage, point::MultiReadMetrics},
};
use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};

fn row_key(n: u64) -> EncodedKey {
	RowKey {
		storage: StorageId::Table(TableId(1)),
		row: RowNumber(n),
	}
	.encode()
}

fn persistent_only_set(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	let persistent = store.persistent().expect("persistent tier configured");
	let table = classify_key(k);
	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batches.entry(table).or_default().push((k.clone(), Some(CowVec::new(value.as_bytes().to_vec()))));
	persistent.set(CommitVersion(version), batches).unwrap();
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.bytes.to_vec())
}

fn reads(store: &StandardMultiStore) -> MultiReadMetrics {
	store.point_shard_metrics().into_iter().fold(MultiReadMetrics::default(), |mut acc, shard| {
		acc.hits += shard.reads.hits;
		acc.previous_hits += shard.reads.previous_hits;
		acc.misses += shard.reads.misses;
		acc
	})
}

#[test]
fn a_cold_read_populates_the_point_tier_and_the_next_read_is_served_by_it() {
	// Without this the whole reroute is unobservable: every assertion about values still passes when the
	// point tier is never consulted, because the persistent tier answers correctly on its own.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let key = row_key(1);
	persistent_only_set(&store, &key, 5, "five");

	assert_eq!(get(&store, &key, 7).as_deref(), Some(b"five".as_slice()));
	let cold = reads(&store);
	assert_eq!(cold.hits, 0, "the first read cannot be a hit, the key was never cached");

	assert_eq!(get(&store, &key, 7).as_deref(), Some(b"five".as_slice()));
	let warm = reads(&store);
	assert!(warm.hits > cold.hits, "the second read must be answered by the point tier, not by persistent again");
}

#[test]
fn a_read_below_every_cached_version_is_counted_a_miss_not_a_hit() {
	// The shared tier scores a hit on residency alone. If that leaked through, a reader the cache could
	// not answer would be indistinguishable from one it could, and cache pressure would read as health.
	// The key must be seated with both versions first, or this exercises a plain absence instead.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let key = row_key(2);
	store.insert_read_key(key.clone(), CommitVersion(20), Some(CowVec::new(b"twenty".to_vec())));
	store.insert_read_key(key.clone(), CommitVersion(30), Some(CowVec::new(b"thirty".to_vec())));

	assert_eq!(get(&store, &key, 30).as_deref(), Some(b"thirty".as_slice()));
	assert_eq!(get(&store, &key, 25).as_deref(), Some(b"twenty".as_slice()));
	let seated = reads(&store);
	assert_eq!(seated.previous_hits, 1, "the second read must come from the displaced version, or the entry never held two");

	let _ = get(&store, &key, 1);
	let after = reads(&store);

	assert_eq!(after.hits, seated.hits, "a version the cache cannot serve must not score a hit");
	assert_eq!(after.misses, seated.misses + 1, "a resident key with no visible version must score a miss");
}
