// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Range serving through the page-resident read cache, exercised end-to-end through StandardMultiStore.
//!
//! The cache fronts the persistent tier's contribution to a range scan: once a Source bucket has been read
//! through often enough it is loaded whole (range_complete) and subsequent scans serve it from RAM. These tests
//! pin the load-bearing invariant - a cache-served scan is byte-for-byte identical to the cache-cold read-through
//! and to the data actually written - across warming, both scopes, commit/flush, reverse + pagination, physical
//! deletion, non-Source ranges, and mid-scan cache drops.

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::{EntryKind, MultiVersionCommit},
	},
	key::row::RowKey,
};
use reifydb_store_multi::{
	MultiVersionScope,
	store::StandardMultiStore,
	tier::{TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

const SHAPE: ShapeId = ShapeId::Table(TableId(1));

/// Enough rows in one bucket (default shift 16 keeps rows 0..65535 in bucket 0) to cross the warm threshold,
/// so the second scan actually serves from a range_complete page rather than reading through again.
const BUCKET_ROWS: u64 = 200;

fn store() -> (StandardMultiStore, impl Drop) {
	StandardMultiStore::testing_memory_with_persistent_sqlite()
}

fn commit(store: &StandardMultiStore, n: u64, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: RowKey::encoded(SHAPE, n),
			row: EncodedRow(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

/// Deterministic stand-in for the FlushActor sweep: persist the latest-<=cutoff value of every key to the
/// persistent tier, invalidate those keys in the read tier (clearing bucket completeness), then drop them from
/// the commit tier - the same persist -> invalidate-read -> drop ordering the actor runs.
fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
	let commit = store.commit().expect("commit tier configured");
	for kind in commit.list_all_entry_kinds().unwrap() {
		let (to_persist, to_drop) = match commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff),
		};
		if to_drop.is_empty() {
			continue;
		}
		if !to_persist.is_empty() {
			let persistent = store.persistent().expect("persistent tier configured");
			let mut by_version: HashMap<
				CommitVersion,
				HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>>,
			> = HashMap::new();
			for (key, version, value) in to_persist {
				by_version.entry(version).or_default().entry(kind).or_default().push((key, value));
			}
			for (version, batch) in by_version {
				persistent.set(version, batch).unwrap();
			}
		}
		for (key, _) in &to_drop {
			store.invalidate_read_key(key);
		}
		commit.drop(HashMap::from([(kind, to_drop)])).unwrap();
	}
}

fn scan_fwd(store: &StandardMultiStore, read_version: u64, batch: usize) -> Vec<(Vec<u8>, Vec<u8>, CommitVersion)> {
	scan_scope(
		store,
		MultiVersionScope::AsOf {
			read: CommitVersion(read_version),
		},
		batch,
	)
}

fn scan_scope(
	store: &StandardMultiStore,
	scope: MultiVersionScope,
	batch: usize,
) -> Vec<(Vec<u8>, Vec<u8>, CommitVersion)> {
	store.range(RowKey::full_scan(SHAPE), scope, batch)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|r| (r.key.to_vec(), r.row.to_vec(), r.version))
		.collect()
}

fn keys_only(rows: &[(Vec<u8>, Vec<u8>, CommitVersion)]) -> Vec<Vec<u8>> {
	rows.iter().map(|(k, _, _)| k.clone()).collect()
}

#[test]
fn warm_then_serve_matches_cold_readthrough_and_truth() {
	// The equivalence gate: a first (cold, mostly read-through) scan warms the bucket; a second scan serves it
	// from the range_complete page. Both must be byte-for-byte identical to each other AND to the data written,
	// in the same ascending-encoded key order a SQLite scan produces.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));

	let cold = scan_fwd(&store, 1000, 64);
	let warm = scan_fwd(&store, 1000, 64);

	assert_eq!(cold, warm, "the cache-warm scan must be byte-for-byte identical to the cache-cold read-through");
	assert_eq!(cold.len(), BUCKET_ROWS as usize);

	let mut sorted_keys = keys_only(&cold);
	let mut expected_keys: Vec<Vec<u8>> = (1..=BUCKET_ROWS).map(|n| RowKey::encoded(SHAPE, n).to_vec()).collect();
	expected_keys.sort();
	sorted_keys.sort();
	assert_eq!(sorted_keys, expected_keys, "every written key, exactly once");

	let got: HashMap<Vec<u8>, Vec<u8>> = cold.iter().map(|(k, v, _)| (k.clone(), v.clone())).collect();
	for n in 1..=BUCKET_ROWS {
		assert_eq!(
			got.get(&RowKey::encoded(SHAPE, n).to_vec()).map(|v| v.as_slice()),
			Some(format!("v{n}").as_bytes())
		);
	}

	assert_eq!(
		keys_only(&cold),
		{
			let mut s = keys_only(&cold);
			s.sort();
			s
		},
		"rows must already be in ascending-encoded order"
	);
}

#[test]
fn asof_and_between_match_after_warm() {
	// Both scopes must survive a warm/serve round-trip: the cache applies the same scope.contains predicate the
	// persistent tier does, including the Between lower bound.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 3, &format!("v{n}"));
	}
	flush(&store, CommitVersion(3));

	for scope in [
		MultiVersionScope::AsOf {
			read: CommitVersion(10),
		},
		MultiVersionScope::Between {
			after: CommitVersion(2),
			read: CommitVersion(10),
		},
		MultiVersionScope::Between {
			after: CommitVersion(3),
			read: CommitVersion(10),
		},
	] {
		let cold = scan_scope(&store, scope, 64);
		let warm = scan_scope(&store, scope, 64);
		assert_eq!(cold, warm, "scope {scope:?} must be identical warm vs cold");
	}

	// (2,10] admits the committed version 3 -> all rows; (3,10] excludes version 3 -> none.
	assert_eq!(
		scan_scope(
			&store,
			MultiVersionScope::Between {
				after: CommitVersion(2),
				read: CommitVersion(10)
			},
			64
		)
		.len(),
		BUCKET_ROWS as usize
	);
	assert!(
		scan_scope(
			&store,
			MultiVersionScope::Between {
				after: CommitVersion(3),
				read: CommitVersion(10)
			},
			64
		)
		.is_empty(),
		"Between lower bound must exclude version 3"
	);
}

#[test]
fn commit_after_warm_serves_newer_value_and_keeps_others() {
	// After a bucket is warm, a fresh commit lands in the commit buffer and invalidates that key (clearing
	// completeness). The always-scanned commit buffer must win on version, so the new value shows up and the
	// cache can never mask it - even though the page is re-warmed from the (older) persistent state mid-scan.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm

	commit(&store, 5, 5, "updated");

	// Single batch (>= the row count) so the whole scan resolves in one range_next call: this isolates the
	// cache invariant (a warm complete page must never mask a newer commit) from an unrelated pre-existing
	// cold-merge horizon defect that drops a sparse commit's contribution across batch boundaries.
	let rows = scan_fwd(&store, 1000, (BUCKET_ROWS as usize) + 64);
	let by_key: HashMap<Vec<u8>, (Vec<u8>, CommitVersion)> =
		rows.iter().map(|(k, v, ver)| (k.clone(), (v.clone(), *ver))).collect();

	assert_eq!(rows.len(), BUCKET_ROWS as usize);
	assert_eq!(
		by_key.get(&RowKey::encoded(SHAPE, 5).to_vec()),
		Some(&(b"updated".to_vec(), CommitVersion(5))),
		"the newer committed value must win over the cached persistent value"
	);
	assert_eq!(
		by_key.get(&RowKey::encoded(SHAPE, 6).to_vec()),
		Some(&(b"v6".to_vec(), CommitVersion(1))),
		"untouched keys keep their persisted value"
	);
}

#[test]
fn flush_after_commit_returns_persisted_state() {
	// A value committed then flushed must be served as the persisted state; the flush sweep clears the bucket's
	// completeness so the next scan re-reads the new persisted version.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm

	commit(&store, 5, 5, "persisted-update");
	flush(&store, CommitVersion(5));

	let rows = scan_fwd(&store, 1000, 64);
	let by_key: HashMap<Vec<u8>, (Vec<u8>, CommitVersion)> =
		rows.iter().map(|(k, v, ver)| (k.clone(), (v.clone(), *ver))).collect();
	assert_eq!(
		by_key.get(&RowKey::encoded(SHAPE, 5).to_vec()),
		Some(&(b"persisted-update".to_vec(), CommitVersion(5))),
		"the flushed value must be served from the persistent tier"
	);
	assert_eq!(rows.len(), BUCKET_ROWS as usize);
}

#[test]
fn reverse_and_small_batch_match_forward() {
	// Reverse serving and small-batch pagination must walk the cache without duplicating or skipping a key at
	// any chunk or bucket boundary: reverse == forward reversed, and a tiny batch == a big batch.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm

	let forward = keys_only(&scan_fwd(&store, 1000, 64));

	let mut reverse: Vec<Vec<u8>> = store
		.range_rev(
			RowKey::full_scan(SHAPE),
			MultiVersionScope::AsOf {
				read: CommitVersion(1000),
			},
			7,
		)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|r| r.key.to_vec())
		.collect();
	reverse.reverse();
	assert_eq!(forward, reverse, "reverse scan must equal the forward scan reversed");

	let small_batch = keys_only(&scan_fwd(&store, 1000, 7));
	assert_eq!(forward, small_batch, "small-batch pagination must match the single-batch order");
	assert_eq!(forward.len(), BUCKET_ROWS as usize);
}

#[test]
fn physical_delete_then_range_omits_row_no_ghost() {
	// A row physically removed from the persistent tier must never be resurrected by a stale complete page.
	// Delete-then-invalidate (the ordering the drop/TTL paths use) clears completeness so the scan reads
	// through and omits the row.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm bucket 0 complete

	let removed = RowKey::encoded(SHAPE, 5);
	store.persistent().unwrap().delete_keys(EntryKind::Source(SHAPE), &[removed.clone()]).unwrap();
	store.invalidate_read_key(&removed);

	let rows = scan_fwd(&store, 1000, 64);
	assert_eq!(rows.len(), (BUCKET_ROWS - 1) as usize, "exactly one row removed");
	assert!(
		!keys_only(&rows).contains(&removed.to_vec()),
		"the physically-deleted row must not be served from a stale complete page"
	);
}

#[test]
fn non_source_range_reads_through_with_warm_cache() {
	// The cache only ever serves Source buckets (the EntryKind::Source guard in step_persistent_cached). Keys
	// committed under a non-RowKey (Multi) classification live in a separate persistent table; the unbounded
	// range classifies as Multi and must return exactly those, untouched by a warm Source cache.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	let multi_keys: Vec<EncodedKey> = (0u8..5).map(|i| EncodedKey::new(vec![0x00, i])).collect();
	for (i, key) in multi_keys.iter().enumerate() {
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: key.clone(),
				row: EncodedRow(CowVec::new(format!("m{i}").into_bytes())),
			}],
			CommitVersion(1),
		)
		.unwrap();
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm the Source bucket

	let mut got: Vec<Vec<u8>> = store
		.range(
			EncodedKeyRange::all(),
			MultiVersionScope::AsOf {
				read: CommitVersion(1000),
			},
			64,
		)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|r| r.key.to_vec())
		.collect();
	got.sort();

	let mut expected: Vec<Vec<u8>> = multi_keys.iter().map(|k| k.to_vec()).collect();
	expected.sort();
	assert_eq!(
		got, expected,
		"a non-Source range returns only its own Multi rows, unaffected by the warm Source cache"
	);
}

#[test]
fn cache_cleared_mid_scan_reads_through_without_corruption() {
	// Eviction of a complete page is purely a RAM trade: dropping the cache mid-scan must turn cache hits into
	// read-throughs with no duplicated, skipped, or corrupted row.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit(&store, n, 1, &format!("v{n}"));
	}
	flush(&store, CommitVersion(1));
	let _ = scan_fwd(&store, 1000, 64); // warm

	let mut it = store.range(
		RowKey::full_scan(SHAPE),
		MultiVersionScope::AsOf {
			read: CommitVersion(1000),
		},
		16,
	);
	let mut all: Vec<Vec<u8>> = Vec::new();
	for r in it.by_ref().take(40) {
		all.push(r.unwrap().key.to_vec());
	}
	store.clear_read(); // drop the warm pages mid-scan
	for r in it {
		all.push(r.unwrap().key.to_vec());
	}

	let mut expected: Vec<Vec<u8>> = (1..=BUCKET_ROWS).map(|n| RowKey::encoded(SHAPE, n).to_vec()).collect();
	expected.sort();
	let mut sorted = all.clone();
	sorted.sort();
	assert_eq!(sorted, expected, "no row lost or corrupted by a mid-scan cache clear");
	assert_eq!(all.len(), BUCKET_ROWS as usize, "no duplicated row across the eviction boundary");
}
