// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{EntryKind, MultiVersionCommit},
};
use reifydb_store_multi::{
	MultiStore, MultiVersionScope,
	store::StandardMultiStore,
	tier::{RangeCursor, TierStorage, persistent::MultiPersistentTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn key(label: &[u8]) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + label.len());
	bytes.push(0x00);
	bytes.extend_from_slice(label);
	EncodedKey::new(bytes)
}

fn row(label: &[u8]) -> EncodedRow {
	let mut bytes = Vec::with_capacity(1 + label.len());
	bytes.push(b'v');
	bytes.extend_from_slice(label);
	EncodedRow(CowVec::new(bytes))
}

fn write(store: &MultiStore, k: &EncodedKey, payload: &[u8], version: CommitVersion) {
	let row = row(payload);
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row
		}],
		version,
	)
	.unwrap();
}

#[test]
fn single_key_scope_filter() {
	let store = MultiStore::testing_memory();
	let k = key(b"K");

	write(&store, &k, b"v1", CommitVersion(1));
	write(&store, &k, b"v5", CommitVersion(5));
	write(&store, &k, b"v10", CommitVersion(10));

	// AsOf { read: 20 } -> highest visible (v=10).
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

	// Between { after: 5, read: 20 } -> still v=10 (10 > 5).
	let scope = MultiVersionScope::Between {
		after: CommitVersion(5),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

	// Between { after: 10, read: 20 } -> nothing (10 is excluded; v=5,1 are also).
	let scope = MultiVersionScope::Between {
		after: CommitVersion(10),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert!(rows.is_empty(), "expected no rows above watermark = 10, got {rows:?}");

	// Between { after: 4, read: 20 } -> v=10 (highest in (4, 20]).
	let scope = MultiVersionScope::Between {
		after: CommitVersion(4),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

	// Between { after: 4, read: 7 } -> v=5 (highest in (4, 7]).
	let scope = MultiVersionScope::Between {
		after: CommitVersion(4),
		read: CommitVersion(7),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(5));
}

#[test]
fn multi_key_independent_filter() {
	let store = MultiStore::testing_memory();
	let a = key(b"A");
	let b = key(b"B");

	write(&store, &a, b"a2", CommitVersion(2));
	write(&store, &a, b"a8", CommitVersion(8));
	write(&store, &b, b"b4", CommitVersion(4));
	write(&store, &b, b"b12", CommitVersion(12));

	let scope = MultiVersionScope::Between {
		after: CommitVersion(5),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();

	assert_eq!(rows.len(), 2, "expected one row per key");
	assert_eq!(rows[0].key, a);
	assert_eq!(rows[0].version, CommitVersion(8));
	assert_eq!(rows[1].key, b);
	assert_eq!(rows[1].version, CommitVersion(12));
}

#[test]
fn skipped_versions_do_not_consume_batch_budget() {
	// Each key has versions 1 and 11. With after=5, only v=11 qualifies per key.
	// Insert N keys. Run with batch_size = N. Assert exactly N rows back, proving
	// that the v=1 entries are skipped at the tier level (never reach the
	// `collected` BTreeMap in store/multi.rs::scan_tier_chunk).
	const N: usize = 5;
	let store = MultiStore::testing_memory();

	let keys: Vec<_> = (0..N).map(|i| key(&[b'K', i as u8])).collect();
	for k in &keys {
		write(&store, k, b"old", CommitVersion(1));
		write(&store, k, b"new", CommitVersion(11));
	}

	let scope = MultiVersionScope::Between {
		after: CommitVersion(5),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, N).collect::<Result<_, _>>().unwrap();

	assert_eq!(rows.len(), N, "batch should fit all N qualifying keys");
	for r in &rows {
		assert_eq!(r.version, CommitVersion(11));
	}
}

#[test]
fn sqlite_tier_parity_single_key() {
	// Drive the SQLite path via TierStorage directly. Verifies that the persistent
	// (single-version-per-key) SQLite tier honors MultiVersionScope filtering for both
	// the SQL `version <= read` predicate AND the Rust-side `scope.contains` check
	// (the `v > after` lower bound for Between). The persistent tier overwrites on
	// every `set`, so this test installs distinct keys at distinct versions rather
	// than multiple versions of one key.
	let (storage, _guard) = MultiPersistentTier::sqlite_in_memory();
	let table = EntryKind::Multi;

	for (label, version) in [(b"A", 1u64), (b"B", 5), (b"C", 10)] {
		let mut entries = HashMap::new();
		entries.insert(table, vec![(EncodedKey::new(label.to_vec()), Some(CowVec::new(label.to_vec())))]);
		storage.set(CommitVersion(version), entries).unwrap();
	}

	let read_at = |scope: MultiVersionScope| -> Vec<CommitVersion> {
		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				table,
				&mut cursor,
				std::ops::Bound::Unbounded,
				std::ops::Bound::Unbounded,
				scope,
				16,
			)
			.unwrap();
		batch.entries.into_iter().map(|e| e.version).collect()
	};

	// AsOf { read: 20 } -> all three keys are visible (versions 1, 5, 10 all <= 20).
	assert_eq!(
		read_at(MultiVersionScope::AsOf {
			read: CommitVersion(20),
		}),
		vec![CommitVersion(1), CommitVersion(5), CommitVersion(10)]
	);

	// AsOf { read: 4 } -> SQL `version <= 4` predicate excludes B (v=5) and C (v=10).
	assert_eq!(
		read_at(MultiVersionScope::AsOf {
			read: CommitVersion(4),
		}),
		vec![CommitVersion(1)]
	);

	// Between { after: 1, read: 20 } -> the Rust-side `scope.contains` excludes A (v=1
	// fails v > 1). B and C remain.
	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(1),
			read: CommitVersion(20),
		}),
		vec![CommitVersion(5), CommitVersion(10)]
	);

	// Between { after: 5, read: 20 } -> excludes A and B (v=1, v=5 both <= 5). Only C.
	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(5),
			read: CommitVersion(20),
		}),
		vec![CommitVersion(10)]
	);

	// Between { after: 0, read: 4 } -> SQL upper bound filters out B and C; the lower
	// bound admits A (1 > 0). Exercises both filtering layers together.
	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(0),
			read: CommitVersion(4),
		}),
		vec![CommitVersion(1)]
	);
}

// Sanity check: AsOf path is byte-for-byte equivalent to the prior
// `version` parameter behavior. Built directly against StandardMultiStore
// to avoid any wrapping.
#[test]
fn asof_matches_prior_behavior() {
	let storage = MultiStore::testing_memory();
	let k = key(b"sentinel");
	write(&storage, &k, b"v3", CommitVersion(3));
	write(&storage, &k, b"v7", CommitVersion(7));

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(5),
	};
	let rows: Vec<_> = storage.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(3));
}

// Ensure StandardMultiStore is reachable via the MultiStore::standard
// constructor path; only used to avoid unused-import lints if other
// constructors are removed in the future.
#[allow(dead_code)]
fn _assert_constructors_compile() {
	let _ = StandardMultiStore::testing_memory();
}
