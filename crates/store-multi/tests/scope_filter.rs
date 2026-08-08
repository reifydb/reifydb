// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
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

fn encoded_bytes(label: &[u8]) -> EncodedBytes {
	let mut bytes = Vec::with_capacity(1 + label.len());
	bytes.push(b'v');
	bytes.extend_from_slice(label);
	EncodedBytes(CowVec::new(bytes))
}

fn write(store: &MultiStore, k: &EncodedKey, payload: &[u8], version: CommitVersion) {
	let bytes = encoded_bytes(payload);
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			bytes
		}],
		version,
	)
	.unwrap();
}

#[test]
fn single_key_scope_filter() {
	// Between's lower bound is exclusive: after=10 admits nothing even though v=10 exists.
	let store = MultiStore::testing_memory();
	let k = key(b"K");

	write(&store, &k, b"v1", CommitVersion(1));
	write(&store, &k, b"v5", CommitVersion(5));
	write(&store, &k, b"v10", CommitVersion(10));

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

	let scope = MultiVersionScope::Between {
		after: CommitVersion(5),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

	let scope = MultiVersionScope::Between {
		after: CommitVersion(10),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert!(rows.is_empty(), "expected no rows above watermark = 10, got {rows:?}");

	let scope = MultiVersionScope::Between {
		after: CommitVersion(4),
		read: CommitVersion(20),
	};
	let rows: Vec<_> = store.range(EncodedKeyRange::all(), scope, 16).collect::<Result<_, _>>().unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].version, CommitVersion(10));

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
	// Only v=11 qualifies per key, so a batch of exactly N must still return N rows - proof the skipped
	// v=1 entries never consume batch budget.
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
	// The persistent tier keeps one version per key, so the fixture uses distinct keys at distinct
	// versions; both the SQL upper bound and the Rust-side exclusive lower bound must apply.
	let (storage, _guard) = MultiPersistentTier::sqlite_in_memory();
	let table = EntryKind::Multi;

	for (label, version) in [(b"A", 1u64), (b"B", 5), (b"C", 10)] {
		let mut entries = HashMap::new();
		entries.insert(table, vec![(EncodedKey::new(label), Some(CowVec::new(label.to_vec())))]);
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

	assert_eq!(
		read_at(MultiVersionScope::AsOf {
			read: CommitVersion(20),
		}),
		vec![CommitVersion(1), CommitVersion(5), CommitVersion(10)]
	);

	// The SQL upper bound alone excludes B and C.
	assert_eq!(
		read_at(MultiVersionScope::AsOf {
			read: CommitVersion(4),
		}),
		vec![CommitVersion(1)]
	);

	// The Rust-side exclusive lower bound alone excludes A.
	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(1),
			read: CommitVersion(20),
		}),
		vec![CommitVersion(5), CommitVersion(10)]
	);

	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(5),
			read: CommitVersion(20),
		}),
		vec![CommitVersion(10)]
	);

	// Both filtering layers together: the upper bound drops B and C, the lower bound still admits A.
	assert_eq!(
		read_at(MultiVersionScope::Between {
			after: CommitVersion(0),
			read: CommitVersion(4),
		}),
		vec![CommitVersion(1)]
	);
}

#[test]
fn asof_matches_prior_behavior() {
	// Built directly against the store so no wrapper can influence the AsOf resolution.
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

#[allow(dead_code)]
fn _assert_constructors_compile() {
	// Keeps the constructor path referenced so removing other constructors cannot silently orphan it.
	let _ = StandardMultiStore::testing_memory();
}
