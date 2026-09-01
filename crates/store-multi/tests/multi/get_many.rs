// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{catalog::flow::OperatorId, store::MultiVersionCommit},
	key::{
		EncodableKey,
		operator::state::{GroupId, KeyspaceId, OperatorStateKey},
	},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

fn fns(node: u64, payload: &[u8]) -> EncodedKey {
	OperatorStateKey::new(OperatorId(node), GroupId::ROOT, KeyspaceId::CUSTOM_NOT_CACHED, payload.to_vec()).encode()
}

fn encoded_bytes(bytes: &[u8]) -> EncodedBytes {
	EncodedBytes(CowVec::new(bytes.to_vec()))
}

fn check_get_many_across_tables(store: &StandardMultiStore, flush: bool) {
	// k1 and k2 carry the same payload under different operator nodes, so a table-scoping bug shows
	// up as one bleeding into the other. Testscript snapshots cannot reach this fan-out: their raw
	// keys all classify to the single Multi table.
	let k1 = fns(1, b"shared");
	let k2 = fns(2, b"shared");
	let p = EncodedKey::new(b"plain");
	let absent_op = fns(1, b"ghost");
	let absent_multi = EncodedKey::new(b"nope");

	MultiVersionCommit::commit(
		store,
		CowVec::new(vec![
			Delta::Set {
				key: k1.clone(),
				bytes: encoded_bytes(b"n1"),
			},
			Delta::Set {
				key: k2.clone(),
				bytes: encoded_bytes(b"n2"),
			},
			Delta::Set {
				key: p.clone(),
				bytes: encoded_bytes(b"pp"),
			},
		]),
		CommitVersion(1),
	)
	.unwrap();

	if flush {
		store.flush_pending_blocking();
	}

	let found = store
		.get_many(
			&[k1.clone(), k2.clone(), p.clone(), absent_op.clone(), absent_multi.clone()],
			CommitVersion(1),
		)
		.unwrap();

	assert_eq!(found.len(), 3);
	assert_eq!(found.get(&k1).map(|r| r.bytes.to_vec()), Some(b"n1".to_vec()));
	assert_eq!(found.get(&k2).map(|r| r.bytes.to_vec()), Some(b"n2".to_vec()));
	assert_eq!(found.get(&p).map(|r| r.bytes.to_vec()), Some(b"pp".to_vec()));
	assert!(!found.contains_key(&absent_op));
	assert!(!found.contains_key(&absent_multi));
}

fn check_get_many_bucket_boundaries(store: &StandardMultiStore) {
	// Persistent get_many rounds a chunk up to a placeholder bucket {1,8,64,512,900} and pads the
	// spare slots with a repeat of the first key, so the counts here sit just below, on, and just
	// above the bucket edges where a pad could drop a key or invent a phantom result.
	let mut deltas = Vec::new();
	let mut present: Vec<EncodedKey> = Vec::new();
	for i in 0u64..130 {
		let key = fns(7, format!("k{:04}", i).as_bytes());
		deltas.push(Delta::Set {
			key: key.clone(),
			bytes: encoded_bytes(format!("v{}", i).as_bytes()),
		});
		present.push(key);
	}
	MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(1)).unwrap();
	store.flush_pending_blocking();

	for count in [1usize, 2, 7, 8, 9, 63, 64, 65, 129, 130] {
		let absent = fns(7, format!("ghost{:04}", count).as_bytes());
		let mut lookup: Vec<EncodedKey> = present[..count].to_vec();
		lookup.push(absent.clone());

		let found = store.get_many(&lookup, CommitVersion(1)).unwrap();

		assert_eq!(found.len(), count, "count={}: expected exactly {} resolved keys", count, count);
		assert!(!found.contains_key(&absent), "count={}: absent key must not resolve via padding", count);
		for (i, key) in present[..count].iter().enumerate() {
			assert_eq!(
				found.get(key).map(|r| r.bytes.to_vec()),
				Some(format!("v{}", i).into_bytes()),
				"count={}: key index {} returned wrong value",
				count,
				i
			);
		}
	}
}

#[test]
fn get_many_across_tables_memory_only() {
	check_get_many_across_tables(&StandardMultiStore::testing_memory(), false);
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
#[test]
fn get_many_bucket_boundaries_sqlite() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	check_get_many_bucket_boundaries(&store);
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
#[test]
fn get_many_across_tables_sqlite_only() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	check_get_many_across_tables(&store, false);
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
#[test]
fn get_many_across_tables_memory_with_sqlite_flush() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	check_get_many_across_tables(&store, true);
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
#[test]
fn get_many_across_tables_memory_with_sqlite_no_flush() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	check_get_many_across_tables(&store, false);
}
