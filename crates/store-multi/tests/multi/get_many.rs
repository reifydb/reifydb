// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{catalog::flow::FlowNodeId, store::MultiVersionCommit},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

fn fns(node: u64, payload: &[u8]) -> EncodedKey {
	FlowNodeStateKey::new(FlowNodeId(node), payload.to_vec()).encode()
}

fn row(bytes: &[u8]) -> EncodedRow {
	EncodedRow(CowVec::new(bytes.to_vec()))
}

// Exercises StandardMultiStore::get_many across three distinct tables - two operator-state
// tables (EntryKind::Operator) and the multi table (EntryKind::Multi) - which is the fan-out the
// testscript snapshots cannot reach, since their raw keys all classify to EntryKind::Multi.
//
// Verifies that each key resolves from its own table, that the SAME payload under different nodes
// does not bleed across tables (k1 and k2 share the payload "shared" but live in different
// operator tables and must return different values), and that absent keys are omitted.
//
// `flush` moves the buffer contents into persistent before reading, so the same assertions run
// against whichever tier ends up serving the read.
fn check_get_many_across_tables(store: &StandardMultiStore, flush: bool) {
	let k1 = fns(1, b"shared");
	let k2 = fns(2, b"shared");
	let p = EncodedKey::new(b"plain".to_vec());
	let absent_op = fns(1, b"ghost");
	let absent_multi = EncodedKey::new(b"nope".to_vec());

	MultiVersionCommit::commit(
		store,
		CowVec::new(vec![
			Delta::Set {
				key: k1.clone(),
				row: row(b"n1"),
			},
			Delta::Set {
				key: k2.clone(),
				row: row(b"n2"),
			},
			Delta::Set {
				key: p.clone(),
				row: row(b"pp"),
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
	assert_eq!(found.get(&k1).map(|r| r.row.to_vec()), Some(b"n1".to_vec()));
	assert_eq!(found.get(&k2).map(|r| r.row.to_vec()), Some(b"n2".to_vec()));
	assert_eq!(found.get(&p).map(|r| r.row.to_vec()), Some(b"pp".to_vec()));
	assert!(!found.contains_key(&absent_op));
	assert!(!found.contains_key(&absent_multi));
}

// Persistent get_many rounds chunk.len() up to a fixed placeholder bucket {1,8,64,512,900} and pads
// the extra IN-list slots with a repeat of the first key, so distinct prepared SQL stays O(tables).
// This guards that padding never drops a present key, never invents a phantom result for an absent
// key, and returns the right value per key across counts that sit just below, exactly on, and just
// above the 1/8/64 bucket edges - the exact place the round-up + pad-with-duplicate could go wrong.
fn check_get_many_bucket_boundaries(store: &StandardMultiStore) {
	let mut deltas = Vec::new();
	let mut present: Vec<EncodedKey> = Vec::new();
	for i in 0u64..130 {
		let key = fns(7, format!("k{:04}", i).as_bytes());
		deltas.push(Delta::Set {
			key: key.clone(),
			row: row(format!("v{}", i).as_bytes()),
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
				found.get(key).map(|r| r.row.to_vec()),
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
	let (store, _guard) = StandardMultiStore::testing_persistent_sqlite_only();
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
