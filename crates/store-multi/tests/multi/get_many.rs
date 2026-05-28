// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{catalog::flow::FlowNodeId, store::MultiVersionCommit},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_type::util::cowvec::CowVec;

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

	store.commit(
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

#[test]
fn get_many_across_tables_memory_only() {
	check_get_many_across_tables(&StandardMultiStore::testing_memory(), false);
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
