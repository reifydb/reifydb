// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::{MultiVersionCommit, MultiVersionGet},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_type::util::cowvec::CowVec;

fn row(bytes: &[u8]) -> EncodedRow {
	EncodedRow(CowVec::new(bytes.to_vec()))
}

// Exercises the persistent-tier read-connection pool: many reader threads read from the pool while
// the writer connection commits concurrently (persistent-only store -> every get/commit hits SQLite
// directly). Verifies no deadlock, reads never error, and the final read observes the last commit.
// The "memory" config is a real /dev/shm WAL file, so this validates concurrent multi-connection WAL
// access (the same code path used for on-disk File configs).
#[test]
fn concurrent_reads_during_writes_no_deadlock() {
	let (store, _guard) = StandardMultiStore::testing_persistent_sqlite_only();
	let key = EncodedKey::new(b"k".to_vec());

	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![Delta::Set {
			key: key.clone(),
			row: row(b"v0"),
		}]),
		CommitVersion(1),
	)
	.unwrap();

	let last: u64 = 200;

	let readers: Vec<_> = (0..4)
		.map(|_| {
			let store = store.clone();
			let key = key.clone();
			thread::spawn(move || {
				for _ in 0..500 {
					// Reads must succeed and always observe some committed value (never None,
					// since v0 was committed before any reader started).
					let got = store.get(&key, CommitVersion(u64::MAX)).unwrap();
					assert!(got.is_some());
				}
			})
		})
		.collect();

	for v in 2..=last {
		MultiVersionCommit::commit(
			&store,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				row: row(format!("v{v}").as_bytes()),
			}]),
			CommitVersion(v),
		)
		.unwrap();
	}

	for reader in readers {
		reader.join().expect("reader thread panicked (deadlock or read error)");
	}

	let final_value = store.get(&key, CommitVersion(u64::MAX)).unwrap().unwrap();
	assert_eq!(final_value.row.as_slice(), format!("v{last}").as_bytes());
}
