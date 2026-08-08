// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{MultiVersionCommit, MultiVersionGet},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

fn encoded_bytes(bytes: &[u8]) -> EncodedBytes {
	EncodedBytes(CowVec::new(bytes.to_vec()))
}

#[test]
fn concurrent_reads_during_writes_no_deadlock() {
	// The "memory" config is a real /dev/shm WAL file, so reader threads against the pool while the
	// writer connection commits exercise the same multi-connection WAL path an on-disk config uses.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let key = EncodedKey::new(b"k");

	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![Delta::Set {
			key: key.clone(),
			bytes: encoded_bytes(b"v0"),
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
					// v0 was committed before any reader started, so None is never correct here.
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
				bytes: encoded_bytes(format!("v{v}").as_bytes()),
			}]),
			CommitVersion(v),
		)
		.unwrap();
	}

	for reader in readers {
		reader.join().expect("reader thread panicked (deadlock or read error)");
	}

	let final_value = store.get(&key, CommitVersion(u64::MAX)).unwrap().unwrap();
	assert_eq!(final_value.bytes.as_slice(), format!("v{last}").as_bytes());
}
