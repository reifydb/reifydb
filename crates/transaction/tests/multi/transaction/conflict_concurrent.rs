// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file ports transaction tests from the badger project (https://github.com/hypermodeinc/badger),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2017 Dgraph Labs, Inc. and Contributors
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Ported from badger's txn_test.go: TestConflict (both the TxnGet and ItrSeek variants),
// TestTxnReadAfterWrite and TestTxnCommitAsync. These exercise the oracle with genuinely
// concurrent committers, which neither the testscript runner nor the deterministic simulator
// can express: both drive one commit to completion before starting the next, so a defect that
// lives inside the commit critical section is invisible to them.
//
// One semantic difference from badger is deliberate. badger detects only read-write conflicts,
// so two blind writes to the same key both commit and the last writer wins. ReifyDB also treats
// an overlapping write set as a conflict (ConflictManager::has_conflict, multi/conflict.rs), so
// a losing writer here aborts with TXN_001 rather than silently overwriting. Ports that relied
// on badger's "writes never conflict" assumption therefore retry instead of asserting success.

use std::sync::{
	Arc, Barrier,
	atomic::{AtomicBool, AtomicU64, Ordering},
};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_transaction::multi::{RangeScope, transaction::write::MultiWriteTransaction};

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

fn read_u64(txn: &mut MultiWriteTransaction, key: &EncodedKey) -> Option<u64> {
	txn.get(key).unwrap().map(|sv| {
		let row = sv.row();
		from_row!(u64, row)
	})
}

#[test]
fn test_conflict_point_get_admits_exactly_one_writer() {
	const LOOPS: usize = 10;
	const THREADS: usize = 16;

	let key: EncodedKey = as_key!("foo".to_string());

	for loop_index in 0..LOOPS {
		let engine = Arc::new(test_multi());
		let committed = Arc::new(AtomicU64::new(0));
		let barrier = Arc::new(Barrier::new(THREADS));

		let handles: Vec<_> = (0..THREADS)
			.map(|_| {
				let engine = Arc::clone(&engine);
				let committed = Arc::clone(&committed);
				let barrier = Arc::clone(&barrier);
				let key = key.clone();

				std::thread::spawn(move || {
					let mut txn = engine.begin_command().unwrap();
					let absent = read_u64(&mut txn, &key).is_none();

					barrier.wait();

					if absent {
						txn.set(&key, as_values!(1u64)).unwrap();
						if txn.commit(vec![]).is_ok() {
							committed.fetch_add(1, Ordering::SeqCst);
						}
					}
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		assert_eq!(
			committed.load(Ordering::SeqCst),
			1,
			"loop {loop_index}: all {THREADS} transactions observed the key as absent and raced to \
			 create it, so exactly one may commit; the rest read a key that the winner went on to write \
			 and must be aborted as conflicts"
		);
	}
}

#[test]
fn test_conflict_range_scan_admits_exactly_one_writer() {
	const LOOPS: usize = 10;
	const THREADS: usize = 16;

	let key: EncodedKey = as_key!("foo".to_string());

	for loop_index in 0..LOOPS {
		let engine = Arc::new(test_multi());
		let committed = Arc::new(AtomicU64::new(0));
		let barrier = Arc::new(Barrier::new(THREADS));

		let handles: Vec<_> = (0..THREADS)
			.map(|_| {
				let engine = Arc::clone(&engine);
				let committed = Arc::clone(&committed);
				let barrier = Arc::clone(&barrier);
				let key = key.clone();

				std::thread::spawn(move || {
					let mut txn = engine.begin_command().unwrap();
					let found = txn
						.range(EncodedKeyRange::all(), RangeScope::All, 1024)
						.next()
						.is_some();

					barrier.wait();

					if !found {
						txn.set(&key, as_values!(1u64)).unwrap();
						if txn.commit(vec![]).is_ok() {
							committed.fetch_add(1, Ordering::SeqCst);
						}
					}
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		assert_eq!(
			committed.load(Ordering::SeqCst),
			1,
			"loop {loop_index}: every transaction established the emptiness of the whole keyspace by \
			 range scan, so the winner's write falls inside a range the others read; range-conflict \
			 detection must abort them all but one"
		);
	}
}

#[test]
fn test_read_after_write_is_visible_to_a_later_transaction() {
	const THREADS: u64 = 100;

	let engine = Arc::new(test_multi());
	let handles: Vec<_> = (0..THREADS)
		.map(|i| {
			let engine = Arc::clone(&engine);
			std::thread::spawn(move || {
				let key: EncodedKey = as_key!(i);

				let mut writer = engine.begin_command().unwrap();
				writer.set(&key, as_values!(i)).unwrap();
				writer.commit(vec![]).unwrap();

				let mut reader = engine.begin_command().unwrap();
				assert_eq!(
					read_u64(&mut reader, &key),
					Some(i),
					"a transaction begun after a commit returned must observe that commit; \
					 seeing None means the read snapshot was taken at a version the commit \
					 had not yet been published to"
				);
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}
}

#[test]
fn test_concurrent_writers_never_expose_a_partial_commit() {
	const ACCOUNTS: u64 = 40;
	const OPENING: u64 = 100;
	const TOTAL: u64 = ACCOUNTS * OPENING;
	const WRITERS: usize = 32;

	let engine = Arc::new(test_multi());

	let mut seed = engine.begin_command().unwrap();
	for i in 0..ACCOUNTS {
		seed.set(&as_key!(i), as_values!(OPENING)).unwrap();
	}
	seed.commit(vec![]).unwrap();

	let stop = Arc::new(AtomicBool::new(false));
	let observations = Arc::new(AtomicU64::new(0));

	let auditor = {
		let engine = Arc::clone(&engine);
		let stop = Arc::clone(&stop);
		let observations = Arc::clone(&observations);
		std::thread::spawn(move || {
			while !stop.load(Ordering::Relaxed) {
				let mut txn = engine.begin_command().unwrap();
				let mut total = 0u64;
				for i in 0..ACCOUNTS {
					total += read_u64(&mut txn, &as_key!(i))
						.expect("every account is seeded before the auditor starts");
				}
				drop(txn);

				assert_eq!(
					total, TOTAL,
					"a single transaction read all {ACCOUNTS} accounts and saw {total} instead \
					 of {TOTAL}. Every writer moves the same amount out of the low accounts as \
					 it moves into the high ones, so the sum is invariant; observing any other \
					 total means the snapshot exposed part of a transfer without the rest"
				);
				observations.fetch_add(1, Ordering::Relaxed);
			}
		})
	};

	let writers: Vec<_> = (0..WRITERS)
		.map(|w| {
			let engine = Arc::clone(&engine);
			std::thread::spawn(move || {
				let delta = (w as u64 % OPENING) / 2;
				loop {
					let mut txn = engine.begin_command().unwrap();
					for i in 0..ACCOUNTS / 2 {
						txn.set(&as_key!(i), as_values!(OPENING - delta)).unwrap();
					}
					for i in ACCOUNTS / 2..ACCOUNTS {
						txn.set(&as_key!(i), as_values!(OPENING + delta)).unwrap();
					}
					if txn.commit(vec![]).is_ok() {
						return;
					}
				}
			})
		})
		.collect();

	for writer in writers {
		writer.join().unwrap();
	}
	stop.store(true, Ordering::Relaxed);
	auditor.join().unwrap();

	assert!(
		observations.load(Ordering::Relaxed) > 0,
		"the auditor must have completed at least one full read of the accounts, otherwise the invariant \
		 was never actually checked"
	);
}
