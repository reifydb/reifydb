// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc, Barrier,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_transaction::multi::transaction::write::MultiWriteTransaction;

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

const COUNTER: u64 = 1;

fn read_counter(txn: &mut MultiWriteTransaction, key: &EncodedKey) -> u64 {
	let sv = txn.get(key).unwrap().unwrap();
	let row = sv.row();
	from_row!(u64, row)
}

#[test]
fn test_lost_update_rejected_when_commits_are_serialized() {
	let key: EncodedKey = as_key!(COUNTER);
	let engine = test_multi();

	let mut seed = engine.begin_command().unwrap();
	seed.set(&key, as_values!(0u64)).unwrap();
	seed.commit(vec![]).unwrap();

	let mut txn1 = engine.begin_command().unwrap();
	let mut txn2 = engine.begin_command().unwrap();

	let read1 = read_counter(&mut txn1, &key);
	let read2 = read_counter(&mut txn2, &key);
	assert_eq!(read1, 0);
	assert_eq!(read2, 0);

	txn1.set(&key, as_values!(read1 + 1)).unwrap();
	txn2.set(&key, as_values!(read2 + 1)).unwrap();

	txn1.commit(vec![]).unwrap();

	let err = txn2.commit(vec![]).unwrap_err();
	assert!(
		err.to_string().contains("conflict"),
		"txn2 read the counter that txn1 then overwrote, so letting txn2 commit would discard txn1's \
		 increment. The oracle must abort txn2, but it returned: {err}"
	);

	let mut verify = engine.begin_command().unwrap();
	assert_eq!(
		read_counter(&mut verify, &key),
		1,
		"exactly one of the two increments may survive when one committer is aborted"
	);
}

#[test]
fn test_lost_update_rejected_when_commits_race() {
	const THREADS: usize = 16;
	const ROUNDS: usize = 500;

	let key: EncodedKey = as_key!(COUNTER);

	let mut lossy_rounds = 0usize;
	let mut first_loss: Option<(usize, u64, u64)> = None;

	for round in 0..ROUNDS {
		let engine = Arc::new(test_multi());

		let mut seed = engine.begin_command().unwrap();
		seed.set(&key, as_values!(0u64)).unwrap();
		seed.commit(vec![]).unwrap();

		let barrier = Arc::new(Barrier::new(THREADS));
		let committed = Arc::new(AtomicU64::new(0));

		let handles: Vec<_> = (0..THREADS)
			.map(|_| {
				let engine = Arc::clone(&engine);
				let barrier = Arc::clone(&barrier);
				let committed = Arc::clone(&committed);
				let key = key.clone();

				std::thread::spawn(move || {
					let mut txn = engine.begin_command().unwrap();
					let current = read_counter(&mut txn, &key);

					barrier.wait();

					txn.set(&key, as_values!(current + 1)).unwrap();
					if txn.commit(vec![]).is_ok() {
						committed.fetch_add(1, Ordering::SeqCst);
					}
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		let mut verify = engine.begin_command().unwrap();
		let observed = read_counter(&mut verify, &key);
		let succeeded = committed.load(Ordering::SeqCst);

		assert!(
			succeeded >= 1,
			"round {round}: all {THREADS} committers aborted and the counter stayed at {observed}. \
			 Aborting everyone would satisfy the no-lost-update check while making no progress, so the \
			 oracle must let at least one of a set of conflicting transactions through"
		);

		if observed != succeeded {
			lossy_rounds += 1;
			first_loss.get_or_insert((round, succeeded, observed));
		}
	}

	if let Some((round, succeeded, observed)) = first_loss {
		panic!(
			"{lossy_rounds} of {ROUNDS} rounds lost updates. First at round {round}: {succeeded} \
			 transactions each committed a read-modify-write of the same counter, but the counter only \
			 reached {observed}, so {} increment(s) were silently discarded. Concurrent committers \
			 validated against the same snapshot and none of them saw the others, because \
			 Oracle::new_commit releases its read lock after detect_conflicts and reacquires a write lock \
			 in register_committed, leaving a window where two conflicting transactions can both pass \
			 validation. A lost update is forbidden under serializability and under snapshot isolation \
			 alike, so this is a violation at any isolation level the engine claims to offer.",
			succeeded - observed
		);
	}
}
