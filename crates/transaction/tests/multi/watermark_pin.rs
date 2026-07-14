// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::transaction::test_multi;
use crate::{as_key, as_values};

#[test]
fn a_live_transaction_keeps_the_query_watermark_from_passing_its_snapshot() {
	let engine = test_multi();

	let long_running = engine.begin_command().unwrap();
	let base = long_running.version();

	for i in 0..5u64 {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(i), as_values!(i)).unwrap();
		txn.commit(vec![]).unwrap();
	}

	let done = engine.query_done_until();
	assert!(
		done <= base,
		"query_done_until reached {done} while a transaction is still open at snapshot {base}. That \
		 watermark is the cutoff for both conflict-history eviction and MVCC garbage collection, so once \
		 it passes a live snapshot the open transaction can have its conflict history evicted (TXN_004) \
		 and the versions it is still reading physically reclaimed underneath it. An open transaction must \
		 hold the watermark at or below its own snapshot for as long as it lives"
	);

	drop(long_running);
}

#[test]
fn transactions_sharing_a_snapshot_each_hold_the_pin_independently() {
	let engine = test_multi();

	let first = engine.begin_command().unwrap();
	let second = engine.begin_command().unwrap();
	let base = first.version();
	assert_eq!(second.version(), base, "both transactions begin before any commit, so they share a snapshot");

	drop(first);

	for i in 0..5u64 {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(i), as_values!(i)).unwrap();
		txn.commit(vec![]).unwrap();
	}

	let done = engine.query_done_until();
	assert!(
		done <= base,
		"query_done_until reached {done} while a second transaction is still open at snapshot {base}. \
		 Two transactions began at the same snapshot, so the pin on that version is held twice; the first \
		 one finishing may only drop its own reference. Releasing the version outright strands the second \
		 transaction with no protection against eviction or garbage collection"
	);

	drop(second);
}
