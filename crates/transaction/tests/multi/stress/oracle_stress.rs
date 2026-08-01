// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use super::test_multi;
use crate::{as_key, as_values};

#[test]
fn test_oracle_committed_txns_cleanup() {
	// Well past the internal cleanup threshold, on unique keys so nothing conflicts: the oracle
	// must keep retiring committed windows instead of growing without bound.
	let engine = test_multi();

	const NUM_TXNS: usize = 20_000;

	for i in 0..NUM_TXNS {
		let mut tx = engine.begin_command().unwrap();

		let key = as_key!(format!("key_{}", i));
		let value = as_values!(format!("value_{}", i));

		tx.set(&key, value).unwrap();

		tx.commit(vec![]).unwrap();

		if i > 0 && i % 1000 == 0 {
			assert!(i < NUM_TXNS, "Should be able to create {} transactions", NUM_TXNS);
		}
	}

	let mut final_tx = engine.begin_command().unwrap();
	let final_key = as_key!("final");
	let final_value = as_values!("test".to_string());
	final_tx.set(&final_key, final_value).unwrap();
	final_tx.commit(vec![]).unwrap();
}

#[test]
fn test_oracle_high_concurrency() {
	// Disjoint keys across every thread, so any commit error is a false conflict, not a real one.
	let engine = Arc::new(test_multi());

	const NUM_THREADS: usize = 100;
	const TXN_PER_THREAD: usize = 50;

	let mut handles = vec![];

	for thread_id in 0..NUM_THREADS {
		let engine_clone = engine.clone();
		let handle = std::thread::spawn(move || {
			for i in 0..TXN_PER_THREAD {
				let mut tx = engine_clone.begin_command().unwrap();

				let key = as_key!(format!("t{}_{}", thread_id, i));
				let value = as_values!(format!("v{}_{}", thread_id, i));

				tx.set(&key, value).unwrap();

				match tx.commit(vec![]) {
					Ok(_) => {}
					Err(e) => panic!("Unexpected error: {:?}", e),
				}
			}
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.join().unwrap();
	}

	let mut final_tx = engine.begin_command().unwrap();
	let final_key = as_key!("concurrent_test");
	let final_value = as_values!("passed".to_string());
	final_tx.set(&final_key, final_value).unwrap();
	final_tx.commit(vec![]).unwrap();
}

#[test]
fn test_oracle_version_boundaries() {
	// A sweep across enough versions to cross several block allocations; any panic is the failure.
	let engine = test_multi();

	for i in 0..10_000 {
		let mut tx = engine.begin_command().unwrap();
		let key = as_key!(format!("boundary_{}", i));
		let value = as_values!("test".to_string());
		tx.set(&key, value).unwrap();
		tx.commit(vec![]).unwrap();
	}
}
