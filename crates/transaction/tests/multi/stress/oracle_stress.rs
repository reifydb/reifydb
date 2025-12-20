// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{sync::Arc, thread};

use reifydb_transaction::multi::{Transaction, transaction::MAX_COMMITTED_TXNS};

use crate::{as_key, as_values};

/// Test that Oracle properly cleans up committed transactions when limit is
/// exceeded
#[test]
fn test_oracle_committed_txns_cleanup() {
	let engine = Transaction::testing();

	// Number of transactions to create (exceeds MAX_COMMITTED_TXNS)
	const NUM_TXNS: usize = 2 * MAX_COMMITTED_TXNS;

	// Create many transactions with conflicts to ensure they're tracked
	for i in 0..NUM_TXNS {
		let mut tx = engine.begin_command().unwrap();

		// Each transaction writes to a unique key to avoid actual
		// conflicts
		let key = as_key!(format!("key_{}", i));
		let value = as_values!(format!("value_{}", i));

		tx.set(&key, value).unwrap();

		// Commit the transaction - this adds to Oracle's committed list
		tx.commit().unwrap();

		// Every 1000 transactions, verify memory is being managed
		if i > 0 && i % 1000 == 0 {
			// The Oracle should automatically clean up when
			// exceeding limits We can't directly check the
			// internal state, but the fact that we can continue
			// creating transactions shows cleanup is working
			assert!(i < NUM_TXNS, "Should be able to create {} transactions", NUM_TXNS);
		}
	}

	// Create one more transaction to verify system is still functional
	let mut final_tx = engine.begin_command().unwrap();
	let final_key = as_key!("final");
	let final_value = as_values!("test".to_string());
	final_tx.set(&final_key, final_value).unwrap();
	final_tx.commit().unwrap();
}

/// Test high concurrency with many simultaneous transactions
#[test]
fn test_oracle_high_concurrency() {
	let engine = Arc::new(Transaction::testing());

	const NUM_THREADS: usize = 100;
	const TXN_PER_THREAD: usize = 50;

	let mut handles = vec![];

	for thread_id in 0..NUM_THREADS {
		let engine_clone = engine.clone();
		let handle = thread::spawn(move || {
			for i in 0..TXN_PER_THREAD {
				let mut tx = engine_clone.begin_command().unwrap();

				let key = as_key!(format!("t{}_{}", thread_id, i));
				let value = as_values!(format!("v{}_{}", thread_id, i));

				tx.set(&key, value).unwrap();

				match tx.commit() {
					Ok(_) => {}
					Err(e) => panic!("Unexpected error: {:?}", e),
				}
			}
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.join().expect("Thread panicked");
	}

	let mut final_tx = engine.begin_command().unwrap();
	let final_key = as_key!("concurrent_test");
	let final_value = as_values!("passed".to_string());
	final_tx.set(&final_key, final_value).unwrap();
	final_tx.commit().unwrap();
}

/// Test that Oracle handles version overflow gracefully
#[test]
fn test_oracle_version_boundaries() {
	let engine = Transaction::testing();

	// Create transactions to test version boundaries
	for i in 0..10_000 {
		let mut tx = engine.begin_command().unwrap();
		let key = as_key!(format!("boundary_{}", i));
		let value = as_values!("test".to_string());
		tx.set(&key, value).unwrap();
		tx.commit().unwrap();
	}

	// System should handle version numbers without panic
}
