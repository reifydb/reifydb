// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Instant};

use encoding::keycode;
use reifydb_core::{EncodedKey, util::encoding, value::encoded::EncodedValues};
use reifydb_transaction::multi::TransactionMulti;
use tokio::spawn;

macro_rules! as_key {
	($key:expr) => {{ EncodedKey::new(keycode::serialize(&$key)) }};
}

macro_rules! as_values {
	($val:expr) => {{ EncodedValues(reifydb_core::CowVec::new(keycode::serialize(&$val))) }};
}

/// Benchmark showing the performance improvement of the new oracle
/// implementation
pub async fn oracle_performance_benchmark() {
	println!("=== Oracle Performance Benchmark ===\n");

	// Test different transaction counts to show scaling behavior
	let test_sizes = vec![1000, 5000, 10000, 25000];

	for &num_txns in &test_sizes {
		println!("Testing with {} transactions...", num_txns);

		let engine = TransactionMulti::testing().await;

		let start = Instant::now();

		// Create transactions sequentially (worst case for O(N²)
		// algorithm)
		for i in 0..num_txns {
			let mut tx = engine.begin_command().await.unwrap();

			let key = as_key!(format!("key_{}", i));
			let value = as_values!(format!("value_{}", i));

			tx.set(&key, value).unwrap();
			tx.commit().await.unwrap();
		}

		let duration = start.elapsed();
		let tps = num_txns as f64 / duration.as_secs_f64();

		println!("  {} transactions in {:?}", num_txns, duration);
		println!("  {:.0} TPS (transactions per second)", tps);
		println!("  {:.2} μs per transaction\n", duration.as_micros() as f64 / num_txns as f64);
	}
}

/// Benchmark concurrent performance
pub async fn concurrent_oracle_benchmark() {
	println!("=== Concurrent Oracle Performance Benchmark ===\n");

	let test_configs = vec![
		(10, 1000), // 10 threads, 1000 txns each
		(50, 500),  // 50 threads, 500 txns each
		(100, 250), // 100 threads, 250 txns each
		(1000, 50), // 100 threads, 250 txns each
	];

	for &(num_threads, txns_per_thread) in &test_configs {
		let total_txns = num_threads * txns_per_thread;
		println!(
			"Testing {} threads × {} transactions = {} total...",
			num_threads, txns_per_thread, total_txns
		);

		let engine = Arc::new(TransactionMulti::testing().await);
		let start = Instant::now();

		let mut handles = vec![];

		for thread_id in 0..num_threads {
			let engine_clone = engine.clone();
			let handle = spawn(async move {
				let base_key = thread_id * txns_per_thread;
				for i in 0..txns_per_thread {
					let mut tx = engine_clone.begin_command().await.unwrap();

					let key = as_key!(base_key + i);
					let value = as_values!(i);

					tx.set(&key, value).unwrap();
					tx.commit().await.unwrap();
				}
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.await.expect("Task panicked");
		}

		let duration = start.elapsed();
		let tps = total_txns as f64 / duration.as_secs_f64();

		println!("  {} total transactions in {:?}", total_txns, duration);
		println!("  {:.0} TPS (transactions per second)", tps);
		println!("  {:.2} μs per transaction\n", duration.as_micros() as f64 / total_txns as f64);
	}
}

/// Benchmark with actual conflicts to test conflict detection performance
pub async fn conflict_detection_benchmark() {
	println!("=== Conflict Detection Performance Benchmark ===\n");

	let engine = TransactionMulti::testing().await;

	// Pre-populate with some data to create realistic conflict scenarios
	for i in 0..1000 {
		let mut tx = engine.begin_command().await.unwrap();
		let key = as_key!(format!("shared_key_{}", i % 100)); // 100 different keys
		let value = as_values!(i);
		tx.set(&key, value).unwrap();
		tx.commit().await.unwrap();
	}

	println!("Pre-populated with 1000 transactions across 100 keys");

	// Now test conflict detection performance
	let num_conflict_txns = 10000;
	let start = Instant::now();
	let mut conflicts = 0;

	for i in 0..num_conflict_txns {
		let mut tx = engine.begin_command().await.unwrap();

		// Try to modify keys that might conflict
		let key = as_key!(format!("shared_key_{}", i % 100));
		let value = as_values!(i + 1000);

		tx.set(&key, value).unwrap();

		match tx.commit().await {
			Ok(_) => {}
			Err(e) if e.code == "TXN_001" => {
				conflicts += 1;
			}
			Err(e) => panic!("Unexpected error: {:?}", e),
		};
	}

	let duration = start.elapsed();
	let tps = num_conflict_txns as f64 / duration.as_secs_f64();

	println!("  {} transactions with potential conflicts in {:?}", num_conflict_txns, duration);
	println!(
		"  {} actual conflicts detected ({:.1}%)",
		conflicts,
		conflicts as f64 / num_conflict_txns as f64 * 100.0
	);
	println!("  {:.0} TPS (transactions per second)", tps);
	println!("  {:.2} μs per transaction", duration.as_micros() as f64 / num_conflict_txns as f64);
}

#[tokio::main]
async fn main() {
	println!("🚀 ReifyDB Oracle Performance Benchmarks\n");

	oracle_performance_benchmark().await;
	println!("\n{}\n", "=".repeat(60));

	concurrent_oracle_benchmark().await;
	println!("\n{}\n", "=".repeat(60));

	conflict_detection_benchmark().await;

	println!("\n✅ All benchmarks completed!");
}
