// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Insertion speed benchmark
//!
//! Measures write performance with different batch sizes and value sizes.
//! Run with: cargo run --example insertion_benchmark --release

use std::time::Instant;

use reifydb_core::{CommitVersion, CowVec, EncodedKey, delta::Delta, value::encoded::EncodedValues};
use reifydb_store_transaction::{
	backend::multi::BackendMultiVersionCommit,
	sqlite::{SqliteBackend, SqliteConfig},
};

fn main() {
	println!("Insertion Speed Benchmark");
	println!("=========================\n");

	// Configuration
	let value_sizes = vec![300, 500, 700, 1000]; // bytes
	let batch_sizes = vec![1, 10, 100, 1000]; // rows per commit
	let total_rows_per_test = 10_000; // Total rows to insert per test
	let warmup_rows = 1_000; // Warmup before measuring

	for value_size in &value_sizes {
		println!("Value Size: {} bytes", value_size);
		println!("{}", "-".repeat(80));

		for batch_size in &batch_sizes {
			run_benchmark(*value_size, *batch_size, total_rows_per_test, warmup_rows);
		}

		println!();
	}
}

fn run_benchmark(value_size: usize, batch_size: usize, total_rows: usize, warmup_rows: usize) {
	// Create a new tmpfs-backed database for each test
	let backend = SqliteBackend::new(SqliteConfig::in_memory());
	// let backend = MemoryBackend::new();

	// Warmup
	let mut version = CommitVersion(0);
	for _ in 0..(warmup_rows / batch_size) {
		version.0 += 1;
		let deltas = generate_batch(batch_size, value_size, version.0 * batch_size as u64);
		backend.commit(deltas, version).unwrap();
	}

	// Actual benchmark
	let num_batches = total_rows / batch_size;
	let start = Instant::now();

	for _ in 0..num_batches {
		version.0 += 1;
		let deltas = generate_batch(batch_size, value_size, version.0 * batch_size as u64);
		backend.commit(deltas, version).unwrap();
	}

	let elapsed = start.elapsed();

	// Calculate metrics
	let total_ms = elapsed.as_secs_f64() * 1000.0;
	let rows_per_sec = (total_rows as f64 / elapsed.as_secs_f64()) as u64;
	let ms_per_row = total_ms / total_rows as f64;
	let ms_per_batch = total_ms / num_batches as f64;

	println!(
		"  Batch Size: {:>5} | Total: {:>7.1}ms | Throughput: {:>10} rows/sec | {:>7.3}ms/row | {:>7.3}ms/batch",
		batch_size,
		total_ms,
		format_number(rows_per_sec),
		ms_per_row,
		ms_per_batch
	);
}

fn generate_batch(size: usize, value_size: usize, start_offset: u64) -> CowVec<Delta> {
	let mut deltas = Vec::with_capacity(size);

	for i in 0..size {
		let key_num = start_offset + i as u64;
		let key = EncodedKey(CowVec::new(format!("key_{:010}", key_num).into_bytes()));
		let value = vec![b'x'; value_size]; // Fill with 'x' characters
		let values = EncodedValues(CowVec::new(value));

		deltas.push(Delta::Set {
			key,
			values,
		});
	}

	CowVec::new(deltas)
}

fn format_number(n: u64) -> String {
	n.to_string()
		.as_bytes()
		.rchunks(3)
		.rev()
		.map(std::str::from_utf8)
		.collect::<Result<Vec<&str>, _>>()
		.unwrap()
		.join(",")
}
