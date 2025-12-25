// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! StandardEngine transaction benchmarks for ReifyDB.
//!
//! These benchmarks test full transaction lifecycle including:
//! - Begin transaction
//! - Perform operations (set, remove, get)
//! - Commit transaction
//!
//! This provides a higher-level benchmark compared to store_transaction.rs
//! which benchmarks the underlying TransactionStore directly.

use std::{hint::black_box, time::Duration};

use criterion::{
	BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
	measurement::WallTime,
};
use rand::{Rng, seq::SliceRandom};
use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	CowVec, EncodedKey,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{CommandTransaction, Engine, QueryTransaction},
	ioc::IocContainer,
	value::encoded::EncodedValues,
};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};
use tokio::runtime::Runtime;

/// Create a test engine with in-memory storage.
async fn create_engine() -> StandardEngine {
	let store = TransactionStore::testing_memory().await;
	let eventbus = EventBus::new();
	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store, single.clone(), eventbus.clone()).await.unwrap();

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		MaterializedCatalog::new(),
		None,
		IocContainer::new(),
	)
	.await
}

/// Generate test data with deterministic keys and random values.
fn generate_test_data(size: usize, key_size: usize, value_size: usize) -> Vec<(EncodedKey, EncodedValues)> {
	let mut rng = rand::rng();
	let mut data = Vec::with_capacity(size);

	for i in 0..size {
		let key = format!("{:0width$}", i, width = key_size).into_bytes();
		let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();

		data.push((EncodedKey(CowVec::new(key)), EncodedValues(CowVec::new(value))));
	}

	data
}

/// Configure benchmark group with stable measurement settings.
fn configure_group(group: &mut BenchmarkGroup<WallTime>) {
	group.measurement_time(Duration::from_secs(10));
	group.warm_up_time(Duration::from_secs(5));
	group.sample_size(200);
	group.noise_threshold(0.03);
	group.confidence_level(0.99);
}

// ============================================================================
// Insert Benchmarks
// ============================================================================

fn benchmark_insert_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_insert_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let data = generate_test_data(size, 16, 64);
					(engine, data)
				},
				|(engine, data)| {
					// Measurement - this is timed
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in data {
							txn.set(black_box(&key), black_box(value)).await.unwrap();
						}
						txn.commit().await.unwrap();
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_insert_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_insert_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let mut data = generate_test_data(size, 16, 64);
					data.shuffle(&mut rand::rng());
					(engine, data)
				},
				|(engine, data)| {
					// Measurement - this is timed
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in data {
							txn.set(black_box(&key), black_box(value)).await.unwrap();
						}
						txn.commit().await.unwrap();
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

// ============================================================================
// Delete Benchmarks
// ============================================================================

fn benchmark_delete_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_delete_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in &data {
							txn.set(key, value.clone()).await.unwrap();
						}
						txn.commit().await.unwrap();
					});

					(engine, data)
				},
				|(engine, data)| {
					// Only measure deletes
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, _) in data {
							txn.remove(black_box(&key)).await.unwrap();
						}
						txn.commit().await.unwrap();
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_delete_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_delete_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let mut data = generate_test_data(size, 16, 64);

					// Insert data in setup
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in &data {
							txn.set(key, value.clone()).await.unwrap();
						}
						txn.commit().await.unwrap();
					});

					// Shuffle for random deletion order
					data.shuffle(&mut rand::rng());

					(engine, data)
				},
				|(engine, data)| {
					// Only measure deletes
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, _) in data {
							txn.remove(black_box(&key)).await.unwrap();
						}
						txn.commit().await.unwrap();
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

// ============================================================================
// Get Benchmarks
// ============================================================================

fn benchmark_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_get_operations");
	configure_group(&mut group);

	for size in [1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in &data {
							txn.set(key, value.clone()).await.unwrap();
						}
						txn.commit().await.unwrap();
					});

					(engine, data)
				},
				|(engine, data)| {
					// Only measure gets (using query transaction)
					rt.block_on(async {
						let mut txn = engine.begin_query().await.unwrap();
						for (key, _) in data {
							let _result = txn.get(black_box(&key)).await.unwrap();
						}
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

// ============================================================================
// Mixed Operations Benchmark
// ============================================================================

fn benchmark_mixed_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("txn_mixed_operations");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("mixed", size), size, |b, &size| {
			let rt = Runtime::new().unwrap();
			b.iter_batched(
				|| {
					// Setup - not timed
					let engine = rt.block_on(create_engine());
					let data = generate_test_data(size, 16, 64);

					// Pre-insert half the data
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();
						for (key, value) in data.iter().take(size / 2) {
							txn.set(key, value.clone()).await.unwrap();
						}
						txn.commit().await.unwrap();
					});

					(engine, data)
				},
				|(engine, data)| {
					// Mixed: insert new, update existing, delete some
					rt.block_on(async {
						let mut txn = engine.begin_command().await.unwrap();

						// Insert second half (new)
						for (key, value) in data.iter().skip(data.len() / 2) {
							txn.set(black_box(key), black_box(value.clone()))
								.await
								.unwrap();
						}

						// Delete first quarter
						for (key, _) in data.iter().take(data.len() / 4) {
							txn.remove(black_box(key)).await.unwrap();
						}

						txn.commit().await.unwrap();
					});
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

criterion_group!(
	benches,
	benchmark_insert_sequential,
	benchmark_insert_random,
	benchmark_delete_sequential,
	benchmark_delete_random,
	benchmark_get_operations,
	benchmark_mixed_operations,
);

criterion_main!(benches);
