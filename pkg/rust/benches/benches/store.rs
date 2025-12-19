// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! TransactionStore benchmarks for ReifyDB.

use std::{hint::black_box, time::Duration};

use criterion::{
	BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
	measurement::WallTime,
};
use rand::{Rng, seq::SliceRandom};
use reifydb_core::{CommitVersion, CowVec, EncodedKey, delta::Delta, value::encoded::EncodedValues};
use reifydb_store_transaction::{
	BackendConfig, MultiVersionCommit, MultiVersionGet, StandardTransactionStore, TransactionStoreConfig,
	backend::BackendStorage,
};

/// Create a new TransactionStore with the given backend.
fn create_store(backend: BackendStorage) -> StandardTransactionStore {
	StandardTransactionStore::new(TransactionStoreConfig {
		hot: Some(BackendConfig {
			storage: backend,
			retention_period: Duration::from_secs(3600),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		stats: Default::default(),
	})
	.expect("failed to create store")
}

/// Generate test data with deterministic keys and random values.
fn generate_test_data(size: usize, key_size: usize, value_size: usize) -> Vec<(EncodedKey, EncodedValues)> {
	let mut rng = rand::rng();
	let mut data = Vec::with_capacity(size);

	for i in 0..size {
		// Generate deterministic keys to ensure consistent benchmarking
		let key = format!("{:0width$}", i, width = key_size).into_bytes();
		let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();

		data.push((EncodedKey(CowVec::new(key)), EncodedValues(CowVec::new(value))));
	}

	data
}

/// Convert test data to deltas for insertion.
fn to_set_deltas(data: &[(EncodedKey, EncodedValues)]) -> CowVec<Delta> {
	CowVec::new(
		data.iter()
			.map(|(key, values)| Delta::Set {
				key: key.clone(),
				values: values.clone(),
			})
			.collect(),
	)
}

/// Convert test data to deltas for deletion.
fn to_remove_deltas(data: &[(EncodedKey, EncodedValues)]) -> CowVec<Delta> {
	CowVec::new(
		data.iter()
			.map(|(key, _)| Delta::Remove {
				key: key.clone(),
			})
			.collect(),
	)
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
// Memory Backend Benchmarks
// ============================================================================

fn benchmark_memory_insert_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("memory_insert_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::memory());
					let data = generate_test_data(size, 16, 64);
					(store, data)
				},
				|(store, data)| {
					// Measurement - this is timed
					let deltas = to_set_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(1)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_memory_insert_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("memory_insert_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::memory());
					let mut data = generate_test_data(size, 16, 64);
					// Shuffle in setup
					data.shuffle(&mut rand::rng());
					(store, data)
				},
				|(store, data)| {
					// Measurement - this is timed
					let deltas = to_set_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(1)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_memory_delete_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("memory_delete_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::memory());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					(store, data)
				},
				|(store, data)| {
					// Only measure deletes
					let deltas = to_remove_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(2)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_memory_delete_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("memory_delete_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::memory());
					let mut data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					// Shuffle for random deletion order
					data.shuffle(&mut rand::rng());

					(store, data)
				},
				|(store, data)| {
					// Only measure deletes
					let deltas = to_remove_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(2)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_memory_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("memory_get_operations");
	configure_group(&mut group);

	for size in [1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::memory());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					(store, data)
				},
				|(store, data)| {
					// Only measure gets
					for (key, _) in data {
						let _result = store.get(black_box(&key), CommitVersion(1)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

// ============================================================================
// SQLite Backend Benchmarks
// ============================================================================

fn benchmark_sqlite_insert_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("sqlite_insert_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::sqlite_in_memory());
					let data = generate_test_data(size, 16, 64);
					(store, data)
				},
				|(store, data)| {
					// Measurement - this is timed
					let deltas = to_set_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(1)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_sqlite_insert_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("sqlite_insert_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::sqlite_in_memory());
					let mut data = generate_test_data(size, 16, 64);
					// Shuffle in setup
					data.shuffle(&mut rand::rng());
					(store, data)
				},
				|(store, data)| {
					// Measurement - this is timed
					let deltas = to_set_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(1)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_sqlite_delete_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("sqlite_delete_sequential");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::sqlite_in_memory());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					(store, data)
				},
				|(store, data)| {
					// Only measure deletes
					let deltas = to_remove_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(2)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_sqlite_delete_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("sqlite_delete_random");
	configure_group(&mut group);

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::sqlite_in_memory());
					let mut data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					// Shuffle for random deletion order
					data.shuffle(&mut rand::rng());

					(store, data)
				},
				|(store, data)| {
					// Only measure deletes
					let deltas = to_remove_deltas(&data);
					store.commit(black_box(deltas), CommitVersion(2)).unwrap();
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_sqlite_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("sqlite_get_operations");
	configure_group(&mut group);

	for size in [1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let store = create_store(BackendStorage::sqlite_in_memory());
					let data = generate_test_data(size, 16, 64);

					// Insert data in setup
					let deltas = to_set_deltas(&data);
					store.commit(deltas, CommitVersion(1)).unwrap();

					(store, data)
				},
				|(store, data)| {
					// Only measure gets
					for (key, _) in data {
						let _result = store.get(black_box(&key), CommitVersion(1)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

criterion_group!(
	memory_benches,
	benchmark_memory_insert_sequential,
	benchmark_memory_insert_random,
	benchmark_memory_delete_sequential,
	benchmark_memory_delete_random,
	benchmark_memory_get_operations,
);

criterion_group!(
	sqlite_benches,
	benchmark_sqlite_insert_sequential,
	benchmark_sqlite_insert_random,
	benchmark_sqlite_delete_sequential,
	benchmark_sqlite_delete_random,
	benchmark_sqlite_get_operations,
);

criterion_main!(memory_benches, sqlite_benches);
