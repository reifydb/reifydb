// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	sync::Arc,
	thread,
	time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{
	BenchReport, env_flag, env_list_usize, env_opt, env_select, env_u64, env_usize, latency_histogram,
	median_by_throughput, merge,
};
use reifydb_codec::{encoded::bytes::EncodedBytes, key as keycode, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::TableId,
			storage::StorageId,
		},
		store::{EntryKind, classify_key},
	},
	key::{EncodableKey, row::RowKey},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{clock::Clock, rng::Rng},
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

set_global_allocator!();

const DEFAULT_TXNS_PER_THREAD: u64 = 50_000;
const DEFAULT_REPEATS: usize = 5;

const KEY_STRIDE: u64 = 1_000_000;

#[derive(Clone, Copy, PartialEq)]
enum TableLayout {
	SharedTable,
	TablePerThread,
}

impl TableLayout {
	fn label(self) -> &'static str {
		match self {
			TableLayout::SharedTable => "shared_table",
			TableLayout::TablePerThread => "table_per_thread",
		}
	}

	fn storage_for(self, thread_id: u64) -> StorageId {
		match self {
			TableLayout::SharedTable => StorageId::table(TableId(1)),
			TableLayout::TablePerThread => StorageId::table(TableId(thread_id + 1)),
		}
	}
}

struct DefaultConfig;

impl GetConfig for DefaultConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		key.default_value()
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		key.default_value()
	}
}

fn build_stack() -> (ActorSystem, MultiTransaction) {
	let actor_system = ActorSystem::testing(Clock::Real);
	let spawner = actor_system.spawner();
	let event_bus = EventBus::new(&spawner);
	let multi = MultiTransaction::new(
		MultiStore::testing_memory(),
		SingleTransaction::new(SingleStore::testing_memory(), event_bus.clone()),
		event_bus,
		spawner,
		Clock::Real,
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(DefaultConfig),
	)
	.expect("benchmark transaction stack must build");
	(actor_system, multi)
}

fn encoded_key(layout: TableLayout, thread_id: u64, index: u64) -> EncodedKey {
	RowKey {
		storage: layout.storage_for(thread_id),
		row: RowNumber(thread_id * KEY_STRIDE + index + 1),
	}
	.encode()
}

fn encoded_bytes(value: u64) -> EncodedBytes {
	EncodedBytes(CowVec::new(keycode::serialize(&value)))
}

struct Sample {
	ops: u64,
	elapsed: Duration,
	begin: Histogram<u64>,
	commit: Histogram<u64>,
}

fn run_once(threads: usize, layout: TableLayout, iterations: u64) -> Sample {
	let (_actor_system, multi) = build_stack();
	let total = iterations * threads as u64;

	let start = Instant::now();
	let mut handles = Vec::with_capacity(threads);
	for thread_id in 0..threads as u64 {
		let multi = multi.clone();
		handles.push(thread::spawn(move || {
			let mut begin_histogram = latency_histogram();
			let mut commit_histogram = latency_histogram();
			for index in 0..iterations {
				let begin_start = Instant::now();
				let mut txn = multi.begin_command().expect("begin_command must succeed");
				begin_histogram
					.record(begin_start.elapsed().as_nanos() as u64)
					.expect("latency within bounds");
				txn.set(&encoded_key(layout, thread_id, index), encoded_bytes(index))
					.expect("set must succeed");
				let commit_start = Instant::now();
				txn.commit(vec![]).expect("disjoint keys must not conflict");
				commit_histogram
					.record(commit_start.elapsed().as_nanos() as u64)
					.expect("latency within bounds");
			}
			(begin_histogram, commit_histogram)
		}));
	}
	let (begin_histograms, commit_histograms): (Vec<_>, Vec<_>) =
		handles.into_iter().map(|handle| handle.join().expect("bench thread panicked")).unzip();

	Sample {
		ops: total,
		elapsed: start.elapsed(),
		begin: merge(begin_histograms),
		commit: merge(commit_histograms),
	}
}

fn write_txns(report: &mut BenchReport, threads: usize, layout: TableLayout, iterations: u64, repeats: usize) {
	let samples: Vec<Sample> = (0..repeats).map(|_| run_once(threads, layout, iterations)).collect();
	let median = median_by_throughput(&samples, |s| (s.ops, s.elapsed));

	let label = layout.label();
	report.record(&format!("txn_begin/{label} threads={threads}"), median.ops, median.elapsed, &median.begin);
	report.record(&format!("txn_commit/{label} threads={threads}"), median.ops, median.elapsed, &median.commit);
	println!("repro= make bench-txn LAYOUTS={label} THREADS={threads}");
}

fn verify_key_classification() {
	let shared = classify_key(&encoded_key(TableLayout::SharedTable, 0, 0));
	let other = classify_key(&encoded_key(TableLayout::SharedTable, 7, 0));
	assert_eq!(shared, other, "shared_table must place every thread in one storage entry");
	assert_ne!(
		shared,
		EntryKind::Multi,
		"benchmark keys must decode as Key::Row, otherwise every thread contends on EntryKind::Multi"
	);

	let a = classify_key(&encoded_key(TableLayout::TablePerThread, 0, 0));
	let b = classify_key(&encoded_key(TableLayout::TablePerThread, 1, 0));
	assert_ne!(a, b, "table_per_thread must place each thread in its own storage entry");

	for layout in [TableLayout::SharedTable, TableLayout::TablePerThread] {
		let mut seen = HashSet::new();
		for thread_id in 0..32u64 {
			for index in [0u64, 1, KEY_STRIDE - 1] {
				let key = encoded_key(layout, thread_id, index);
				assert!(
					seen.insert(key),
					"{} produced a duplicate key for thread {thread_id} index {index}; \
					 overlapping write sets would measure conflict handling, not throughput",
					layout.label()
				);
			}
		}
	}
}

fn main() {
	verify_key_classification();

	let matrix = env_flag("MATRIX");
	let explicit_threads = env_opt("THREADS").is_some();
	let iterations = env_u64("ITERATIONS", DEFAULT_TXNS_PER_THREAD);
	let repeats = env_usize("REPEATS", DEFAULT_REPEATS);

	assert!(
		iterations <= KEY_STRIDE,
		"ITERATIONS={iterations} exceeds the {KEY_STRIDE} row-number stride, so threads would write \
		 overlapping keys and the benchmark would measure conflict handling rather than throughput"
	);
	assert!(repeats > 0, "REPEATS must be at least one");

	let default_layouts: &[TableLayout] = if matrix || explicit_threads {
		&[TableLayout::SharedTable]
	} else {
		&[TableLayout::SharedTable, TableLayout::TablePerThread]
	};
	let layouts = env_select(
		"LAYOUTS",
		&[("shared_table", TableLayout::SharedTable), ("table_per_thread", TableLayout::TablePerThread)],
		default_layouts,
	);
	assert!(!layouts.is_empty(), "LAYOUTS matched no known table layout");

	let default_threads: &[usize] = if matrix {
		&[16, 24, 32]
	} else {
		&[1, 2, 4, 8, 12, 16, 24, 32]
	};
	let swept = env_list_usize("THREADS", default_threads);
	assert!(!swept.is_empty(), "THREADS matched no thread count");

	let mut report = BenchReport::new("txn");
	println!(
		"matrix cells={} layouts={} threads={} iterations={} repeats={}",
		layouts.len() * swept.len(),
		layouts.len(),
		swept.len(),
		iterations,
		repeats
	);

	for layout in &layouts {
		for threads in &swept {
			write_txns(&mut report, *threads, *layout, iterations, repeats);
		}
	}
	report.save();
}
