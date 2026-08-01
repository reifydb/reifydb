// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, thread, time::Instant};

use reifydb_benches::{BenchReport, latency_histogram, merge};
use reifydb_codec::{encoded::row::EncodedRow, key as keycode, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{clock::Clock, rng::Rng},
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};
use reifydb_value::{util::cowvec::CowVec, value::Value};

const TXNS_PER_THREAD: u64 = 20_000;

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

fn encoded_key(thread_id: u64, index: u64) -> EncodedKey {
	EncodedKey::new(keycode::serialize(&((thread_id << 32) | index)))
}

fn encoded_row(value: u64) -> EncodedRow {
	EncodedRow(CowVec::new(keycode::serialize(&value)))
}

fn write_txns(report: &mut BenchReport, threads: usize) {
	let (_actor_system, multi) = build_stack();
	let total = TXNS_PER_THREAD * threads as u64;

	let start = Instant::now();
	let mut handles = Vec::with_capacity(threads);
	for thread_id in 0..threads as u64 {
		let multi = multi.clone();
		handles.push(thread::spawn(move || {
			let mut begin_histogram = latency_histogram();
			let mut commit_histogram = latency_histogram();
			for index in 0..TXNS_PER_THREAD {
				let begin_start = Instant::now();
				let mut txn = multi.begin_command().expect("begin_command must succeed");
				begin_histogram
					.record(begin_start.elapsed().as_nanos() as u64)
					.expect("latency within bounds");
				txn.set(&encoded_key(thread_id, index), encoded_row(index)).expect("set must succeed");
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
	let elapsed = start.elapsed();

	let begin_histogram = merge(begin_histograms);
	let commit_histogram = merge(commit_histograms);
	report.record(&format!("txn_begin threads={threads}"), total, elapsed, &begin_histogram);
	report.record(&format!("txn_commit threads={threads}"), total, elapsed, &commit_histogram);
}

fn main() {
	let mut report = BenchReport::new("txn");
	for threads in [1, 4, 8, 16] {
		write_txns(&mut report, threads);
	}
	report.save();
}
