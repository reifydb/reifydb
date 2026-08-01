// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! TombstoneReap task: the single physical deleter for delete-mode tombstones across every persistent table.
//!
//! `store-multi` covers the tier primitive, so what is testable here is the task around it: the cutoff comes from
//! the flush watermark, so a tombstone whose superseding write may be unflushed survives, and a backlog larger than
//! one batch drains across slices instead of in one unbounded delete.

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowNodeId,
		},
		store::{MultiVersionCommit, MultiVersionGet},
	},
	key::operator_state::OperatorStateKey,
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_sub_lifecycle::{
	plane::{RetentionPlane, ledger::FloorSource},
	store::tombstone::TombstoneReapTask,
};
use reifydb_value::{util::cowvec::CowVec, value::Value};

const NODE: FlowNodeId = FlowNodeId(1);

/// Only `flush_watermark` is scripted; the tombstone reaper's floor is the flush watermark alone, so this drives
/// its cutoff directly.
#[derive(Clone)]
struct ScriptedFlushWatermark(Arc<AtomicU64>);

impl FloorSource for ScriptedFlushWatermark {
	fn query_done_until(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn lease_min(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn consumer_checkpoint(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn consumer_position(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn flush_watermark(&self) -> CommitVersion {
		CommitVersion(self.0.load(Ordering::SeqCst))
	}

	fn owning_flow_checkpoint(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}
}

struct StubConfig {
	batch_size: u64,
}

impl GetConfig for StubConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		match key {
			ConfigKey::TombstoneReapBatchSize => Value::Uint8(self.batch_size),
			ConfigKey::TombstoneReapInterval => Value::duration_seconds(60),
			other => panic!("the tombstone reaper must not read config key {other}"),
		}
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		self.get_config(key)
	}
}

fn opkey(n: u8) -> reifydb_codec::key::encoded::EncodedKey {
	OperatorStateKey::encoded(NODE, vec![n])
}

fn commit_set(store: &StandardMultiStore, n: u8, version: u64) {
	MultiVersionCommit::commit(
		store,
		CowVec::new(vec![Delta::Set {
			key: opkey(n),
			row: EncodedRow(CowVec::new(b"live".to_vec())),
		}]),
		CommitVersion(version),
	)
	.unwrap();
}

fn commit_remove(store: &StandardMultiStore, n: u8, version: u64) {
	MultiVersionCommit::commit(store, CowVec::new(vec![Delta::remove_silent(opkey(n))]), CommitVersion(version))
		.unwrap();
}

fn work_done(plane: &RetentionPlane) -> u64 {
	plane.snapshot(RetentionClass::TombstoneReap).work_done
}

#[test]
fn reaps_only_tombstones_at_or_below_the_flush_watermark() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	commit_set(&store, 1, 1);
	commit_remove(&store, 1, 2);
	commit_set(&store, 2, 3);
	store.flush_all_blocking();

	let watermark = ScriptedFlushWatermark(Arc::new(AtomicU64::new(0)));
	let plane = RetentionPlane::new(Arc::new(watermark.clone()), VersionEpoch::new());
	let mut task = TombstoneReapTask::new(
		store.clone(),
		plane.clone(),
		Clock::testing(),
		Arc::new(StubConfig {
			batch_size: 1024,
		}),
	);

	watermark.0.store(1, Ordering::SeqCst);
	let blocked = task.run_slice();
	assert_eq!(blocked, Progress::Exhausted, "no eligible tombstone must report Exhausted, never a spin");
	assert_eq!(work_done(&plane), 0, "a tombstone above the flush watermark must not be reaped");

	watermark.0.store(100, Ordering::SeqCst);
	let reaped = task.run_slice();
	assert_eq!(reaped, Progress::Exhausted, "one tombstone under a 1024 batch drains in a single slice");
	assert_eq!(
		work_done(&plane),
		1,
		"once the watermark passes the tombstone version it is reaped - proving it survived the blocked slice"
	);

	assert!(
		MultiVersionGet::get(&store, &opkey(2), CommitVersion(u64::MAX)).unwrap().is_some(),
		"a live operator row must never be reaped"
	);
}

#[test]
fn a_backlog_larger_than_the_batch_drains_across_slices_with_yield() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	for n in 1..=3u8 {
		commit_set(&store, n, (n as u64) * 2 - 1);
		commit_remove(&store, n, (n as u64) * 2);
	}
	store.flush_all_blocking();

	let watermark = ScriptedFlushWatermark(Arc::new(AtomicU64::new(100)));
	let plane = RetentionPlane::new(Arc::new(watermark), VersionEpoch::new());
	let mut task = TombstoneReapTask::new(
		store.clone(),
		plane.clone(),
		Clock::testing(),
		Arc::new(StubConfig {
			batch_size: 2,
		}),
	);

	let first = task.run_slice();
	assert_eq!(
		first,
		Progress::Yielded,
		"a batch of 2 against 3 tombstones must yield so the catch-up tick drains it"
	);
	assert_eq!(work_done(&plane), 2, "the first slice reaps exactly one batch");

	let second = task.run_slice();
	assert_eq!(second, Progress::Exhausted, "the final tombstone drains and the slice reports Exhausted");
	assert_eq!(work_done(&plane), 3, "every tombstone is reaped across the two slices");
}
