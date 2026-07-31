// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! VacuumBudget task: bounded incremental_vacuum that keeps the persistent file from ratcheting.
//!
//! `store-multi`'s own tests cover `freelist_page_count` and `incremental_vacuum`, the tier primitives. What is only
//! testable here is the task wrapped around them: that it leaves the freelist alone below the configured threshold,
//! and that above it a freelist larger than the per-slice page bound drains across slices under the three-way pacing
//! rule rather than one unbounded vacuum.

use std::sync::Arc;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowNodeId,
		},
		store::{EntryKind, MultiVersionCommit},
	},
	key::flow_node_state::FlowNodeStateKey,
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_sub_lifecycle::{
	plane::{RetentionPlane, ledger::FloorSource},
	store::vacuum::VacuumBudgetTask,
};
use reifydb_value::{util::cowvec::CowVec, value::Value};

const NODE: FlowNodeId = FlowNodeId(1);

/// Vacuum reclaims free pages, not versioned data, so no reader floor constrains it; every term sits wide open.
struct NoFloors;

impl FloorSource for NoFloors {
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
		CommitVersion(u64::MAX)
	}

	fn owning_flow_checkpoint(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}
}

struct StubConfig {
	threshold: u64,
	pages_per_slice: u64,
}

impl GetConfig for StubConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		match key {
			ConfigKey::VacuumInterval => Value::duration_seconds(60),
			ConfigKey::VacuumFreelistThresholdPercent => Value::Uint8(self.threshold),
			ConfigKey::VacuumPagesPerSlice => Value::Uint8(self.pages_per_slice),
			other => panic!("the vacuum task must not read config key {other}"),
		}
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		self.get_config(key)
	}
}

fn opkey(n: u64) -> reifydb_codec::key::encoded::EncodedKey {
	FlowNodeStateKey::encoded(NODE, n.to_be_bytes().to_vec())
}

/// Writes and flushes 500 fat rows, then deletes them from the persistent tier so their pages land on the freelist
/// (auto_vacuum=INCREMENTAL does not reclaim them until incremental_vacuum runs). Returns (freelist, page_count).
fn seed_freelist(store: &StandardMultiStore) -> (u64, u64) {
	for n in 1..=500u64 {
		MultiVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Set {
				key: opkey(n),
				row: EncodedRow(CowVec::new(vec![0u8; 200])),
			}]),
			CommitVersion(n),
		)
		.unwrap();
	}
	store.flush_all_blocking();
	let persistent = store.persistent().expect("persistent tier configured");
	let keys: Vec<_> = (1..=500u64).map(opkey).collect();
	persistent.delete_keys(EntryKind::Operator(NODE), &keys).unwrap();
	persistent.freelist_page_count().unwrap()
}

fn work_done(plane: &RetentionPlane) -> u64 {
	plane.snapshot(RetentionClass::VacuumBudget).work_done
}

#[test]
fn drains_the_freelist_across_slices_with_yield() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let (freelist, _) = seed_freelist(&store);
	assert!(freelist >= 4, "the test needs a freelist larger than the per-slice bound, got {freelist}");

	let plane = RetentionPlane::new(Arc::new(NoFloors), VersionEpoch::new());
	let mut task = VacuumBudgetTask::new(
		store.clone(),
		plane.clone(),
		Arc::new(StubConfig {
			threshold: 0,
			pages_per_slice: 2,
		}),
	);

	// The first slice must not vacuum the whole freelist at once: it reclaims at most the per-slice bound and
	// yields so the catch-up tick drains the rest. (incremental_vacuum may reclaim fewer than requested when free
	// pages have not yet migrated to the file end, so the count is bounded above, not fixed.)
	let first = task.run_slice();
	assert_eq!(
		first,
		Progress::Yielded,
		"a freelist above the page bound must yield rather than vacuum all at once"
	);
	let after_first = work_done(&plane);
	assert!(
		after_first >= 1 && after_first <= 2,
		"the first slice reclaims a bounded, non-zero page count, got {after_first}"
	);

	let mut slices = 1u64;
	while task.run_slice() == Progress::Yielded {
		slices += 1;
	}

	let (freelist_after, _) = store.persistent().unwrap().freelist_page_count().unwrap();
	assert!(
		freelist_after < freelist,
		"the vacuum campaign must reduce the freelist, {freelist} -> {freelist_after}"
	);
	assert_eq!(
		work_done(&plane),
		freelist - freelist_after,
		"cumulative pages moved must equal the freelist reduction the campaign achieved"
	);
	assert!(
		slices >= 2,
		"a bounded per-slice vacuum of {freelist} freed pages must take multiple slices, took {slices}"
	);
}

#[test]
fn leaves_the_freelist_alone_below_the_threshold() {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let (freelist, pages) = seed_freelist(&store);
	assert!(freelist > 0 && pages > 0, "precondition: a non-empty freelist in a non-empty file");

	let ratio_pct = freelist * 100 / pages;
	let plane = RetentionPlane::new(Arc::new(NoFloors), VersionEpoch::new());
	let mut task = VacuumBudgetTask::new(
		store.clone(),
		plane.clone(),
		Arc::new(StubConfig {
			threshold: ratio_pct + 10,
			pages_per_slice: 1024,
		}),
	);

	let progress = task.run_slice();
	assert_eq!(progress, Progress::Exhausted, "a freelist below the threshold ratio must not trigger a vacuum");
	assert_eq!(work_done(&plane), 0, "nothing may be reclaimed while the freelist ratio is below the threshold");

	let (freelist_after, _) = store.persistent().unwrap().freelist_page_count().unwrap();
	assert_eq!(freelist_after, freelist, "the freelist must be untouched below the threshold");
}
