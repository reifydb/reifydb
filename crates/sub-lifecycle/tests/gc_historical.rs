// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Historical GC task: reclaiming superseded versions without ever passing the reader floor.
//!
//! Two ways this class can be wrong, and they fail in opposite directions:
//!
//! - Reclaim too much - drop a version at or above `effective_gc_cutoff` - and a live reader's snapshot loses a row
//!   mid-query. That is silent data loss, not a stall.
//! - Reclaim too little - treat a zero or absent watermark as "sweep everything", or bail before the backlog is drained
//!   - and superseded versions accumulate forever, which is the unbounded-memory failure.
//!
//! `store-multi`'s own tests cover `scan_historical_below`, the tier primitive. What is only testable here is the
//! task wrapped around it: that it honours the floor it is handed, that a zero floor means do nothing rather than
//! everything, and that it keeps its own cursor across slices.

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::TableId,
			object::ObjectId,
		},
		store::EntryKind,
	},
	lifecycle::{progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::{store::StandardMultiStore, tier::TierStorage};
use reifydb_sub_lifecycle::{
	gc::historical::actor::HistoricalGcTask,
	plane::{RetentionPlane, ledger::FloorSource},
};
use reifydb_value::{util::cowvec::CowVec, value::Value};

const SHAPE: EntryKind = EntryKind::Source(ObjectId::Table(TableId(1)));

/// Only `query_done_until` is scripted; the other reader terms sit wide open, so each test drives the
/// BufferHistoricalGc cutoff through exactly one term.
#[derive(Clone)]
struct ScriptedWatermark(Arc<AtomicU64>);

impl FloorSource for ScriptedWatermark {
	fn query_done_until(&self) -> CommitVersion {
		CommitVersion(self.0.load(Ordering::SeqCst))
	}

	fn lease_min(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn consumer_checkpoint(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn subscription_snapshot(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn flush_watermark(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}

	fn owning_flow_checkpoint(&self) -> CommitVersion {
		CommitVersion(u64::MAX)
	}
}

struct StubConfig;

impl GetConfig for StubConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		match key {
			ConfigKey::HistoricalGcBatchSize => Value::Uint8(1024),
			other => panic!("historical gc must not read config key {other}"),
		}
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		self.get_config(key)
	}
}

fn key(name: &str) -> EncodedKey {
	EncodedKey::new(name.as_bytes().to_vec())
}

fn value(payload: &str) -> CowVec<u8> {
	CowVec::new(payload.as_bytes().to_vec())
}

fn store() -> StandardMultiStore {
	let pools = Pools::new(PoolConfig::sync_only());
	let clock = Clock::testing();
	let actor_system = ActorSystem::new(pools, clock);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	StandardMultiStore::testing_memory_with_eventbus(event_bus)
}

/// Writes `versions` successive values for one key, so every version below the newest is superseded.
fn write_versions(store: &StandardMultiStore, name: &str, versions: u64) {
	let buffer = store.commit();
	for v in 1..=versions {
		buffer.set(
			CommitVersion(v),
			HashMap::from([(SHAPE, vec![(key(name), Some(value(&format!("v{v}"))))])]),
		)
		.unwrap();
	}
}

fn visible_at(store: &StandardMultiStore, name: &str, version: u64) -> Option<String> {
	let buffer = store.commit();
	buffer.get(SHAPE, key(name).as_ref(), CommitVersion(version))
		.unwrap()
		.value()
		.map(|v| String::from_utf8_lossy(v.as_ref()).to_string())
}

fn task(store: StandardMultiStore, watermark: ScriptedWatermark) -> HistoricalGcTask {
	let plane = RetentionPlane::new(Arc::new(watermark), VersionEpoch::new());
	HistoricalGcTask::new(store, plane, Clock::testing(), Arc::new(StubConfig))
}

#[test]
fn a_zero_watermark_reclaims_nothing_rather_than_everything() {
	// Zero is the "no floor established yet" state - during boot, or before any reader has registered. Reading it
	// as an unbounded cutoff would reclaim the entire version space of a freshly started database.
	let store = store();
	write_versions(&store, "k", 5);
	let watermark = ScriptedWatermark(Arc::new(AtomicU64::new(0)));
	let mut task = task(store.clone(), watermark);

	assert_eq!(task.run_slice(), Progress::Exhausted);

	for v in 1..=5u64 {
		assert_eq!(
			visible_at(&store, "k", v),
			Some(format!("v{v}")),
			"a zero watermark must leave version {v} untouched"
		);
	}
}

#[test]
fn never_reclaims_a_version_a_reader_at_the_floor_can_still_reach() {
	// The floor is inclusive: a reader holding version 3 must still resolve version 3 after the sweep. Dropping
	// at the cutoff instead of strictly below it is the classic off-by-one that loses a live snapshot's row.
	let store = store();
	write_versions(&store, "k", 5);
	let watermark = ScriptedWatermark(Arc::new(AtomicU64::new(3)));
	let mut task = task(store.clone(), watermark);

	task.run_slice();

	assert_eq!(
		visible_at(&store, "k", 3),
		Some("v3".to_string()),
		"the version at the floor must survive - a reader pinned there is still using it"
	);
	assert_eq!(visible_at(&store, "k", 5), Some("v5".to_string()), "versions above the floor must survive");
}

#[test]
fn raising_the_floor_reclaims_the_versions_it_releases() {
	// The liveness half: once readers move on, the superseded versions must actually go. A task that honours
	// the floor but never reclaims anything is indistinguishable from the leak it is meant to prevent.
	let store = store();
	write_versions(&store, "k", 5);
	let floor = Arc::new(AtomicU64::new(0));
	let mut task = task(store.clone(), ScriptedWatermark(floor.clone()));

	floor.store(5, Ordering::SeqCst);
	task.run_slice();

	assert_eq!(
		visible_at(&store, "k", 5),
		Some("v5".to_string()),
		"the newest version must always survive - it is the current value, not history"
	);
	assert_eq!(
		visible_at(&store, "k", 1),
		None,
		"a superseded version released by the floor must actually be reclaimed"
	);
}

#[test]
fn reports_exhausted_so_the_lane_does_not_spin_on_a_drained_store() {
	// This class drains its whole backlog per slice. Reporting Yielded would make the lane immediately schedule
	// a catch-up tick that has nothing to do, burning the lane other classes are queued behind.
	let store = store();
	let mut task = task(store, ScriptedWatermark(Arc::new(AtomicU64::new(10))));

	assert_eq!(
		task.run_slice(),
		Progress::Exhausted,
		"an empty store must report Exhausted, not schedule a pointless catch-up"
	);
}

#[test]
fn a_second_slice_after_the_floor_advances_reclaims_the_newly_released_versions() {
	// Cursors persist across slices. If a cursor were left exhausted and never reset, the next sweep would skip
	// the shape entirely and versions released later would never be collected - a leak that only appears after
	// the first successful sweep, which is exactly the kind that survives a short test.
	let store = store();
	write_versions(&store, "k", 6);
	let floor = Arc::new(AtomicU64::new(2));
	let mut task = task(store.clone(), ScriptedWatermark(floor.clone()));

	task.run_slice();
	assert_eq!(visible_at(&store, "k", 2), Some("v2".to_string()), "version at the first floor survives");

	floor.store(6, Ordering::SeqCst);
	task.run_slice();

	assert_eq!(
		visible_at(&store, "k", 2),
		None,
		"raising the floor and sweeping again must reclaim what the first sweep had to keep"
	);
	assert_eq!(visible_at(&store, "k", 6), Some("v6".to_string()), "the current value still survives");
}
