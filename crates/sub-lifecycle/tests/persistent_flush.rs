// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! PersistentFlushTask: the scheduler wrapped around the multi flush engine.
//!
//! `store-multi` covers the sweep itself, so what is testable here is the seam the engine cannot see: the
//! per-slice byte budget is read from MULTI_FLUSH_BUDGET_BYTES on every slice, so an operator sizing the
//! budget down actually gets a smaller slice instead of the engine's compiled fallback.

use std::sync::Arc;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			storage::StorageId,
		},
		store::MultiVersionCommit,
	},
	lifecycle::{progress::Progress, task::LifecycleTask, watermark::EvictionWatermark},
};
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_store_multi::{
	flush::{ObjectPersistence, engine::FLUSH_BYTE_BUDGET},
	store::StandardMultiStore,
};
use reifydb_sub_lifecycle::{
	plane::{RetentionPlane, ledger::FloorSource},
	store::flush::PersistentFlushTask,
};
use reifydb_value::{
	byte_size::ByteSize,
	util::cowvec::CowVec,
	value::{Value, duration::Duration},
};

const ROWS: u8 = 8;

struct Unpinned;

impl FloorSource for Unpinned {
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
}

struct StaticWatermark(CommitVersion);

impl EvictionWatermark for StaticWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

struct AllPersistent;

impl ObjectPersistence for AllPersistent {
	fn is_persistent(&self, _storage: StorageId) -> bool {
		true
	}
}

struct StubConfig(u64);

impl GetConfig for StubConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		match key {
			ConfigKey::MultiFlushBudgetBytes => Value::Uint8(self.0),
			other => panic!("the persistent flush task must not read config key {other}"),
		}
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		self.get_config(key)
	}
}

fn seeded_task(store: &StandardMultiStore, budget: u64) -> PersistentFlushTask {
	store.set_row_settings_provider(Arc::new(AllPersistent));
	for n in 1..=ROWS {
		MultiVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Set {
				key: EncodedKey::new(format!("k{n}").into_bytes()),
				bytes: EncodedBytes(CowVec::new(b"v".to_vec())),
			}]),
			CommitVersion(n as u64),
		)
		.unwrap();
	}
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(u64::MAX))));

	let plane = RetentionPlane::new(Arc::new(Unpinned), VersionEpoch::new());
	PersistentFlushTask::new(
		store.flush_engine().expect("a persistent store must build a flush engine"),
		Arc::new(StubConfig(budget)),
		plane,
		Clock::testing(),
		Duration::from_seconds(60).unwrap(),
	)
}

#[test]
fn the_configured_budget_bounds_one_slice() {
	// The engine carries a 4 MiB compiled fallback that would swallow this whole backlog in one slice, so
	// a task that ignored MULTI_FLUSH_BUDGET_BYTES and reached for that const would drain here and report
	// Exhausted. Only a task that actually reads the configured budget can be held to one row per slice.
	let (throttled_store, _throttled_guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let mut throttled = seeded_task(&throttled_store, 1);
	assert_eq!(
		throttled.run_slice(),
		Progress::Yielded,
		"a one-byte budget must buy one row and yield with the rest still pending"
	);

	let (open_store, _open_guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let mut unthrottled = seeded_task(&open_store, u64::MAX);
	assert_eq!(
		unthrottled.run_slice(),
		Progress::Exhausted,
		"the same backlog under an unbounded budget must drain in a single slice, or the throttled arm \
		 proves nothing about the budget"
	);
}

#[test]
fn the_catalog_default_budget_matches_the_engine_compiled_fallback() {
	// Two independent defaults size the same sweep: the catalog answers an unconfigured store, the const
	// answers the drain paths. Drift between them makes the scheduled slice and the shutdown slice
	// silently different sizes.
	assert_eq!(
		ConfigKey::MultiFlushBudgetBytes.default_value(),
		Value::Uint8(FLUSH_BYTE_BUDGET.as_bytes()),
		"the catalog default and the engine fallback must be the same number of bytes"
	);
	assert_eq!(FLUSH_BYTE_BUDGET, ByteSize::from_mib(4), "the compiled fallback is the documented 4 MiB");
}
