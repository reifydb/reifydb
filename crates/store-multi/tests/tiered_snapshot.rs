// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_core::event::EventBus;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_multi::{
	config::{CommitBufferConfig, MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
	tier::commit::buffer::MultiCommitBufferTier,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/flush" as ts_flush => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/versions" as ts_versions => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/tombstones" as ts_tombstones => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/cascade" as ts_cascade => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/versioned_get" as ts_versioned_get => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/get_many" as ts_get_many => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/seed" as ts_seed => test_snapshot }

fn test_snapshot(path: &Path) {
	for read_pool_size in [1u32, 2, 4] {
		temp_dir(|_db_path| {
			let pools = Pools::new(PoolConfig::default());
			let actor_system = ActorSystem::new(pools, Clock::Real);
			let spawner = actor_system.spawner();
			std::mem::forget(actor_system);
			let event_bus = EventBus::new(&spawner);
			let (sqlite_config, _guard) = SqliteConfig::in_memory();
			let sqlite_config = sqlite_config.read_pool_size(read_pool_size);
			let store = StandardMultiStore::new(MultiStoreConfig {
				commit: Some(CommitBufferConfig {
					storage: MultiCommitBufferTier::memory(),
				}),
				persistent: Some(PersistentConfig::sqlite(sqlite_config)),
				retention: Default::default(),
				merge_config: Default::default(),
				event_bus,
				spawner,
				clock: Clock::Real,
			})
			.unwrap();
			run_path(&mut Runner::from_store_no_auto_flush(store), path)
		})
		.unwrap_or_else(|e| panic!("read_pool_size={read_pool_size}: {e}"));
	}
}
