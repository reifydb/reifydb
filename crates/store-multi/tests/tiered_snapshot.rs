// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb_core::event::EventBus;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	config::{HotConfig, MultiStoreConfig, WarmConfig},
	hot::storage::HotStorage,
	store::StandardMultiStore,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/flush" as ts_flush => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/versions" as ts_versions => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/tombstones" as ts_tombstones => test_snapshot }
test_each_path! { in "crates/store-multi/tests/scripts/tiered_snapshot/cascade" as ts_cascade => test_snapshot }

fn test_snapshot(path: &Path) {
	temp_dir(|_db_path| {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let store = StandardMultiStore::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
			}),
			warm: Some(WarmConfig::sqlite_in_memory()),
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		run_path(&mut Runner::from_store_no_auto_flush(store), path)
	})
	.expect("test failed")
}
