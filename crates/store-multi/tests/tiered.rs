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
	buffer::storage::BufferStorage,
	config::{BufferConfig, MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/multi" as store_multi_tiered => test_tiered }

fn test_tiered(path: &Path) {
	temp_dir(|_db_path| {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let store = StandardMultiStore::new(MultiStoreConfig {
			buffer: Some(BufferConfig {
				storage: BufferStorage::memory(),
			}),
			persistent: Some(PersistentConfig::sqlite_in_memory()),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		run_path(&mut Runner::from_store(store), path)
	})
	.expect("test failed")
}
