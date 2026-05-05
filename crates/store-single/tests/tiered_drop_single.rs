// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb_core::event::EventBus;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_single::{
	buffer::tier::BufferTier,
	config::{BufferConfig, PersistentConfig, SingleStoreConfig},
	store::StandardSingleStore,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-single/tests/scripts/drop" as tiered_drop_single => test_tiered }

fn test_tiered(path: &Path) {
	temp_dir(|_db_path| {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let store = StandardSingleStore::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: BufferTier::memory(),
			}),
			persistent: Some(PersistentConfig::sqlite_in_memory()),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		run_path(&mut Runner::from_store_auto_flush(store), path)
	})
	.expect("test failed")
}
