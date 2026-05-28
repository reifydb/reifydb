// SPDX-License-Identifier: AGPL-3.0-or-later
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
	buffer::tier::MultiBufferTier,
	config::{BufferConfig, MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/multi" as store_multi_tiered => test_tiered }
test_each_path! { in "crates/store-multi/tests/scripts/historical" as store_multi_tiered_historical => test_tiered }

fn test_tiered(path: &Path) {
	for read_pool_size in [1u32, 2, 4] {
		temp_dir(|_db_path| {
			let pools = Pools::new(PoolConfig::default());
			let actor_system = ActorSystem::new(pools, Clock::Real);
			let event_bus = EventBus::new(&actor_system);
			let (sqlite_config, _guard) = SqliteConfig::in_memory();
			let sqlite_config = sqlite_config.read_pool_size(read_pool_size);
			let store = StandardMultiStore::new(MultiStoreConfig {
				buffer: Some(BufferConfig {
					storage: MultiBufferTier::memory(),
				}),
				persistent: Some(PersistentConfig::sqlite(sqlite_config)),
				retention: Default::default(),
				merge_config: Default::default(),
				event_bus,
				actor_system,
				clock: Clock::Real,
			})
			.unwrap();
			run_path(&mut Runner::from_store(store), path)
		})
		.unwrap_or_else(|e| panic!("read_pool_size={read_pool_size}: {e}"));
	}
}
