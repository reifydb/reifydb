// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_single::{
	buffer::tier::SingleBufferTier,
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
		let (persistent, _guard) = PersistentConfig::sqlite_in_memory();
		let store = StandardSingleStore::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: Some(persistent),
			actor_system,
			clock: Clock::Real,
		})
		.unwrap();
		run_path(&mut Runner::from_store_auto_flush(store), path)
	})
	.expect("test failed")
}
