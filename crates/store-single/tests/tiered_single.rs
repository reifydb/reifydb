// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::Path, sync::Arc};

use reifydb_core::interface::store::SingleWalCheckpoint;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_single::{
	config::{CommitBufferConfig, PersistentConfig, SingleStoreConfig},
	store::StandardSingleStore,
	tier::commit::buffer::SingleCommitBufferTier,
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use reifydb_wal::lsn::Lsn;
use test_each_file::test_each_path;

mod common;
use common::Runner;

struct NoWal;

impl SingleWalCheckpoint for NoWal {
	fn make_durable(&self, _through: Lsn) -> reifydb_value::Result<()> {
		// Store-level scripts never log a commit, so there is no lsn to make durable.
		Ok(())
	}

	fn saved_through(&self, _through: Lsn) -> reifydb_value::Result<()> {
		Ok(())
	}
}

test_each_path! { in "crates/store-single/tests/scripts/single" as tiered_single => test_tiered }
test_each_path! { in "crates/store-single/tests/scripts/single" as tiered_single_sqlite_reads => test_tiered_sqlite_reads }

fn test_tiered(path: &Path) {
	run_tiered(path, false)
}

fn test_tiered_sqlite_reads(path: &Path) {
	// The buffer keeps every flushed value, so without a buffer-free reader no script read ever reaches sqlite.
	run_tiered(path, true)
}

fn run_tiered(path: &Path, sqlite_reads: bool) {
	temp_dir(|_db_path| {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let spawner = actor_system.spawner();
		std::mem::forget(actor_system);
		let (persistent, _guard) = PersistentConfig::sqlite_in_memory(Arc::new(NoWal));
		let reads = sqlite_reads.then(|| {
			StandardSingleStore::new(SingleStoreConfig {
				commit: None,
				persistent: Some(persistent.clone()),
				spawner: spawner.clone(),
				clock: Clock::Real,
			})
			.unwrap()
		});
		let store = StandardSingleStore::new(SingleStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: SingleCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			spawner,
			clock: Clock::Real,
		})
		.unwrap();
		run_path(
			&mut Runner {
				reads,
				..Runner::from_store_auto_flush(store)
			},
			path,
		)
	})
	.expect("test failed")
}
