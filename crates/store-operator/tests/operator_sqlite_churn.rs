// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use reifydb_value::value::duration::Duration;
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-operator/tests/scripts/state" as operator_churn_state => test_churn }
test_each_path! { in "crates/store-operator/tests/scripts/checkpoint" as operator_churn_checkpoint => test_churn }
test_each_path! { in "crates/store-operator/tests/scripts/anchor" as operator_churn_anchor => test_churn }

fn test_churn(path: &Path) {
	// the flusher races every command here, so a read that depends on where a row currently lives must fail
	for read_pool_size in [1u32, 2, 4] {
		temp_dir(|_db_path| {
			let pools = Pools::new(PoolConfig::default());
			let actor_system = ActorSystem::new(pools, Clock::Real);
			let spawner = actor_system.spawner();
			std::mem::forget(actor_system);
			let (sqlite_config, _guard) = SqliteConfig::in_memory();
			let sqlite_config = sqlite_config.read_pool_size(read_pool_size);
			let store = OperatorStore::standard(OperatorStoreConfig {
				commit: Default::default(),
				persistent: Some(OperatorPersistentConfig::sqlite(sqlite_config)
					.flush_interval(Duration::from_milliseconds_const(1))),
				point: Some(OperatorPointConfig::default()),
				range: Some(OperatorRangeConfig::default()),
				spawner,
				clock: Clock::Real,
			});
			run_path(&mut Runner::from_store_no_auto_flush(store), path)
		})
		.unwrap_or_else(|e| panic!("read_pool_size={read_pool_size}: {e}"));
	}
}
