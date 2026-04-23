// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	thread,
	time::{Duration, Instant},
};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_sub_column::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};

fn poll_until<T>(mut f: impl FnMut() -> Option<T>, timeout: Duration) -> Option<T> {
	let deadline = Instant::now() + timeout;
	loop {
		if let Some(value) = f() {
			return Some(value);
		}
		if Instant::now() >= deadline {
			return None;
		}
		thread::sleep(Duration::from_millis(10));
	}
}

#[test]
fn table_materialization_produces_snapshot_in_registry() {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_millis(50),
		series_tick_interval: Duration::from_millis(50),
		..StorageConfig::default()
	};

	let mut db = db_embedded::memory()
		.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
		.build()
		.expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE test::t { id: int4, name: utf8, score: float8 }", Params::None)
		.expect("create table");
	db.command_as_root(
		"INSERT test::t [{id: 1, name: \"alpha\", score: 1.5},\
		 {id: 2, name: \"bravo\", score: 2.5},\
		 {id: 3, name: \"charlie\", score: 3.5}]",
		Params::None,
	)
	.expect("insert");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let registry = storage.registry();

	let snapshot = poll_until(
		|| {
			let snaps = registry.list();
			snaps.into_iter().find(|s| s.name == "t" && s.row_count == 3)
		},
		Duration::from_secs(5),
	)
	.expect("snapshot with 3 rows did not appear in registry within 5 seconds");

	assert_eq!(snapshot.namespace, "test");
	assert_eq!(snapshot.row_count, 3);

	db.stop().expect("stop");
}
