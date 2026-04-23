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

// Exercises the series actor's bucket enumeration + `is_closed` path with
// integer keys. For integer-keyed series, a bucket is considered closed once
// `metadata.newest_key >= bucket.end`, so inserting keys past a bucket
// boundary closes prior buckets on the next tick.
#[test]
fn series_materialization_produces_snapshot_in_registry() {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_millis(50),
		series_tick_interval: Duration::from_millis(50),
		// Small integer bucket width — with keys 0..=11 and width 5,
		// buckets are [0,5), [5,10), [10,15). newest_key=11 closes the
		// first two buckets.
		series_bucket_width: 5,
		series_grace: Duration::from_millis(0),
	};

	let mut db = db_embedded::memory()
		.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
		.build()
		.expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
	db.admin_as_root("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }", Params::None)
		.expect("create series");

	db.command_as_root(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
		Params::None,
	)
	.expect("insert");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let registry = storage.registry();

	let snaps = poll_until(
		|| {
			let list: Vec<_> = registry.list().into_iter().filter(|s| s.name == "s").collect();
			if list.len() >= 2 {
				Some(list)
			} else {
				None
			}
		},
		Duration::from_secs(5),
	)
	.expect("at least two series buckets did not materialize within 5 seconds");

	assert!(snaps.len() >= 2, "expected >= 2 closed-bucket snapshots, got {}", snaps.len());
	for snap in &snaps {
		assert_eq!(snap.namespace, "test");
		assert!(snap.row_count > 0);
	}

	db.stop().expect("stop");
}
