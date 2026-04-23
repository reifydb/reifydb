// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::BTreeSet,
	thread,
	time::{Duration, Instant},
};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_column::{
	array::{canonical::CanonicalStorage, fixed::Primitive},
	column_block::ColumnBlock,
};
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

fn fixed_slice<T: Primitive>(block: &ColumnBlock, name: &str) -> Vec<T> {
	let (_, chunked) = block.column_by_name(name).unwrap_or_else(|| panic!("column `{name}` missing"));
	let canonical = chunked.chunks[0].to_canonical().expect("to_canonical");
	match &canonical.storage {
		CanonicalStorage::Fixed(f) => f
			.try_as_slice::<T>()
			.unwrap_or_else(|| panic!("column `{name}` is not the requested fixed type"))
			.to_vec(),
		_ => panic!("column `{name}` is not a fixed-width column"),
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
		// Small integer bucket width - with keys 0..=11 and width 5,
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

	let metas = poll_until(
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

	assert!(metas.len() >= 2, "expected >= 2 closed-bucket snapshots, got {}", metas.len());

	let mut all_keys: BTreeSet<u64> = BTreeSet::new();
	for meta in &metas {
		assert_eq!(meta.namespace, "test");
		assert!(meta.row_count > 0);

		let snap = registry.get(&meta.id).expect("snapshot fetchable by id");
		let block = &snap.block;

		let schema_names: Vec<&str> = block.schema.iter().map(|(n, _, _)| n.as_str()).collect();
		assert_eq!(schema_names, vec!["k", "value"]);

		let ks = fixed_slice::<u64>(block, "k");
		let values = fixed_slice::<f64>(block, "value");
		assert_eq!(ks.len(), values.len());
		assert_eq!(ks.len(), meta.row_count);

		for (k, v) in ks.iter().zip(values.iter()) {
			assert_eq!(*v, *k as f64, "value should equal key for k={k}");
			assert!(all_keys.insert(*k), "duplicate key {k} across snapshots");
		}
	}

	for k in 0u64..=9 {
		assert!(all_keys.contains(&k), "expected key {k} from closed buckets [0,5) and [5,10)");
	}

	db.stop().expect("stop");
}
