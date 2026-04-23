// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::BTreeMap,
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

fn utf8_strings(block: &ColumnBlock, name: &str) -> Vec<String> {
	let (_, chunked) = block.column_by_name(name).unwrap_or_else(|| panic!("column `{name}` missing"));
	let canonical = chunked.chunks[0].to_canonical().expect("to_canonical");
	match &canonical.storage {
		CanonicalStorage::VarLen(v) => {
			(0..v.len()).map(|i| std::str::from_utf8(v.bytes_at(i)).expect("utf8").to_string()).collect()
		}
		_ => panic!("column `{name}` is not a varlen column"),
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

	let meta = poll_until(
		|| {
			let snaps = registry.list();
			snaps.into_iter().find(|s| s.name == "t" && s.row_count == 3)
		},
		Duration::from_secs(5),
	)
	.expect("snapshot with 3 rows did not appear in registry within 5 seconds");

	assert_eq!(meta.namespace, "test");
	assert_eq!(meta.row_count, 3);

	let snap = registry.get(&meta.id).expect("snapshot fetchable by id");
	let block = &snap.block;

	let schema_names: Vec<&str> = block.schema.iter().map(|(n, _, _)| n.as_str()).collect();
	assert_eq!(schema_names, vec!["id", "name", "score"]);

	let ids = fixed_slice::<i32>(block, "id");
	let names = utf8_strings(block, "name");
	let scores = fixed_slice::<f64>(block, "score");

	assert_eq!(ids.len(), 3);
	assert_eq!(names.len(), 3);
	assert_eq!(scores.len(), 3);

	let actual: BTreeMap<i32, (String, f64)> =
		ids.iter().zip(names.iter()).zip(scores.iter()).map(|((id, n), s)| (*id, (n.clone(), *s))).collect();
	let expected: BTreeMap<i32, (String, f64)> = BTreeMap::from([
		(1, ("alpha".to_string(), 1.5)),
		(2, ("bravo".to_string(), 2.5)),
		(3, ("charlie".to_string(), 3.5)),
	]);
	assert_eq!(actual, expected);

	db.stop().expect("stop");
}
