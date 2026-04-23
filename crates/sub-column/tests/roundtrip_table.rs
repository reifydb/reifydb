// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_column::reader::SnapshotReader;
use reifydb_sub_column::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_type::value::Value;

mod common;
use common::poll_until;

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

	let schema_names: Vec<&str> = snap.block.schema.iter().map(|(n, _, _)| n.as_str()).collect();
	assert_eq!(schema_names, vec!["id", "name", "score"]);

	let mut reader = SnapshotReader::new(Arc::clone(&snap), 100);
	let batch = reader.next().expect("read batch").expect("batch present");
	assert!(reader.next().expect("drain").is_none(), "reader should yield a single batch for 3 rows");
	assert_eq!(batch.row_count(), 3);

	let id_col = batch.column("id").expect("id column");
	let name_col = batch.column("name").expect("name column");
	let score_col = batch.column("score").expect("score column");

	let mut actual: BTreeMap<i32, (String, f64)> = BTreeMap::new();
	for i in 0..3 {
		let id = match id_col.data().get_value(i) {
			Value::Int4(v) => v,
			other => panic!("row {i}: expected Int4, got {other:?}"),
		};
		let name = match name_col.data().get_value(i) {
			Value::Utf8(s) => s,
			other => panic!("row {i}: expected Utf8, got {other:?}"),
		};
		let score = match score_col.data().get_value(i) {
			Value::Float8(v) => f64::from(v),
			other => panic!("row {i}: expected Float8, got {other:?}"),
		};
		actual.insert(id, (name, score));
	}
	let expected: BTreeMap<i32, (String, f64)> = BTreeMap::from([
		(1, ("alpha".to_string(), 1.5)),
		(2, ("bravo".to_string(), 2.5)),
		(3, ("charlie".to_string(), 3.5)),
	]);
	assert_eq!(actual, expected);

	db.stop().expect("stop");
}
