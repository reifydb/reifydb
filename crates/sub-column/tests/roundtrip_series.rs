// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_column::reader::SnapshotReader;
use reifydb_sub_column::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_type::value::Value;

mod common;
use common::poll_until;

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

		let schema_names: Vec<&str> = snap.block.schema.iter().map(|(n, _, _)| n.as_str()).collect();
		assert_eq!(schema_names, vec!["k", "value"]);

		let mut reader = SnapshotReader::new(Arc::clone(&snap), 100);
		let batch = reader.next().expect("batch present").expect("read batch");
		assert!(reader.next().is_none(), "reader should yield a single batch per bucket");
		assert_eq!(batch.row_count(), meta.row_count);

		let k_col = batch.column("k").expect("k column");
		let v_col = batch.column("value").expect("value column");

		for i in 0..meta.row_count {
			let k = match k_col.data().get_value(i) {
				Value::Uint8(v) => v,
				other => panic!("row {i}: expected Uint8, got {other:?}"),
			};
			let v = match v_col.data().get_value(i) {
				Value::Float8(v) => f64::from(v),
				other => panic!("row {i}: expected Float8, got {other:?}"),
			};
			assert_eq!(v, k as f64, "value should equal key for k={k}");
			assert!(all_keys.insert(k), "duplicate key {k} across snapshots");
		}
	}

	for k in 0u64..=9 {
		assert!(all_keys.contains(&k), "expected key {k} from closed buckets [0,5) and [5,10)");
	}

	db.stop().expect("stop");
}
