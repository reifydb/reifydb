// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::thread::sleep;

use reifydb::{embedded as db_embedded, testing::db::TestDb};
use reifydb_sub_store::subsystem::StorageConfig;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

#[test]
fn materialize_off_keeps_series_rows_and_writes_no_block() {
	// Same setup that seals [0,5) and [5,10) when on, so a block here means the actors still spawned.
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
		materialize: false,
		..StorageConfig::default()
	};

	let mut db = TestDb::from(db_embedded::memory().with_storage_config(config).build().expect("build"));

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");

	db.command(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
	);

	// About 20 ticks: a live seal would have drained the closed buckets well within this window.
	sleep(Duration::from_seconds(1).unwrap().to_std());

	{
		let engine = db.engine();
		let catalog = engine.catalog();
		let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
		let mut tx = Transaction::Query(&mut txn);
		let namespace = catalog
			.find_namespace_by_name(&mut tx, "test")
			.expect("find namespace")
			.expect("namespace test");
		let series = catalog
			.find_series_by_name(&mut tx, namespace.id(), "s")
			.expect("find series")
			.expect("series test::s");
		let blocks = catalog.list_column_blocks_for_series(&mut tx, series.id).expect("list series blocks");
		assert!(blocks.is_empty(), "materialize off must write no block, got {}", blocks.len());
	}

	// A drained row would vanish from the row scan, which is the data loss the switch must prevent.
	assert_eq!(db.row_count("FROM test::s"), 12);

	db.stop();
}

#[test]
fn materialize_defaults_off() {
	// Defaulting to on would spawn the table and series actors in deployments that never asked for them.
	assert!(!StorageConfig::default().materialize);
}
