// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_series_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create series demo::s { ts: datetime, v: int2 } with { key: ts }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let series = mat.find_series_by_name(ns.id(), "s").unwrap();
	assert_eq!(series.name, "s");
	assert_eq!(series.namespace, ns.id());
	assert_eq!(series.key.column(), "ts");
}
