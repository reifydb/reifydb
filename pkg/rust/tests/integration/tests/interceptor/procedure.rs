// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_procedure_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create procedure demo::greet as { \"hi\" }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let proc = mat.find_procedure_by_name(ns.id(), "greet").unwrap();
	assert_eq!(proc.name(), "greet");
	assert_eq!(proc.namespace(), ns.id());
}
