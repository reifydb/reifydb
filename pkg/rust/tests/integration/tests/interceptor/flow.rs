// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_view_propagates_flow_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create table demo::t { id: uint8 }");
	db.admin("create view demo::v { id: uint8 } as { from demo::t }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let flow = mat.find_flow_by_name(ns.id(), "v").unwrap();
	assert_eq!(flow.name, "v");
	assert_eq!(flow.namespace, ns.id());
}
