// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_enum_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create enum demo::status { Active, Inactive }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let st = mat.find_sumtype_by_name(ns.id(), "status").unwrap();
	assert_eq!(st.name, "status");
	assert_eq!(st.namespace, ns.id());
	let names: Vec<&str> = st.variants.iter().map(|v| v.name.as_str()).collect();
	assert_eq!(names, vec!["active", "inactive"]);
}
