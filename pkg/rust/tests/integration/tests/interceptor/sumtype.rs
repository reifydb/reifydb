// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_enum_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create enum demo::status { Active, Inactive }");

	let cat = db.catalog();
	let mat = cat.materialized();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let st = mat.find_sumtype_by_name(ns.id(), "status").unwrap();
	assert_eq!(st.name, "status");
	assert_eq!(st.namespace, ns.id());
	let names: Vec<&str> = st.variants.iter().map(|v| v.name.as_str()).collect();
	assert_eq!(names, vec!["active", "inactive"]);
}
