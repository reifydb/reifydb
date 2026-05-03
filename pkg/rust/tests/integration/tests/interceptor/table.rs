// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_table_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::t { id: uint8 }");

	let cat = db.catalog();
	let mat = cat.materialized();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let table = mat.find_table_by_name(ns.id(), "t").unwrap();
	assert_eq!(table.name, "t");
	assert_eq!(table.namespace, ns.id());
}
