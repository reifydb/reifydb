// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_procedure_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create procedure demo::greet as { \"hi\" }");

	let mat = &db.engine().catalog().materialized;
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let proc = mat.find_procedure_by_name(ns.id(), "greet").unwrap();
	assert_eq!(proc.name(), "greet");
	assert_eq!(proc.namespace(), ns.id());
}
