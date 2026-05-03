// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_view_propagates_flow_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::t { id: uint8 }");
	admin(&db, "create view demo::v { id: uint8 } as { from demo::t }");

	let mat = &db.engine().catalog().materialized;
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let flow = mat.find_flow_by_name(ns.id(), "v").unwrap();
	assert_eq!(flow.name, "v");
	assert_eq!(flow.namespace, ns.id());
}
