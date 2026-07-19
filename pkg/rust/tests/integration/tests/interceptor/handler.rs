// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::core::common::CommitVersion;
use reifydb_test_harness::db::TestDb;

#[test]
fn create_handler_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create table demo::audit { kind: utf8 }");
	db.admin("create event demo::order_event { OrderPlaced { id: int4 } }");
	db.admin(
		"create handler demo::on_placed on demo::order_event::OrderPlaced { insert demo::audit [{ kind: \"placed\" }] }",
	);

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let handler = mat.find_handler_by_name_at(ns.id(), "on_placed", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(handler.name, "on_placed");
	assert_eq!(handler.namespace, ns.id());
}
