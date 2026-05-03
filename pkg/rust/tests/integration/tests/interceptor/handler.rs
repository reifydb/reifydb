// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn create_handler_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::audit { kind: utf8 }");
	admin(&db, "create event demo::order_event { OrderPlaced { id: int4 } }");
	admin(
		&db,
		"create handler demo::on_placed on demo::order_event::OrderPlaced { insert demo::audit [{ kind: \"placed\" }] }",
	);

	let cat = db.catalog();
	let mat = cat.materialized();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let handler = mat.find_handler_by_name_at(ns.id(), "on_placed", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(handler.name, "on_placed");
	assert_eq!(handler.namespace, ns.id());
}
