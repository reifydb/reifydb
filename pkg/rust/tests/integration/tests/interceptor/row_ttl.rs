// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::{
	interface::catalog::shape::ShapeId,
	row::{TtlAnchor, TtlCleanupMode},
};

use super::common::{admin, fresh_db};

#[test]
fn create_table_with_row_ttl_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(
		&db,
		"create table demo::t { id: uint8 } with { row: { ttl: { duration: '1m', on: created, mode: drop } } }",
	);

	let cat = db.catalog();
	let mat = cat.materialized();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let table = mat.find_table_by_name(ns.id(), "t").unwrap();
	let ttl = mat.find_row_ttl(ShapeId::Table(table.id)).unwrap();
	assert_eq!(ttl.duration_nanos, 60_000_000_000);
	assert_eq!(ttl.anchor, TtlAnchor::Created);
	assert_eq!(ttl.cleanup_mode, TtlCleanupMode::Drop);
}
