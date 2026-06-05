// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Params,
	core::{interface::catalog::shape::ShapeId, row::TtlCleanupMode},
};

use super::common::{admin, fresh_db};

#[test]
fn create_table_with_row_settings_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::t { id: uint8 } with { row: { ttl: { duration: '1m', mode: drop } } }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let table = mat.find_table_by_name(ns.id(), "t").unwrap();
	let settings = mat.find_row_settings(ShapeId::Table(table.id)).unwrap();
	let ttl = settings.ttl.expect("ttl should be set");
	assert_eq!(ttl.duration_nanos, 60_000_000_000);
	assert_eq!(ttl.cleanup_mode, TtlCleanupMode::Drop);
	assert!(settings.persistent, "persistent defaults to true when omitted");
}

#[test]
fn create_table_persistent_false_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(
		&db,
		"create table demo::t { id: uint8 } with { row: { ttl: { duration: '1m', mode: drop }, persistent: false } }",
	);

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let table = mat.find_table_by_name(ns.id(), "t").unwrap();
	let settings = mat.find_row_settings(ShapeId::Table(table.id)).unwrap();
	assert!(!settings.persistent, "persistent: false should be stored");
	assert_eq!(settings.ttl.expect("ttl should be set").duration_nanos, 60_000_000_000);
}

#[test]
fn create_table_persistent_false_without_ttl_is_rejected() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	let result = db
		.admin_as_root("create table demo::t { id: uint8 } with { row: { persistent: false } }", Params::None);
	assert!(result.is_err(), "persistent: false without a ttl must be rejected");
}
