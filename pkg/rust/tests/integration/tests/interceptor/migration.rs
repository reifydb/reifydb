// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_migration_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create migration 'm1' { create namespace demo };");
	admin(&db, "migrate;");

	let cat = db.catalog();
	let mat = cat.materialized();
	let migration = mat.find_migration_by_name("m1").unwrap();
	assert_eq!(migration.name, "m1");
	assert!(migration.body.contains("create namespace demo"));
}
