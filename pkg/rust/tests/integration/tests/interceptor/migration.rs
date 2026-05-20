// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_migration_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create migration 'm1' { create namespace demo };");
	admin(&db, "migrate;");

	let cat = db.catalog();
	let mat = cat.cache();
	let migration = mat.find_migration_by_name("m1").unwrap();
	assert_eq!(migration.name, "m1");
	assert!(migration.body.contains("create namespace demo"));
}
