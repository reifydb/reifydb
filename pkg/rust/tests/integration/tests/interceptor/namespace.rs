// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_namespace_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");

	let ns = db.catalog().cache().find_namespace_by_name("demo").unwrap();
	assert_eq!(ns.name(), "demo");
}
