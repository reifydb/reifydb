// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_namespace_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");

	let ns = db.catalog().cache().find_namespace_by_name("demo").unwrap();
	assert_eq!(ns.name(), "demo");
}
