// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_ringbuffer_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create ringbuffer demo::rb { id: uint8 } with { capacity: 10 }");

	let cat = db.catalog();
	let mat = cat.materialized();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let rb = mat.find_ringbuffer_by_name(ns.id(), "rb").unwrap();
	assert_eq!(rb.name, "rb");
	assert_eq!(rb.namespace, ns.id());
	assert_eq!(rb.capacity, 10);
}
