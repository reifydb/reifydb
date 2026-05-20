// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_series_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create series demo::s { ts: datetime, v: int2 } with { key: ts }");

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let series = mat.find_series_by_name(ns.id(), "s").unwrap();
	assert_eq!(series.name, "s");
	assert_eq!(series.namespace, ns.id());
	assert_eq!(series.key.column(), "ts");
}
