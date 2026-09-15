// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE SERIES s::m { ts: datetime, val: int4 } WITH { key: ts, tag: s::status }");
	t
}

fn command_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn row_count(t: &TestEngine, rql: &str) -> usize {
	t.query(rql).iter().map(|f| f.columns.first().map_or(0, |c| c.data.len())).sum()
}

#[test]
fn tagged_series_insert_with_a_qualified_tag_carrying_a_field_is_column_not_found() {
	// The tag stores only the variant, so a field on it must fail loud instead of being dropped.
	let t = engine();

	let err = command_err(
		&t,
		"INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: s::status::Active { x: 1 } }]",
	);

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "x", "got: {err:?}");
	assert_eq!(row_count(&t, "FROM s::m"), 0);
}

#[test]
fn tagged_series_insert_with_a_bare_tag_carrying_a_field_is_column_not_found() {
	// The unqualified form must refuse the field the same way, never store Active without it.
	let t = engine();

	let err = command_err(
		&t,
		"INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: Active { x: 1 } }]",
	);

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "x", "got: {err:?}");
	assert_eq!(row_count(&t, "FROM s::m"), 0);
}
