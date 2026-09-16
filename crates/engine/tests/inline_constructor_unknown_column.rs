// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_constructor_with_an_unknown_field_in_an_unknown_column_blames_the_column() {
	// The unknown column must be reported before any field of its constructor is checked.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Point }");
	t.admin("CREATE TABLE s::t { a: int4 }");

	for rql in [
		"INSERT s::t [{ a: 1, zzz: s::status::Active { x: 1 } }]",
		"INSERT s::t [{ a: 1, zzz: s::shape::Circle { nope: 1.0 } }]",
	] {
		let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);

		let err =
			r.error.unwrap_or_else(|| panic!("{rql}: expected an error, got {:?}", r.frames)).diagnostic();
		assert_eq!(err.code, "QUERY_001", "{rql}: {err:?}");
		assert_eq!(err.fragment.text(), "zzz", "{rql}: {err:?}");
		assert_eq!(TestEngine::row_count(&t.query("FROM s::t")), 0, "{rql}");
	}
}
