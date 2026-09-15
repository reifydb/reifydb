// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE TABLE s::empty { a: int4 }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.command("INSERT s::t [{ a: 1 }]");
	t.command("INSERT s::e [{ id: 1, status: Active }]");
	t
}

fn query_err(t: &TestEngine, rql: &str) -> Result<Diagnostic, String> {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Ok(e.diagnostic()),
		None => Err(format!("no error, got frames {:?}", r.frames)),
	}
}

#[test]
fn a_duplicate_extend_column_error_points_at_the_later_name_as_written() {
	// Without the fragment the error cannot show which extend field clashes, like QUERY_009 does for a map.
	let t = engine();
	let mut failures = Vec::new();

	for (rql, name) in [
		("FROM s::t | extend { a: 2 }", "a"),
		("FROM s::empty | extend { a: 2 }", "a"),
		("FROM s::t | extend { y: 1, y: 2 }", "y"),
		("FROM s::empty | extend { y: 1, y: 2 }", "y"),
		("extend { y: 1, y: 2 }", "y"),
		("FROM s::e | extend { status_tag: 2 }", "status_tag"),
		("FROM s::e | extend { status: s::status::Active }", "status"),
	] {
		let expected_column = rql.rfind(&format!("{name}: ")).expect("name in query") + 1;
		match query_err(&t, rql) {
			Err(got) => failures.push(format!("{rql}: {got}")),
			Ok(err) => {
				let got = (
					err.code.as_str(),
					err.fragment.text(),
					*err.fragment.line() as usize,
					*err.fragment.column() as usize,
				);
				if got != ("EXTEND_002", name, 1, expected_column) {
					failures.push(format!(
						"{rql}: expected EXTEND_002 at {name:?} line 1 column {expected_column}, got {got:?}"
					));
				}
			}
		}
	}

	assert!(failures.is_empty(), "every duplicate must point at its name as written, got {failures:#?}");
}
