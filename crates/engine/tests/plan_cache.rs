// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

const READ: &str = "FROM test::items MAP { id }";

fn seeded() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8 }");
	t.command("INSERT test::items [{ id: 1 }, { id: 2 }]");
	t
}

fn rows_as(t: &TestEngine, identity: IdentityId) -> usize {
	let r = t.inner().query_as(identity, READ, Params::None);
	assert!(r.error.is_none(), "query failed: {:?}", r.error);
	r.frames.first().map(|f| f.row_count()).unwrap_or(0)
}

#[test]
fn a_privileged_plan_is_never_served_to_an_unprivileged_identity() {
	// A privileged compile skips policy injection entirely; an unprivileged one gets a deny
	// filter when no policy grants it. Caching on query text alone would hand the privileged
	// plan to the next caller of the same text, turning row-level security off for that query.
	let t = seeded();

	assert_eq!(rows_as(&t, IdentityId::system()), 2, "a privileged read must see every row");
	assert_eq!(
		rows_as(&t, IdentityId::anonymous()),
		0,
		"an unprivileged read of the same text must still be denied, not served the cached privileged plan"
	);
}

#[test]
fn an_unprivileged_plan_is_never_cached_for_a_privileged_identity() {
	// The leak runs both ways: a stored deny-filtered plan would silently empty a privileged
	// read of the same text.
	let t = seeded();

	assert_eq!(rows_as(&t, IdentityId::anonymous()), 0, "an unprivileged read is denied without a policy");
	assert_eq!(
		rows_as(&t, IdentityId::system()),
		2,
		"the privileged read must plan for itself, not inherit the denied plan"
	);
}

#[test]
fn a_plan_cached_before_ddl_is_not_reused_after_it() {
	// A cached plan binds to the catalog it was planned against, and nothing else invalidates
	// the cache. Reusing one across a rebuild of its own table would keep reading an object id
	// that no longer holds the rows.
	let t = seeded();
	assert_eq!(rows_as(&t, IdentityId::system()), 2, "the read must be cached against the first table");

	t.admin("DROP TABLE test::items");
	t.admin("CREATE TABLE test::items { id: int8 }");
	t.command("INSERT test::items [{ id: 7 }, { id: 8 }, { id: 9 }]");

	assert_eq!(
		rows_as(&t, IdentityId::system()),
		3,
		"the same query text must be replanned against the rebuilt table, not served from before the ddl"
	);
}
