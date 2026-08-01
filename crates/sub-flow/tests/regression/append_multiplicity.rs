// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// APPEND is a bag union, so a transactional view and a deferred view built on the same definition
// and fed the same DML must hold the same multiset. The divergence needs a deferred upstream
// feeding a transactional APPEND; an all-transactional chain or a single-level union does not.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().expect("build memory db with flow"))
}

#[test]
fn deferred_append_self_union_of_append_view_matches_transactional() {
	let db = setup();
	db.admin("CREATE NAMESPACE v");
	db.admin("CREATE NAMESPACE t");
	db.admin("CREATE NAMESPACE g");
	db.admin("CREATE TABLE v::base { id: int4 }");

	db.admin("CREATE TRANSACTIONAL VIEW t::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	db.admin("CREATE TRANSACTIONAL VIEW t::n2 { id: int4 } AS { FROM v::base APPEND { FROM t::n0 } MAP { id } }");
	db.admin("CREATE TRANSACTIONAL VIEW t::n3 { id: int4 } AS { FROM t::n2 APPEND { FROM t::n2 } MAP { id } }");

	db.admin("CREATE DEFERRED VIEW g::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	db.admin("CREATE TRANSACTIONAL VIEW g::n2 { id: int4 } AS { FROM v::base APPEND { FROM g::n0 } MAP { id } }");
	db.admin("CREATE DEFERRED VIEW g::n3 { id: int4 } AS { FROM g::n2 APPEND { FROM g::n2 } MAP { id } }");

	db.command("INSERT v::base [{ id: 1 }]");
	db.command("INSERT v::base [{ id: 2 }]");
	db.command("INSERT v::base [{ id: 3 }]");

	let oracle = db.row_count("FROM t::n3");
	assert_eq!(
		oracle, 12,
		"transactional twin: n3 = n2 APPEND n2 = 4x base = 12 rows for 3 base rows; got {oracle} \
		 (a change here means APPEND bag-union semantics moved and the test must be re-derived)"
	);

	let deferred = db.await_row_count("FROM g::n3", oracle, StdDuration::from_secs(10));
	assert_eq!(
		deferred, oracle,
		"mixed-kind nested APPEND graph (deferred n0 -> transactional n2 -> deferred self-union n3) must \
		 hold the same multiset as its all-transactional twin ({oracle} rows); got {deferred} -> APPEND \
		 multiplicity is lost across the transactional/deferred flow paths"
	);
}

#[test]
fn deferred_self_union_of_map_view_matches_transactional() {
	let db = setup();
	db.admin("CREATE NAMESPACE w");
	db.admin("CREATE NAMESPACE tw");
	db.admin("CREATE NAMESPACE gw");
	db.admin("CREATE TABLE w::base { id: int4 }");

	db.admin("CREATE TRANSACTIONAL VIEW tw::m { id: int4 } AS { FROM w::base MAP { id } }");
	db.admin("CREATE TRANSACTIONAL VIEW tw::u { id: int4 } AS { FROM tw::m APPEND { FROM tw::m } MAP { id } }");

	db.admin("CREATE TRANSACTIONAL VIEW gw::m { id: int4 } AS { FROM w::base MAP { id } }");
	db.admin("CREATE DEFERRED VIEW gw::u { id: int4 } AS { FROM gw::m APPEND { FROM gw::m } MAP { id } }");

	db.command("INSERT w::base [{ id: 1 }]");
	db.command("INSERT w::base [{ id: 2 }]");
	db.command("INSERT w::base [{ id: 3 }]");

	let oracle = db.row_count("FROM tw::u");
	assert_eq!(oracle, 6, "transactional twin: u = m APPEND m = 2x base = 6 rows; got {oracle}");

	let deferred = db.await_row_count("FROM gw::u", oracle, StdDuration::from_secs(10));
	assert_eq!(
		deferred, oracle,
		"single-level deferred self-union must match the transactional twin ({oracle} rows); got \
		 {deferred} (control: isolates the multiplicity defect to the NESTED-union case above)"
	);
}
