// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Reproducer: a mixed transactional/deferred graph of nested APPEND (union) views does not hold the
// same multiset as the equivalent all-transactional graph. APPEND is a bag union, so `n2 APPEND n2`
// must contain every row of `n2` twice; with `n2 = base APPEND n0` (and `n0` mirroring `base`), the
// four-fold union `n3` must be 4x base on BOTH paths. It is not: the mixed graph under-counts.
//
// WHY this matters: a transactional view and a deferred view built on the same definition, fed the
// same DML, must converge to the same multiset. Here they do not, so APPEND multiplicity is not
// preserved across the flow paths.
//
// Surfaced by the testbed `graph_chaos` scenario (memory config, seed 4043421078586853437):
//   n0=MAP(base)[D]; n2=APPEND(base,n0)[T]; n3=APPEND(n2,n2)[D]
// This pins the minimal, insert-only shape of that case: it must FAIL until the bug is fixed, then pass.
//
// Two narrowing observations baked into the tests below:
//   * The defect needs the DEFERRED upstream `n0` feeding the transactional APPEND `n2`. Making the whole `n0`/`n2`
//     chain transactional (only `n3` deferred) makes the divergence vanish - so it is the
//     deferred-feeds-transactional-APPEND interaction, not the deferred self-union by itself.
//   * A single-level deferred self-union (`m APPEND m`, m = MAP(base)) does NOT diverge - the control test stays green
//     - which isolates the defect to the nested case with a deferred upstream.
// Root-causing the exact mechanism is the follow-up fix, not this change.

use std::{
	thread,
	time::{Duration, Instant},
};

use reifydb::{Database, Params, WithSubsystem, embedded};

fn setup() -> Database {
	embedded::memory().with_flow(|c| c).build().expect("build memory db with flow")
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn command(db: &Database, rql: &str) {
	db.command_as_root(rql, Params::None).unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"));
}

fn row_count(db: &Database, rql: &str) -> usize {
	let frames = db.query_as_root(rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	frames.iter().map(|f| f.row_count()).sum()
}

// Poll a deferred view until it holds `want` rows or the deadline passes, then return whatever it last
// held so the caller's assertion reports the actual (possibly halved) count rather than hanging.
fn await_row_count(db: &Database, rql: &str, want: usize) -> usize {
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		let got = row_count(db, rql);
		if got >= want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(Duration::from_millis(20));
	}
}

// Temporary diagnostic (keep until the APPEND multiplicity fix is confirmed): dumps every
// intermediate view's id multiset so the divergence can be localized to g::n2 vs g::n3.
fn dump(db: &Database, ns: &str, name: &str) {
	let rql = format!("FROM {ns}::{name}");
	let frames = db.query_as_root(&rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	let mut ids: Vec<i64> = Vec::new();
	for f in &frames {
		for row in f.to_rows() {
			for (col, val) in row {
				if col == "id" {
					if let reifydb::Value::Int4(v) = val {
						ids.push(v as i64);
					}
				}
			}
		}
	}
	ids.sort_unstable();
	eprintln!("  {ns}::{name} count={} ids={ids:?}", ids.len());
}

#[test]
fn dbg_dump_intermediate_views() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE v");
	admin(&db, "CREATE NAMESPACE t");
	admin(&db, "CREATE NAMESPACE g");
	admin(&db, "CREATE TABLE v::base { id: int4 }");

	admin(&db, "CREATE TRANSACTIONAL VIEW t::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW t::n2 { id: int4 } AS { FROM v::base APPEND { FROM t::n0 } MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW t::n3 { id: int4 } AS { FROM t::n2 APPEND { FROM t::n2 } MAP { id } }");

	admin(&db, "CREATE DEFERRED VIEW g::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW g::n2 { id: int4 } AS { FROM v::base APPEND { FROM g::n0 } MAP { id } }");
	admin(&db, "CREATE DEFERRED VIEW g::n3 { id: int4 } AS { FROM g::n2 APPEND { FROM g::n2 } MAP { id } }");

	command(&db, "INSERT v::base [{ id: 1 }]");
	command(&db, "INSERT v::base [{ id: 2 }]");
	command(&db, "INSERT v::base [{ id: 3 }]");

	let _ = await_row_count(&db, "FROM g::n3", 12);
	thread::sleep(Duration::from_millis(500));

	eprintln!("=== TWIN (all transactional) ===");
	dump(&db, "t", "n0");
	dump(&db, "t", "n2");
	dump(&db, "t", "n3");
	eprintln!("=== MIXED (g::n0 deferred, g::n2 transactional, g::n3 deferred) ===");
	dump(&db, "g", "n0");
	dump(&db, "g", "n2");
	dump(&db, "g", "n3");
}

#[test]
fn deferred_append_self_union_of_append_view_matches_transactional() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE v");
	admin(&db, "CREATE NAMESPACE t");
	admin(&db, "CREATE NAMESPACE g");
	admin(&db, "CREATE TABLE v::base { id: int4 }");

	admin(&db, "CREATE TRANSACTIONAL VIEW t::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW t::n2 { id: int4 } AS { FROM v::base APPEND { FROM t::n0 } MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW t::n3 { id: int4 } AS { FROM t::n2 APPEND { FROM t::n2 } MAP { id } }");

	admin(&db, "CREATE DEFERRED VIEW g::n0 { id: int4 } AS { FROM v::base MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW g::n2 { id: int4 } AS { FROM v::base APPEND { FROM g::n0 } MAP { id } }");
	admin(&db, "CREATE DEFERRED VIEW g::n3 { id: int4 } AS { FROM g::n2 APPEND { FROM g::n2 } MAP { id } }");

	command(&db, "INSERT v::base [{ id: 1 }]");
	command(&db, "INSERT v::base [{ id: 2 }]");
	command(&db, "INSERT v::base [{ id: 3 }]");

	let oracle = row_count(&db, "FROM t::n3");
	assert_eq!(
		oracle, 12,
		"transactional twin: n3 = n2 APPEND n2 = 4x base = 12 rows for 3 base rows; got {oracle} \
		 (a change here means APPEND bag-union semantics moved and the test must be re-derived)"
	);

	let deferred = await_row_count(&db, "FROM g::n3", oracle);
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
	admin(&db, "CREATE NAMESPACE w");
	admin(&db, "CREATE NAMESPACE tw");
	admin(&db, "CREATE NAMESPACE gw");
	admin(&db, "CREATE TABLE w::base { id: int4 }");

	admin(&db, "CREATE TRANSACTIONAL VIEW tw::m { id: int4 } AS { FROM w::base MAP { id } }");
	admin(&db, "CREATE TRANSACTIONAL VIEW tw::u { id: int4 } AS { FROM tw::m APPEND { FROM tw::m } MAP { id } }");

	admin(&db, "CREATE TRANSACTIONAL VIEW gw::m { id: int4 } AS { FROM w::base MAP { id } }");
	admin(&db, "CREATE DEFERRED VIEW gw::u { id: int4 } AS { FROM gw::m APPEND { FROM gw::m } MAP { id } }");

	command(&db, "INSERT w::base [{ id: 1 }]");
	command(&db, "INSERT w::base [{ id: 2 }]");
	command(&db, "INSERT w::base [{ id: 3 }]");

	let oracle = row_count(&db, "FROM tw::u");
	assert_eq!(oracle, 6, "transactional twin: u = m APPEND m = 2x base = 6 rows; got {oracle}");

	let deferred = await_row_count(&db, "FROM gw::u", oracle);
	assert_eq!(
		deferred, oracle,
		"single-level deferred self-union must match the transactional twin ({oracle} rows); got \
		 {deferred} (control: isolates the multiplicity defect to the NESTED-union case above)"
	);
}
