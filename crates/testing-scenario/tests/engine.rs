// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{SeedableRng, rngs::StdRng};
use reifydb_test_harness::db::TestDb;
use reifydb_testing_scenario::{
	query::OperationKind,
	registry::{all, by_name},
	scenario::Scenario,
	scenarios::{
		join::ORDERS_PER_CUSTOMER,
		scan::{FULL_SCAN_MATCHES, WINDOW},
	},
};

const SCALE: u64 = 200;
const SEED: u64 = 20260803;

fn apply_setup(db: &TestDb, scenario: &Scenario, scale: u64) {
	for statement in scenario.setup_statements(scale) {
		let outcome = match statement.kind {
			OperationKind::Admin => db.try_admin(&statement.rql),
			OperationKind::Command => db.try_command(&statement.rql),
			OperationKind::Query => db.try_query(&statement.rql),
		};

		outcome.unwrap_or_else(|e| {
			panic!("scenario '{}' setup rejected `{}`: {}", scenario.name, statement.rql, e)
		});
	}
}

#[test]
fn every_scenario_setup_and_query_is_accepted_by_the_engine() {
	// The unit tests only prove the crate assembles strings. Nothing there can tell a valid
	// statement from a plausible-looking one, so a typo in any DDL, INSERT or query template
	// would ship green and only surface as a failed load run hours later.
	for scenario in all() {
		let db = TestDb::memory();
		apply_setup(&db, &scenario, SCALE);

		let mut rng = StdRng::seed_from_u64(SEED);
		for query in &scenario.queries {
			for sequence in 0..3 {
				let rql = query.rql.render(&mut rng, SCALE, SCALE + sequence);
				let outcome = match query.kind {
					OperationKind::Query => db.try_query(&rql),
					OperationKind::Command => db.try_command(&rql),
					OperationKind::Admin => db.try_admin(&rql),
				};

				outcome.unwrap_or_else(|e| {
					panic!(
						"scenario '{}' query '{}' rejected `{}`: {}",
						scenario.name, query.name, rql, e
					)
				});
			}
		}

		for statement in scenario.teardown_statements() {
			db.try_admin(&statement.rql).unwrap_or_else(|e| {
				panic!("scenario '{}' teardown rejected `{}`: {}", scenario.name, statement.rql, e)
			});
		}
	}
}

#[test]
fn generated_setup_lands_the_row_count_the_scale_promises() {
	// A profile labelled 100k that seeded a fraction of that would report throughput against a
	// table small enough to sit in cache, which is the most flattering possible lie.
	let scenario = by_name("read").expect("read scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	assert_eq!(db.row_count("from bench::users"), SCALE as usize);
}

#[test]
fn join_setup_fans_orders_out_across_customers() {
	// The join scenario is only measuring join work if orders actually outnumber customers and
	// every order resolves to a real customer.
	let scenario = by_name("join").expect("join scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	assert_eq!(db.row_count("from bench::customers"), SCALE as usize);
	assert_eq!(db.row_count("from bench::orders"), (SCALE * 3) as usize);

	// Every order must reference a customer that was actually seeded, otherwise the left join
	// would degenerate into unmatched rows and stop measuring join work.
	assert_eq!(db.row_count(&format!("from bench::orders filter customer_id >= {}", SCALE)), 0);

	// Each rendered join must return exactly the fan-out, which catches both a join that drops
	// matches and one that duplicates them.
	let query = scenario.query("left_join").expect("join scenario defines left_join");
	let mut rng = StdRng::seed_from_u64(SEED);
	for _ in 0..10 {
		let rql = query.rql.render(&mut rng, SCALE, 0);
		assert_eq!(db.row_count(&rql), 3, "`{}` did not return the three-order fan-out", rql);
	}
}

#[test]
fn point_lookups_actually_find_seeded_rows() {
	// A lookup template drawing ids outside the seeded range would return nothing and measure
	// the cost of a miss while claiming to measure a lookup.
	let scenario = by_name("read").expect("read scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	let query = scenario.query("point_lookup").expect("read scenario defines point_lookup");
	let mut rng = StdRng::seed_from_u64(SEED);

	for _ in 0..25 {
		let rql = query.rql.render(&mut rng, SCALE, 0);
		assert_eq!(db.row_count(&rql), 1, "`{}` matched no seeded row", rql);
	}
}

#[test]
fn range_scans_return_the_window_they_ask_for() {
	// `take 100` against a 200 row table must come back full, otherwise the scan scenario is
	// quietly benchmarking a much smaller result set than its name suggests.
	let scenario = by_name("scan").expect("scan scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	let query = scenario.query("range_scan").expect("scan scenario defines range_scan");
	let mut rng = StdRng::seed_from_u64(SEED);

	for _ in 0..10 {
		let rql = query.rql.render(&mut rng, SCALE, 0);
		assert_eq!(db.row_count(&rql), 100, "`{}` returned a short window", rql);
	}
}

#[test]
fn full_scan_asks_for_more_rows_than_the_filter_can_ever_match() {
	// This is the whole mechanism of the worst-case scan. The filter matches FULL_SCAN_MATCHES
	// rows while the take asks for WINDOW, so take can never fill and the pipeline is forced to
	// exhaust the table before it can conclude there is nothing left. The sibling range_scan
	// query looks like a scan but finishes in ~61us at any scale because the table scan yields
	// rows newest-first and its `id > start` filter is satisfied immediately - take fills from
	// the newest rows and the scan stops. Should FULL_SCAN_MATCHES ever reach WINDOW, take would
	// short-circuit the same way and this scenario would quietly stop being a full scan while
	// still reporting under the same name.
	assert!(FULL_SCAN_MATCHES < WINDOW, "take must be unfillable or the scan short-circuits");

	let scenario = by_name("scan").expect("scan scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	let query = scenario.query("full_scan").expect("scan scenario defines full_scan");
	let mut rng = StdRng::seed_from_u64(SEED);
	let rql = query.rql.render(&mut rng, SCALE, 0);

	assert_eq!(db.row_count(&rql), FULL_SCAN_MATCHES as usize, "`{}` did not match the oldest rows", rql);
}

#[test]
fn full_join_probes_every_seeded_order_against_every_customer() {
	// The worst-case join drops the filter so the hash join builds over every customer and probes
	// with every order, rather than probing with the three orders a single customer_id selects.
	// Anything other than the full order count means the query is not the worst case it claims to
	// be: fewer rows would mean something still narrows the probe side, more would mean the join
	// is duplicating matches.
	let scenario = by_name("join").expect("join scenario is registered");
	let db = TestDb::memory();
	apply_setup(&db, &scenario, SCALE);

	let query = scenario.query("full_join").expect("join scenario defines full_join");
	let mut rng = StdRng::seed_from_u64(SEED);
	let rql = query.rql.render(&mut rng, SCALE, 0);

	assert_eq!(
		db.row_count(&rql),
		(SCALE * ORDERS_PER_CUSTOMER) as usize,
		"`{}` did not probe every seeded order",
		rql
	);
}
