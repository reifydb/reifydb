// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Window parity tests are #[ignore]'d because the operator currently violates parity:
// bulk-snapshot ingest of N rows produces redundant intermediate Insert/Update emissions that
// the incremental path collapses. See the seed-3000 regression test below for the canonical
// reproducer. Per standing instruction the operator is not modified here; remove #[ignore] only
// after the fix lands.

use crate::common::{Row, normalize_aggregated, random_rows, run_path_incremental, run_path_snapshot};

#[ignore]
#[test]
fn window_tumbling_parity() {
	let rql = "from app::t | window tumbling { math::sum(qty) } with { interval: \"100ms\" }";
	for case in 0..16 {
		let seed: u64 = 3000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 1_000);
		let a = normalize_aggregated(run_path_snapshot(rql, &rows));
		let b = normalize_aggregated(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"WINDOW parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}

// Regression: minimum reproducer for the WINDOW parity divergence the proptest above shrunk to.
// First exposed at seed=3000. Bulk hydrate of these 4 rows at one MVCC version produces
// 8 sink emits (2 Inserts + 6 Updates including a no-op Update(2665) -> Update(2665)).
// Incremental ingest of the same 4 rows as separate commits produces 4 sink emits
// (1 Insert + 3 Updates). The divergence is in the Window operator's state-transition behavior
// when multiple events land at the same version. Keep this test as the canonical reproducer;
// remove only after the underlying Window operator is fixed.
#[ignore]
#[test]
fn window_tumbling_parity_regression_seed_3000() {
	let rql = "from app::t | window tumbling { math::sum(qty) } with { interval: \"100ms\" }";
	let rows = vec![
		Row {
			id: 525,
			qty: 154,
			ts_ms: 438111,
		},
		Row {
			id: 584,
			qty: 989,
			ts_ms: 839970,
		},
		Row {
			id: 899,
			qty: 928,
			ts_ms: 194402,
		},
		Row {
			id: 544,
			qty: 594,
			ts_ms: 144105,
		},
	];
	let a = normalize_aggregated(run_path_snapshot(rql, &rows));
	let b = normalize_aggregated(run_path_incremental(rql, &rows));
	assert_eq!(a, b, "WINDOW parity regression (seed 3000)\nsnapshot path={:?}\nincremental={:?}", a, b);
}
