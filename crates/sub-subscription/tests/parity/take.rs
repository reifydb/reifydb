// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::{
	Row, announced_removes, normalize, random_rows, run_path_hydrate_then_live, run_path_incremental,
	run_path_snapshot,
};

// `take N` keeps the most recent N rows by arrival, evicting the oldest in-window arrival once full. The
// bulk-hydrate and incremental ingest paths must converge on the same final sink state.

#[test]
fn smoke_empty_log_take() {
	let a = normalize(run_path_snapshot("from app::t | take 5", &[]));
	let b = normalize(run_path_incremental("from app::t | take 5", &[]));
	assert_eq!(a, b);
	assert!(a.is_empty(), "empty input should produce empty sink output, got {:?}", a);
}

#[test]
fn take_emits_newest_n_rows() {
	// Monotonic insert order makes arrival order match RowNumber order, so the first-inserted of the six
	// rows is the one evicted.
	let rql = "from app::t | take 5";
	let rows = vec![
		Row {
			id: 279,
			qty: 858,
			ts_ms: 659581,
		},
		Row {
			id: 45,
			qty: 766,
			ts_ms: 698929,
		},
		Row {
			id: 611,
			qty: 95,
			ts_ms: 790287,
		},
		Row {
			id: 127,
			qty: 640,
			ts_ms: 153587,
		},
		Row {
			id: 812,
			qty: 208,
			ts_ms: 918440,
		},
		Row {
			id: 20,
			qty: 691,
			ts_ms: 55354,
		},
	];
	let expected =
		vec![(20, 691, 55354), (45, 766, 698929), (127, 640, 153587), (611, 95, 790287), (812, 208, 918440)];

	assert_eq!(
		normalize(run_path_snapshot(rql, &rows)),
		expected,
		"snapshot path must keep the 5 most recent rows by arrival"
	);
	assert_eq!(
		normalize(run_path_incremental(rql, &rows)),
		expected,
		"incremental path must keep the 5 most recent rows by arrival"
	);
}

fn seq_rows(ids: std::ops::RangeInclusive<i32>) -> Vec<Row> {
	// id doubles as arrival rank, so an assertion on ids is an assertion on age.
	ids.map(|id| Row {
		id,
		qty: id * 10,
		ts_ms: id as i64,
	})
	.collect()
}

#[test]
fn take_after_hydration_keeps_the_newest_rows() {
	// Hydrating six rows into a take-5 window holds 2..6, so the arrival of 7 must drop 2 and never 6.
	let rql = "from app::t | take 5";
	let batches = run_path_hydrate_then_live(rql, &seq_rows(1..=6), &seq_rows(7..=7));

	assert_eq!(
		normalize(batches),
		vec![(3, 30, 3), (4, 40, 4), (5, 50, 5), (6, 60, 6), (7, 70, 7)],
		"take must evict its oldest arrival once a live row lands on a hydrated window"
	);
}

#[test]
fn take_after_hydration_evicts_oldest_first_not_newest_first() {
	// Retiring 6, 5, 4 instead of 2, 3, 4 is a cursor walking backwards that holes the tail and pins the head.
	let rql = "from app::t | take 5";
	let batches = run_path_hydrate_then_live(rql, &seq_rows(1..=6), &seq_rows(7..=9));

	assert_eq!(
		announced_removes(&batches),
		vec![2, 3, 4],
		"every eviction after hydration must announce the oldest surviving row, never the newest"
	);
	assert_eq!(
		normalize(batches),
		vec![(5, 50, 5), (6, 60, 6), (7, 70, 7), (8, 80, 8), (9, 90, 9)],
		"the window must end on the five newest rows"
	);
}

#[test]
fn take_parity() {
	let rql = "from app::t | take 5";
	for case in 0..16 {
		let seed: u64 = 1000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 1_000);
		let a = normalize(run_path_snapshot(rql, &rows));
		let b = normalize(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"TAKE parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}
