// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::common::{Row, normalize, random_rows, run_path_incremental, run_path_snapshot};

// `take N` is a sliding window of the most recent N rows by arrival. Each new row is admitted;
// when the window is full, the oldest in-window arrival is evicted. Both bulk-hydrate (snapshot)
// and incremental (CDC) ingest paths must converge on the same final sink state.

#[test]
fn smoke_empty_log_take() {
	let a = normalize(run_path_snapshot("from app::t | take 5", &[]));
	let b = normalize(run_path_incremental("from app::t | take 5", &[]));
	assert_eq!(a, b);
	assert!(a.is_empty(), "empty input should produce empty sink output, got {:?}", a);
}

// 6 rows feed `take 5`. With monotonic insert order, arrival-order matches RowNumber order, so
// the first-inserted row is the oldest arrival and is the one evicted.
#[test]
fn take_emits_newest_n_rows() {
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
