// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::common::{Row, normalize, random_rows, run_path_incremental, run_path_snapshot};

// `distinct {key}` emits exactly one row per distinct key value. When several rows share a
// key, the operator preserves the FIRST row to arrive for that key; later duplicates are
// absorbed into the running count and never overwrite the emitted row. Bulk-hydrate
// (snapshot) and incremental (CDC) ingest paths must agree on this contract.
#[test]
fn distinct_emits_first_row_per_key() {
	let rql = "from app::t | distinct {id}";
	let rows = vec![
		Row {
			id: 3,
			qty: 320,
			ts_ms: 881420,
		},
		Row {
			id: 4,
			qty: 948,
			ts_ms: 821663,
		},
		Row {
			id: 4,
			qty: 351,
			ts_ms: 293762,
		},
	];
	let expected = vec![(3, 320, 881420), (4, 948, 821663)];

	assert_eq!(
		normalize(run_path_snapshot(rql, &rows)),
		expected,
		"snapshot path must emit the first row seen for each distinct key"
	);
	assert_eq!(
		normalize(run_path_incremental(rql, &rows)),
		expected,
		"incremental path must emit the first row seen for each distinct key"
	);
}

#[test]
fn distinct_parity() {
	let rql = "from app::t | distinct {id}";
	for case in 0..16 {
		let seed: u64 = 2000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 5);
		let a = normalize(run_path_snapshot(rql, &rows));
		let b = normalize(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"DISTINCT parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}
