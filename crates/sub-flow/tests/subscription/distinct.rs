// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::common::{normalize, random_rows, run_path_incremental, run_path_snapshot};

#[test]
fn distinct_parity() {
	// Small id-domain (1..=5) so duplicates are common; that is the whole point of distinct.
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
