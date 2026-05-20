// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::common::{normalize_aggregated, random_rows, run_path_incremental, run_path_snapshot};

#[test]
fn map_parity() {
	let rql = "from app::t | map { id, qty, doubled: qty * 2 }";
	for case in 0..16 {
		let seed: u64 = 8000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 1_000);
		let a = normalize_aggregated(run_path_snapshot(rql, &rows));
		let b = normalize_aggregated(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"MAP parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}
