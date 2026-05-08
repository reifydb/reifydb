// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::common::{normalize, random_rows, run_path_incremental, run_path_snapshot};

#[test]
fn smoke_empty_log_take() {
	let a = normalize(run_path_snapshot("from app::t | take 5", &[]));
	let b = normalize(run_path_incremental("from app::t | take 5", &[]));
	assert_eq!(a, b);
	assert!(a.is_empty(), "empty input should produce empty sink output, got {:?}", a);
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
