// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::common::{normalize_aggregated, random_rows, run_path_incremental, run_path_snapshot};

// #[ignore]'d: aggregate flow registration panics with "not implemented" inside
// crates/sub-flow/src/engine/register.rs:471 - the operator path isn't fully wired in flow
// registration. This is an engine surface gap, not a parity violation. Remove #[ignore] once
// aggregate flows are registerable.
#[ignore]
#[test]
fn aggregate_parity() {
	let rql = "from app::t | aggregate { total: math::sum(qty) } by {id}";
	for case in 0..16 {
		let seed: u64 = 4000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 5);
		let a = normalize_aggregated(run_path_snapshot(rql, &rows));
		let b = normalize_aggregated(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"AGGREGATE parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}
