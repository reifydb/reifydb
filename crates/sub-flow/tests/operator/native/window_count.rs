// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A keyed sliding-window count must accumulate across applies within the same
// window and reset for a new window. Pins the exact emitted (window_start,
// count) per apply, so a backend that loses window state across applies fails.

use reifydb_abi::flow::diff::DiffType;

use super::Harness;
use crate::common::{ParityWindow, diff_kind, row_ints, window_change};

#[test]
fn window_counts_accumulate_across_applies() {
	let mut harness = Harness::<ParityWindow>::builder().build().expect("harness build");

	for (row_number, timestamp) in [(1u64, 10i64), (2, 20), (3, 150), (4, 30)] {
		harness.apply(window_change(row_number, timestamp)).expect("apply");
	}

	assert_eq!(row_ints(&harness[0]), vec![0, 1], "ts 10 -> window 0, count 1");
	assert_eq!(row_ints(&harness[1]), vec![0, 2], "ts 20 -> window 0, count 2");
	assert_eq!(row_ints(&harness[2]), vec![100, 1], "ts 150 -> window 100, count 1");
	assert_eq!(row_ints(&harness[3]), vec![0, 3], "ts 30 -> window 0, count 3");

	for i in 0..4 {
		assert_eq!(diff_kind(&harness[i]), DiffType::Insert, "every emission is an Insert");
	}
}
