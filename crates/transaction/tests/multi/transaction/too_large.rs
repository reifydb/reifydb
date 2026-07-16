// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::test_multi;
use crate::{as_key, as_values};

#[test]
fn wide_rows_trip_too_large_via_modify() {
	// PendingWrites::estimate_size measures real row bytes (not a constant), so a
	// handful of multi-MiB rows trips the 1 GiB byte cap in ~512 entries, far below
	// the 1M-entry count cap - see pending.rs's own
	// wide_rows_reach_the_byte_cap_before_the_entry_cap test for the same math.
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();

	let big_value = "x".repeat(2 * 1024 * 1024);
	let mut result = Ok(());
	for i in 0..700u64 {
		result = txn.set(&as_key!(i), as_values!(big_value.clone()));
		if result.is_err() {
			break;
		}
	}

	let err = result.unwrap_err();
	assert_eq!(err.0.code, "TXN_003", "expected the TooLarge diagnostic, got: {err}");
	assert!(
		err.0.message.contains("too many writes") && err.0.message.contains("exceeds size limits"),
		"unexpected message: {}",
		err.0.message
	);
}
