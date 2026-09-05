// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::test_multi;
use crate::{as_key, as_values};

#[test]
fn wide_rows_trip_too_large_via_modify() {
	// Size is measured from real row bytes, so 2 MiB rows hit the 1 GiB byte cap around 512 entries,
	// far short of the 1M-entry cap; a constant per-entry estimate would never trip it here.
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
