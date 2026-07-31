// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::create_subscription_error;

#[test]
fn sort_rejected_in_subscription() {
	let diag = create_subscription_error("from app::t | sort {qty}");
	assert_eq!(diag.code, "SUBS_004", "expected SUBS_004, got {:?}: {}", diag.code, diag.message);
	assert!(diag.message.contains("sort"), "diagnostic should name the offending operator: {}", diag.message);
}
