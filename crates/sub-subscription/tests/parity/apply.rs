// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::create_subscription_error;

#[test]
fn apply_rejected_in_subscription() {
	// A subscription never runs the create-time operator checks, so it must refuse a managed apply outright.
	let diag = create_subscription_error("from app::t | apply counter {} with { lateness: 1s }");
	assert_eq!(diag.code, "SUBS_004", "expected SUBS_004, got {:?}: {}", diag.code, diag.message);
	assert!(diag.message.contains("apply"), "diagnostic should name the offending operator: {}", diag.message);
}
