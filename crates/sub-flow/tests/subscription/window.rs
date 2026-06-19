// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::create_subscription_error;

#[test]
fn window_rejected_in_subscription() {
	let diag = create_subscription_error(
		"from app::t | window tumbling { math::sum(qty) } with { interval: \"100ms\" }",
	);
	assert_eq!(diag.code, "SUBS_004", "expected SUBS_004, got {:?}: {}", diag.code, diag.message);
	assert!(diag.message.contains("window"), "diagnostic should name the offending operator: {}", diag.message);
}
