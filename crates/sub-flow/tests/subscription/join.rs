// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::common::create_subscription_error;

#[test]
fn join_rejected_in_subscription() {
	let diag = create_subscription_error("from app::t | left join { from app::other }  as o using (id,  o.id)");
	assert_eq!(diag.code, "SUBS_004", "expected SUBS_004, got {:?}: {}", diag.code, diag.message);
	assert!(diag.message.contains("join"), "diagnostic should name the offending operator: {}", diag.message);
}
