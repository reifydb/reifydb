// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::create_subscription_error;

#[test]
fn inner_lookup_rejected_in_subscription() {
	// A lookup needs the read lease and the producer tracker that only a deferred view's flow has.
	let diag = create_subscription_error(
		"from app::t inner lookup { from app::other } as o using (id, o.id) with { retention: { left: 10s } }",
	);
	assert_eq!(diag.code, "LOOKUP_004", "expected LOOKUP_004, got {:?}: {}", diag.code, diag.message);
}

#[test]
fn left_lookup_rejected_in_subscription() {
	// The left form reads the right side the same way, so it must be refused the same way.
	let diag = create_subscription_error(
		"from app::t left lookup { from app::other } as o using (id, o.id) with { retention: { left: 10s } }",
	);
	assert_eq!(diag.code, "LOOKUP_004", "expected LOOKUP_004, got {:?}: {}", diag.code, diag.message);
}

#[test]
fn lookup_below_a_map_rejected_in_subscription() {
	// The check must reach a lookup that is not the outermost operator, or a map on top would hide it.
	let diag = create_subscription_error(
		"from app::t inner lookup { from app::other } as o using (id, o.id) with { retention: { left: 10s } } map { id, o_qty }",
	);
	assert_eq!(diag.code, "LOOKUP_004", "expected LOOKUP_004, got {:?}: {}", diag.code, diag.message);
}
