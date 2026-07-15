// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Logical-plan validation tests for `CREATE SEGMENTTREE`. These exercise the full
// `admin_as_root` pipeline (parse -> logical plan -> physical plan), stopping short of
// engine execution, which is not part of this plan. Every case here is a compile-time
// rejection, so it never reaches the (not yet implemented) `Instruction::CreateSegmentTree`
// execution path.

use reifydb::{Params, RuntimeConfig, embedded as db_embedded};

fn admin_must_fail(rql: &str) -> String {
	let mut db = db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().unwrap();
	let err = db.admin_as_root(rql, Params::None).expect_err("expected CREATE SEGMENTTREE to be rejected");
	let message = err.to_string();
	db.stop().unwrap();
	message
}

#[test]
fn duplicate_aggregate_alias_is_rejected() {
	let msg = admin_must_fail(
		"CREATE SEGMENTTREE cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::sum(load), total: math::max(load) } }",
	);
	assert!(msg.contains("total"), "error should reference the duplicate alias 'total', got: {}", msg);
}

#[test]
fn unknown_aggregate_column_is_rejected() {
	let msg = admin_must_fail(
		"CREATE SEGMENTTREE cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::sum(missing) } }",
	);
	assert!(
		msg.contains("missing") && msg.contains("not declared"),
		"error should reference the unknown column 'missing', got: {}",
		msg
	);
}

#[test]
fn row_ttl_is_rejected() {
	let msg = admin_must_fail(
		"CREATE SEGMENTTREE cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::sum(load) }, row: { ttl: { duration: '1m', mode: drop } } }",
	);
	assert!(msg.contains("row ttl is not supported on segment tree"), "error should reject row ttl, got: {}", msg);
}

#[test]
fn precision_on_integer_key_is_rejected() {
	let msg = admin_must_fail(
		"CREATE SEGMENTTREE cpu { id: int4, load: float8 } WITH { key: id, precision: millisecond, aggregates: { total: math::sum(load) } }",
	);
	assert!(
		msg.contains("precision can only be specified for datetime key columns"),
		"error should reject precision on an integer key, got: {}",
		msg
	);
}
