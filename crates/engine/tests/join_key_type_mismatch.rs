// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame, identity::IdentityId, uuid::Uuid4, value_type::ValueType},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::l { k: int4, a: int4 }");
	t.admin("CREATE TABLE test::r { k: int8, b: int4 }");
	t.command("INSERT test::l [{ k: 1, a: 10 }, { k: 2, a: 20 }]");
	t.command("INSERT test::r [{ k: 1, b: 100 }, { k: 2, b: 200 }]");
	t
}

fn query(t: &TestEngine, rql: &str, params: Params) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn assert_names_both_types(err: &Diagnostic, left: ValueType, right: ValueType) {
	// The fix is a cast on one side, so the message must say which type sits on which side.
	assert!(
		err.message.contains(&left.to_string()) && err.message.contains(&right.to_string()),
		"the message must name {left} and {right}, got: {}",
		err.message
	);
}

#[test]
fn inner_join_on_int4_against_int8_reports_join_004() {
	// Equal numbers in different widths used to hash apart and silently return no rows.
	let t = engine();

	let err = query(&t, "FROM test::l INNER JOIN { FROM test::r } AS s USING (k, s.k)", Params::None).unwrap_err();

	assert_eq!(err.code, "JOIN_004", "got: {err:?}");
	assert_names_both_types(&err, ValueType::Int4, ValueType::Int8);
}

#[test]
fn left_join_on_int4_against_int8_reports_join_004() {
	// A left join keeps every probe row, so a mismatch would pass as a join where nothing matched.
	let t = engine();

	let err = query(&t, "FROM test::l LEFT JOIN { FROM test::r } AS s USING (k, s.k)", Params::None).unwrap_err();

	assert_eq!(err.code, "JOIN_004", "got: {err:?}");
}

#[test]
fn natural_join_sharing_a_column_of_different_types_reports_join_004() {
	// A natural join picks its keys by name, so a shared name with two types must fail, not match nothing.
	let t = engine();

	let err = query(&t, "FROM test::l NATURAL JOIN { FROM test::r } AS s", Params::None).unwrap_err();

	assert_eq!(err.code, "JOIN_004", "got: {err:?}");
	assert_names_both_types(&err, ValueType::Int4, ValueType::Int8);
}

#[test]
fn uuid4_against_uuid7_reports_join_004_even_though_their_bytes_line_up() {
	// Both are sixteen raw bytes in storage, so a check on the storage layout alone would let them match.
	let t = TestEngine::new();
	let params = Params::from(HashMap::from([
		("a".to_string(), Value::Uuid4(Uuid4::generate())),
		("b".to_string(), Value::Uuid7(IdentityId::root().value())),
	]));

	let err =
		query(&t, "from [{ k: $a }] INNER JOIN { from [{ k: $b }] } AS s USING (k, s.k)", params).unwrap_err();

	assert_eq!(err.code, "JOIN_004", "got: {err:?}");
	assert_names_both_types(&err, ValueType::Uuid4, ValueType::Uuid7);
}

#[test]
fn matching_key_types_still_join() {
	// The type check must only reject a real mismatch, never a join whose keys agree.
	let t = engine();
	t.admin("CREATE TABLE test::r4 { k: int4, b: int4 }");
	t.command("INSERT test::r4 [{ k: 1, b: 100 }, { k: 3, b: 300 }]");

	let frames = query(&t, "FROM test::l INNER JOIN { FROM test::r4 } AS s USING (k, s.k)", Params::None).unwrap();

	assert_eq!(TestEngine::row_count(&frames), 1, "only k = 1 is on both sides, got:\n{}", frames[0]);
}

#[test]
fn decimal_keys_of_different_precision_and_scale_join_by_value() {
	// Both keys are decimals, so a JOIN_004 or a byte compare on the scale would drop 1.5 = 1.500.
	let t = engine();
	t.admin("CREATE TABLE test::dl { k: decimal(5,1), a: int4 }");
	t.admin("CREATE TABLE test::dr { k: decimal(12,3), b: int4 }");
	t.command("INSERT test::dl [{ k: 1.5, a: 10 }, { k: 2.0, a: 20 }]");
	t.command("INSERT test::dr [{ k: 1.500, b: 100 }, { k: 2.001, b: 200 }]");

	let hash = query(&t, "FROM test::dl INNER JOIN { FROM test::dr } AS s USING (k, s.k)", Params::None).unwrap();
	let natural = query(&t, "FROM test::dl NATURAL JOIN { FROM test::dr } AS s", Params::None).unwrap();

	assert_eq!(TestEngine::row_count(&hash), 1, "only 1.5 = 1.500 must match, got:\n{}", hash[0]);
	assert_eq!(TestEngine::row_count(&natural), 1, "only 1.5 = 1.500 must match, got:\n{}", natural[0]);
}

#[test]
fn int_keys_of_different_precision_join_by_value() {
	// int(5) and int(30) are one family, so the wider precision must hold both sides instead of JOIN_004.
	let t = engine();
	t.admin("CREATE TABLE test::il { k: int(5), a: int4 }");
	t.admin("CREATE TABLE test::ir { k: int(30), b: int4 }");
	t.command("INSERT test::il [{ k: 7, a: 10 }, { k: 8, a: 20 }]");
	t.command("INSERT test::ir [{ k: 7, b: 100 }, { k: 123456789012345678901234567890, b: 200 }]");

	let frames = query(&t, "FROM test::il INNER JOIN { FROM test::ir } AS s USING (k, s.k)", Params::None).unwrap();

	assert_eq!(TestEngine::row_count(&frames), 1, "only k = 7 is on both sides, got:\n{}", frames[0]);
}

#[test]
fn a_decimal_key_against_an_int_key_still_reports_join_004() {
	// Only keys of one family share a common type, so decimal against int must stay a type mismatch.
	let t = engine();
	t.admin("CREATE TABLE test::xl { k: decimal(5,1), a: int4 }");
	t.admin("CREATE TABLE test::xr { k: int(5), b: int4 }");

	let err =
		query(&t, "FROM test::xl INNER JOIN { FROM test::xr } AS s USING (k, s.k)", Params::None).unwrap_err();

	assert_eq!(err.code, "JOIN_004", "decimal against int must be a key type mismatch, got: {}", err.message);
}
