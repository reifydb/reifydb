// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::params;

#[test]
fn test_type_coercion_int_to_larger_int() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::coerce { val: int8 }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::coerce").row(params! { val: 42i32 }).row(params! { val: -100i32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = t.query("FROM test::coerce");
	assert_eq!(TestEngine::row_count(&frames), 2);

	// The negative value catches a widening that zero-extends instead of sign-extending.
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i64>("val").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![-100i64, 42i64]);
}

#[test]
fn test_type_coercion_int_to_float() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::coerce { val: float8 }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::coerce").row(params! { val: 42i32 }).row(params! { val: -100i32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = t.query("FROM test::coerce");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<f64>("val").unwrap().unwrap()).collect();
	values.sort_by(|a, b| a.partial_cmp(b).unwrap());
	assert_eq!(values, vec![-100.0f64, 42.0f64]);
}

#[test]
fn test_missing_column_uses_undefined() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	// b is Option so an omitted column is none rather than a constraint violation.
	t.admin("CREATE TABLE test::partial { a: int4, b: Option(int4) }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::partial").row(params! { a: 1 }).row(params! { a: 2 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = t.query("FROM test::partial");
	assert_eq!(TestEngine::row_count(&frames), 2);

	// An omitted column must read back as none, not as a zero value.
	for row in frames[0].rows() {
		let a = row.get::<i32>("a").unwrap();
		assert!(a.is_some());
		let b = row.get::<i32>("b").unwrap();
		assert!(b.is_none(), "Expected b to be none");
	}
}

#[test]
fn test_mixed_some_none_values() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::mixed { a: Option(int4), b: Option(int4) }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::mixed").row(params! { a: 1, b: 10 }).row(params! { a: 2 }).row(params! { b: 30 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 3);

	let frames = t.query("FROM test::mixed");
	assert_eq!(TestEngine::row_count(&frames), 3);
}

#[test]
fn test_coercion_batch_multiple_rows() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::batch { val: int8 }");

	// 100 rows so the batched coercion path is exercised, not the single-row one.
	let rows: Vec<_> = (1..=100).map(|n| params! { val: n as i32 }).collect();

	let mut builder = t.bulk_insert(identity);
	builder.table("test::batch").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 100);

	let frames = t.query("FROM test::batch");
	assert_eq!(TestEngine::row_count(&frames), 100);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i64>("val").unwrap().unwrap()).collect();
	values.sort();
	let expected: Vec<i64> = (1..=100).collect();
	assert_eq!(values, expected);
}

#[test]
fn test_coercion_float4_to_float8() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::floats { val: float8 }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::floats").row(params! { val: 3.14f32 }).row(params! { val: 2.71f32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = t.query("FROM test::floats");
	assert_eq!(TestEngine::row_count(&frames), 2);

	// f32 -> f64 is not exact, so the comparison has to be tolerant.
	let values: Vec<_> = frames[0].rows().map(|r| r.get::<f64>("val").unwrap().unwrap()).collect();
	assert_eq!(values.len(), 2);
	assert!(values.iter().any(|&v| (v - 3.14).abs() < 0.001));
	assert!(values.iter().any(|&v| (v - 2.71).abs() < 0.001));
}
