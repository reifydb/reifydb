// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Validation mode tests for the bulk_insert module.
//!
//! Tests cover type coercion, constraint validation, and missing column handling.

use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::params;

use crate::{create_namespace, create_table, query_table, row_count, test_identity};

#[test]
fn test_type_coercion_int_to_larger_int() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	// int8 (i64) column, insert int4 (i32) values
	create_table(&engine, "test", "coerce", "val: int8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.coerce").row(params! { val: 42i32 }).row(params! { val: -100i32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test.coerce");
	assert_eq!(row_count(&frames), 2);

	// Verify values were coerced correctly
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i64>("val").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![-100i64, 42i64]);
}

#[test]
fn test_type_coercion_int_to_float() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	// float8 (f64) column, insert int4 (i32) values
	create_table(&engine, "test", "coerce", "val: float8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.coerce").row(params! { val: 42i32 }).row(params! { val: -100i32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test.coerce");
	assert_eq!(row_count(&frames), 2);

	// Verify values were coerced correctly
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<f64>("val").unwrap().unwrap()).collect();
	values.sort_by(|a, b| a.partial_cmp(b).unwrap());
	assert_eq!(values, vec![-100.0f64, 42.0f64]);
}

#[test]
fn test_missing_column_uses_undefined() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	// Two columns, but we only insert into one; b is Option to accept none
	create_table(&engine, "test", "partial", "a: int4, b: Option(int4)");

	let mut builder = engine.bulk_insert(&identity);
	builder
		.table("test.partial")
		.row(params! { a: 1 }) // missing b
		.row(params! { a: 2 }) // missing b
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test.partial");
	assert_eq!(row_count(&frames), 2);

	// Verify column 'a' has values and column 'b' is undefined (None)
	for row in frames[0].rows() {
		let a = row.get::<i32>("a").unwrap();
		assert!(a.is_some());
		let b = row.get::<i32>("b").unwrap();
		assert!(b.is_none(), "Expected b to be none");
	}
}

#[test]
fn test_mixed_some_none_values() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "mixed", "a: Option(int4), b: Option(int4)");

	let mut builder = engine.bulk_insert(&identity);
	builder
		.table("test.mixed")
		.row(params! { a: 1, b: 10 }) // both defined
		.row(params! { a: 2 }) // only a defined
		.row(params! { b: 30 }) // only b defined
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 3);

	let frames = query_table(&engine, "test.mixed");
	assert_eq!(row_count(&frames), 3);
}

#[test]
fn test_coercion_batch_multiple_rows() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	// int8 column, batch of int4 values
	create_table(&engine, "test", "batch", "val: int8");

	// Insert many rows to test batch coercion
	let rows: Vec<_> = (1..=100).map(|n| params! { val: n as i32 }).collect();

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.batch").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 100);

	let frames = query_table(&engine, "test.batch");
	assert_eq!(row_count(&frames), 100);

	// Verify all values were coerced correctly
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i64>("val").unwrap().unwrap()).collect();
	values.sort();
	let expected: Vec<i64> = (1..=100).collect();
	assert_eq!(values, expected);
}

#[test]
fn test_coercion_float4_to_float8() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	// float8 (f64) column, insert float4 (f32) values
	create_table(&engine, "test", "floats", "val: float8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.floats").row(params! { val: 3.14f32 }).row(params! { val: 2.71f32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test.floats");
	assert_eq!(row_count(&frames), 2);

	// Verify values were coerced (allowing for f32->f64 precision)
	let values: Vec<_> = frames[0].rows().map(|r| r.get::<f64>("val").unwrap().unwrap()).collect();
	assert_eq!(values.len(), 2);
	// Values should be close to original f32 values
	assert!(values.iter().any(|&v| (v - 3.14).abs() < 0.001));
	assert!(values.iter().any(|&v| (v - 2.71).abs() < 0.001));
}
