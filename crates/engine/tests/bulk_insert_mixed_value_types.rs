// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use reifydb_engine::bulk_insert::builder::{BulkInsertBuilder, ValidationMode};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

const TARGETS: [&str; 3] = ["s::t", "s::r", "s::x"];

#[derive(Clone, Copy, Debug)]
enum Mode {
	Named,
	Positional,
	UncheckedNamed,
	UncheckedPositional,
}

const MODES: [Mode; 4] = [Mode::Named, Mode::Positional, Mode::UncheckedNamed, Mode::UncheckedPositional];

fn engine(column_type: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin(&format!("CREATE TABLE s::t {{ k: int8, d: {column_type} }}"));
	t.admin(&format!("CREATE RINGBUFFER s::r {{ k: int8, d: {column_type} }} WITH {{ capacity: 10 }}"));
	t.admin(&format!("CREATE SERIES s::x {{ k: int8, d: {column_type} }} WITH {{ key: k }}"));
	t
}

fn rows(mode: Mode, values: &[Value]) -> Vec<Params> {
	values.iter()
		.enumerate()
		.map(|(idx, d)| {
			let k = Value::Int8(idx as i64 + 1);
			match mode {
				Mode::Named | Mode::UncheckedNamed => Params::from(HashMap::from([
					("k".to_string(), k),
					("d".to_string(), d.clone()),
				])),
				Mode::Positional | Mode::UncheckedPositional => Params::from(vec![k, d.clone()]),
			}
		})
		.collect()
}

fn execute<V: ValidationMode>(
	mut builder: BulkInsertBuilder<'_, V>,
	target: &str,
	rows: Vec<Params>,
) -> Result<(), Diagnostic> {
	match target {
		"s::r" => {
			builder.ringbuffer(target).rows(rows).done();
		}
		"s::x" => {
			builder.series(target).rows(rows).done();
		}
		_ => {
			builder.table(target).rows(rows).done();
		}
	}
	builder.execute().map(|_| ()).map_err(|e| e.diagnostic())
}

fn bulk(t: &TestEngine, target: &str, mode: Mode, values: &[Value]) -> Result<(), Diagnostic> {
	let rows = rows(mode, values);
	match mode {
		Mode::Named | Mode::Positional => execute(t.bulk_insert(TestEngine::identity()), target, rows),
		Mode::UncheckedNamed | Mode::UncheckedPositional => {
			execute(t.bulk_insert_unchecked(TestEngine::identity()), target, rows)
		}
	}
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn stored_by_key(t: &TestEngine, target: &str) -> Vec<Value> {
	let frames = t.query(&format!("FROM {target}"));
	let mut rows: Vec<(Value, Value)> =
		column_values(&frames, "k").into_iter().zip(column_values(&frames, "d")).collect();
	rows.sort_by_key(|(k, _)| match k {
		Value::Int8(k) => *k,
		other => panic!("k must be int8, got {other:?}"),
	});
	rows.into_iter().map(|(_, d)| d).collect()
}

fn assert_cast_error_on_row(t: &TestEngine, column_type: &str, values: &[Value], row: usize) {
	for target in TARGETS {
		for mode in MODES {
			let label = format!("{column_type} {target} {mode:?} {values:?}");
			let err = bulk(t, target, mode, values).expect_err(&label);
			assert_eq!(err.code, "CAST_002", "{label}: {err:?}");
			assert_eq!(err.fragment.text(), "d", "{label}: the error must name the column");
			assert!(
				err.notes.iter().any(|n| n.starts_with(&format!("row {row} "))),
				"{label}: the error must name row {row}: {err:?}"
			);
			assert_eq!(TestEngine::row_count(&t.query(&format!("FROM {target}"))), 0, "{label}");
		}
	}
}

#[test]
fn bulk_inserting_text_after_a_float_into_a_float8_column_is_the_cast_error_naming_column_and_row() {
	// Rows are gathered into one column buffer, so a later row of another type must not panic the push.
	let t = engine("float8");

	assert_cast_error_on_row(&t, "float8", &[Value::float8(1.5), Value::Utf8("abc".to_string())], 2);
}

#[test]
fn bulk_inserting_text_before_a_float_into_a_float8_column_is_the_cast_error_naming_column_and_row() {
	// Text in the first row types the buffer as utf8, so the float after it must not panic the push.
	let t = engine("float8");

	assert_cast_error_on_row(&t, "float8", &[Value::Utf8("abc".to_string()), Value::float8(1.5)], 1);
}

#[test]
fn bulk_inserting_text_after_none_into_an_optional_float8_column_is_the_cast_error_naming_column_and_row() {
	// A none first row leaves the buffer untyped, so the text must still be cast and refused, never encoded.
	let t = engine("Option(float8)");

	assert_cast_error_on_row(&t, "Option(float8)", &[Value::none(), Value::Utf8("abc".to_string())], 2);
}

#[test]
fn bulk_inserting_castable_values_of_mixed_types_stores_them_as_the_column_type() {
	// Unchecked mode skips validation, not coercion, so an int4 must never reach a float8 slot uncast.
	let cases: [(&str, Vec<Value>, Vec<Value>); 4] = [
		("float8", vec![Value::Int4(1), Value::float8(2.5)], vec![Value::float8(1.0), Value::float8(2.5)]),
		(
			"float8",
			vec![Value::Utf8("1.5".to_string()), Value::float8(2.5)],
			vec![Value::float8(1.5), Value::float8(2.5)],
		),
		("int8", vec![Value::Int4(1), Value::Int8(2)], vec![Value::Int8(1), Value::Int8(2)]),
		("int4", vec![Value::Boolean(true), Value::Int4(2)], vec![Value::Int4(1), Value::Int4(2)]),
	];
	for (column_type, values, stored) in cases {
		for target in TARGETS {
			for mode in MODES {
				let t = engine(column_type);
				let label = format!("{column_type} {target} {mode:?} {values:?}");
				bulk(&t, target, mode, &values).unwrap_or_else(|e| panic!("{label}: {e:?}"));
				assert_eq!(stored_by_key(&t, target), stored, "{label}");
			}
		}
	}
}
