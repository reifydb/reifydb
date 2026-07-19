// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_core::row::row_shape_from_columns;
use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::interceptor::{
	dictionary_row::dictionary_row_pre_insert,
	interceptors::Interceptors,
	ringbuffer_row::{ringbuffer_row_pre_insert, ringbuffer_row_pre_update},
	series_row::{series_row_pre_insert, series_row_pre_update},
	table_row::{table_row_pre_insert, table_row_pre_update},
};
use reifydb_value::value::{Value, constraint::TypeConstraint, value_type::ValueType};

const MUTATED_VALUE: i64 = 999;

#[test]
fn test_table_row_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_row_pre_insert.add(Arc::new(table_row_pre_insert(|ctx| {
			let shape = row_shape_from_columns(&ctx.table.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int8, b: int8 }");
	t.command("INSERT test::t [{ a: 1, b: 2 }]");

	let frames = t.query("FROM test::t");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_table_row_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_row_pre_update.add(Arc::new(table_row_pre_update(|ctx| {
			let shape = row_shape_from_columns(&ctx.table.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int8, b: int8 }");
	t.command("INSERT test::t [{ a: 1, b: 2 }]");
	t.command("UPDATE test::t { b: 3 } FILTER { a == 1 }");

	let frames = t.query("FROM test::t");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_ringbuffer_row_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_row_pre_insert.add(Arc::new(ringbuffer_row_pre_insert(|ctx| {
			let shape = row_shape_from_columns(&ctx.ringbuffer.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { a: int8, b: int8 } WITH { capacity: 10 }");
	t.command("INSERT test::rb [{ a: 1, b: 2 }]");

	let frames = t.query("FROM test::rb");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_ringbuffer_row_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_row_pre_update.add(Arc::new(ringbuffer_row_pre_update(|ctx| {
			let shape = row_shape_from_columns(&ctx.ringbuffer.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { a: int8, b: int8 } WITH { capacity: 10 }");
	t.command("INSERT test::rb [{ a: 1, b: 2 }]");
	t.command("UPDATE test::rb { b: 3 } FILTER { a == 1 }");

	let frames = t.query("FROM test::rb");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_dictionary_row_pre_insert_mutates_value() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.dictionary_row_pre_insert.add(Arc::new(dictionary_row_pre_insert(|ctx| {
			ctx.values[0] = Value::Utf8("MUTATED".into());
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::d FOR Utf8 AS Uint8");
	t.command("INSERT test::d [{ value: 'hello' }]");

	let frames = t.query("FROM test::d");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<String>("value").unwrap().unwrap(), "MUTATED");
}

fn series_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::new("ts", TypeConstraint::unconstrained(ValueType::Int8)),
		RowShapeField::new("val", TypeConstraint::unconstrained(ValueType::Int8)),
	])
}

#[test]
fn test_series_row_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_row_pre_insert.add(Arc::new(series_row_pre_insert(|ctx| {
			let shape = series_shape();
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 42 }]");

	let frames = t.query("FROM test::s");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("val").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_series_row_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_row_pre_update.add(Arc::new(series_row_pre_update(|ctx| {
			let shape = series_shape();
			shape.set_value(&mut ctx.rows[0], 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 42 }]");
	t.command("UPDATE test::s { val: 100 } FILTER { ts == 1000 }");

	let frames = t.query("FROM test::s");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("val").unwrap().unwrap(), MUTATED_VALUE);
}

// A pre-update interceptor gets unrestricted `&mut [EncodedRow]` access to the row, invoked after the
// UPDATE statement's own partition-column check already validated the (pre-mutation) partition and the
// storage key was computed from it. Without a post-interceptor re-check, an interceptor could flip a
// partition column here and the row would still be written under the now-stale key, desyncing storage
// from the row's actual partition. These pin the re-validation added to guard against that.

#[test]
fn test_table_row_pre_update_partition_change_rejected() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_row_pre_update.add(Arc::new(table_row_pre_update(|ctx| {
			let shape = row_shape_from_columns(&ctx.table.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Utf8("eu".into()));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int8, region: utf8, n: int8 } WITH { partition: { by: { region } } }");
	t.command(r#"INSERT test::t [{ id: 1, region: "us", n: 1 }]"#);

	// Only `n` is assigned; the interceptor is the one flipping `region` (the partition column).
	let err = t.command_err("UPDATE test::t { n: 2 } FILTER { id == 1 }");
	assert!(err.contains("PART_002"), "expected PART_002 (ImmutablePartitionColumn), got: {err}");
}

#[test]
fn test_ringbuffer_row_pre_update_partition_change_rejected() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_row_pre_update.add(Arc::new(ringbuffer_row_pre_update(|ctx| {
			let shape = row_shape_from_columns(&ctx.ringbuffer.columns);
			shape.set_value(&mut ctx.rows[0], 1, &Value::Utf8("eu".into()));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE RINGBUFFER test::rb { id: int8, region: utf8, n: int8 } WITH { capacity: 10, partition: { by: { region } } }",
	);
	t.command(r#"INSERT test::rb [{ id: 1, region: "us", n: 1 }]"#);

	let err = t.command_err("UPDATE test::rb { n: 2 } FILTER { id == 1 }");
	assert!(err.contains("PART_002"), "expected PART_002 (ImmutablePartitionColumn), got: {err}");
}

// Storage layout for series rows is [key_column, ...data_columns] (see get_or_create_series_shape),
// not series.columns' declared order. Declaring the key column (`ts`) after the partition column
// (`region`) means a naive "index within series.columns" lookup (0) would land on the wrong storage
// field (the key, at index 0) instead of `region` (actually at index 1) - this schema exercises that.
fn partitioned_series_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::new("ts", TypeConstraint::unconstrained(ValueType::Int8)),
		RowShapeField::new("region", TypeConstraint::unconstrained(ValueType::Utf8)),
		RowShapeField::new("n", TypeConstraint::unconstrained(ValueType::Int8)),
	])
}

#[test]
fn test_series_row_pre_update_partition_change_rejected() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_row_pre_update.add(Arc::new(series_row_pre_update(|ctx| {
			let shape = partitioned_series_shape();
			shape.set_value(&mut ctx.rows[0], 1, &Value::Utf8("eu".into()));
			Ok(())
		})));
	}));

	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE SERIES test::s { region: utf8, ts: int8, n: int8 } WITH { key: ts, partition: { by: { region } } }",
	);
	t.command(r#"INSERT test::s [{ region: "us", ts: 1000, n: 1 }]"#);

	let err = t.command_err("UPDATE test::s { n: 2 } FILTER { ts == 1000 }");
	assert!(err.contains("PART_002"), "expected PART_002 (ImmutablePartitionColumn), got: {err}");
}
