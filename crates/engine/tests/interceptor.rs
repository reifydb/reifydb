// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::encoded::schema::{Schema, SchemaField};
use reifydb_engine::test_prelude::*;
use reifydb_transaction::interceptor::{
	dictionary::dictionary_pre_insert,
	interceptors::Interceptors,
	ringbuffer::{ringbuffer_pre_insert, ringbuffer_pre_update},
	series::{series_pre_insert, series_pre_update},
	table::{table_pre_insert, table_pre_update},
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

const MUTATED_VALUE: i64 = 999;

#[test]
fn test_table_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_pre_insert.add(Arc::new(table_pre_insert(|ctx| {
			let schema = Schema::from(&ctx.table.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
fn test_table_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_pre_update.add(Arc::new(table_pre_update(|ctx| {
			let schema = Schema::from(&ctx.table.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
fn test_ringbuffer_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_pre_insert.add(Arc::new(ringbuffer_pre_insert(|ctx| {
			let schema = Schema::from(&ctx.ringbuffer.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
fn test_ringbuffer_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_pre_update.add(Arc::new(ringbuffer_pre_update(|ctx| {
			let schema = Schema::from(&ctx.ringbuffer.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
fn test_dictionary_pre_insert_mutates_value() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.dictionary_pre_insert.add(Arc::new(dictionary_pre_insert(|ctx| {
			ctx.value = Value::Utf8("MUTATED".into());
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

fn series_schema() -> Schema {
	Schema::new(vec![
		SchemaField::new("ts", TypeConstraint::unconstrained(Type::Int8)),
		SchemaField::new("val", TypeConstraint::unconstrained(Type::Int8)),
	])
}

#[test]
fn test_series_pre_insert_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_pre_insert.add(Arc::new(series_pre_insert(|ctx| {
			let schema = series_schema();
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
fn test_series_pre_update_mutates_row() {
	let t = TestEngine::new();

	t.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_pre_update.add(Arc::new(series_pre_update(|ctx| {
			let schema = series_schema();
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
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
