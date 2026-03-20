// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::encoded::schema::{Schema, SchemaField};
use reifydb_engine::{engine::StandardEngine, test_utils::create_test_engine};
use reifydb_transaction::interceptor::{
	dictionary::dictionary_pre_insert,
	interceptors::Interceptors,
	ringbuffer::{ringbuffer_pre_insert, ringbuffer_pre_update},
	series::{series_pre_insert, series_pre_update},
	table::{table_pre_insert, table_pre_update},
};
use reifydb_type::value::{Value, constraint::TypeConstraint, frame::frame::Frame, identity::IdentityId, r#type::Type};

fn root() -> IdentityId {
	IdentityId::root()
}

fn admin(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.admin_as(root(), rql, Default::default()).unwrap()
}

fn command(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.command_as(root(), rql, Default::default()).unwrap()
}

fn query(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.query_as(root(), rql, Default::default()).unwrap()
}

const MUTATED_VALUE: i64 = 999;

#[test]
fn test_table_pre_insert_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_pre_insert.add(Arc::new(table_pre_insert(|ctx| {
			let schema = Schema::from(&ctx.table.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { a: int8, b: int8 }");
	command(&engine, "INSERT test::t [{ a: 1, b: 2 }]");

	let frames = query(&engine, "FROM test::t");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_table_pre_update_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.table_pre_update.add(Arc::new(table_pre_update(|ctx| {
			let schema = Schema::from(&ctx.table.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { a: int8, b: int8 }");
	command(&engine, "INSERT test::t [{ a: 1, b: 2 }]");
	command(&engine, "UPDATE test::t { b: 3 } FILTER { a == 1 }");

	let frames = query(&engine, "FROM test::t");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_ringbuffer_pre_insert_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_pre_insert.add(Arc::new(ringbuffer_pre_insert(|ctx| {
			let schema = Schema::from(&ctx.ringbuffer.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE RINGBUFFER test::rb { a: int8, b: int8 } WITH { capacity: 10 }");
	command(&engine, "INSERT test::rb [{ a: 1, b: 2 }]");

	let frames = query(&engine, "FROM test::rb");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_ringbuffer_pre_update_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.ringbuffer_pre_update.add(Arc::new(ringbuffer_pre_update(|ctx| {
			let schema = Schema::from(&ctx.ringbuffer.columns);
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE RINGBUFFER test::rb { a: int8, b: int8 } WITH { capacity: 10 }");
	command(&engine, "INSERT test::rb [{ a: 1, b: 2 }]");
	command(&engine, "UPDATE test::rb { b: 3 } FILTER { a == 1 }");

	let frames = query(&engine, "FROM test::rb");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("b").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_dictionary_pre_insert_mutates_value() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.dictionary_pre_insert.add(Arc::new(dictionary_pre_insert(|ctx| {
			ctx.value = Value::Utf8("MUTATED".into());
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE DICTIONARY test::d FOR Utf8 AS Uint8");
	command(&engine, "INSERT test::d [{ value: 'hello' }]");

	let frames = query(&engine, "FROM test::d");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<String>("value").unwrap().unwrap(), "MUTATED");
}

fn series_schema() -> Schema {
	Schema::new(vec![
		SchemaField::new("timestamp", TypeConstraint::unconstrained(Type::Int8)),
		SchemaField::new("val", TypeConstraint::unconstrained(Type::Int8)),
	])
}

#[test]
fn test_series_pre_insert_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_pre_insert.add(Arc::new(series_pre_insert(|ctx| {
			let schema = series_schema();
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE SERIES test::s { val: int8 } WITH { precision: millisecond }");
	command(&engine, "INSERT test::s [{ timestamp: 1000, val: 42 }]");

	let frames = query(&engine, "FROM test::s");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("val").unwrap().unwrap(), MUTATED_VALUE);
}

#[test]
fn test_series_pre_update_mutates_row() {
	let engine = create_test_engine();

	engine.add_interceptor_factory(Arc::new(|interceptors: &mut Interceptors| {
		interceptors.series_pre_update.add(Arc::new(series_pre_update(|ctx| {
			let schema = series_schema();
			schema.set_value(&mut ctx.row, 1, &Value::Int8(MUTATED_VALUE));
			Ok(())
		})));
	}));

	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE SERIES test::s { val: int8 } WITH { precision: millisecond }");
	command(&engine, "INSERT test::s [{ timestamp: 1000, val: 42 }]");
	command(&engine, "UPDATE test::s { val: 100 } FILTER { timestamp == 1000 }");

	let frames = query(&engine, "FROM test::s");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i64>("val").unwrap().unwrap(), MUTATED_VALUE);
}
