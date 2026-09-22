// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::{text::char::TextChar, time::new::TimeNew};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{identity::IdentityId, value_type::ValueType},
};

fn ctx(name: &str) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count: 1,
		runtime_context: &RUNTIME,
	}
}

fn call(
	routine: &dyn Routine<FunctionContext<'static>>,
	name: &str,
	args: Vec<ColumnBuffer>,
) -> Result<Columns, RoutineError> {
	let columns = Columns::new(
		args.into_iter()
			.enumerate()
			.map(|(i, data)| ColumnWithName::new(Fragment::internal(format!("arg{i}")), data))
			.collect(),
	);
	routine.call(&mut ctx(name), &columns)
}

fn every_integer_width(value: u8) -> Vec<ColumnBuffer> {
	vec![
		ColumnBuffer::int1([value as i8]),
		ColumnBuffer::int2([value as i16]),
		ColumnBuffer::int4([value as i32]),
		ColumnBuffer::int8([value as i64]),
		ColumnBuffer::int16([value as i128]),
		ColumnBuffer::uint1([value]),
		ColumnBuffer::uint2([value as u16]),
		ColumnBuffer::uint4([value as u32]),
		ColumnBuffer::uint8([value as u64]),
		ColumnBuffer::uint16([value as u128]),
	]
}

fn expected_types(result: Result<Columns, RoutineError>) -> Vec<ValueType> {
	match result {
		Err(RoutineError::FunctionInvalidArgumentType {
			expected,
			..
		}) => expected,
		Err(other) => panic!("a text argument must be an invalid argument type error, got {other:?}"),
		Ok(_) => panic!("a text argument must be rejected, but the call succeeded"),
	}
}

fn sorted(mut types: Vec<ValueType>) -> Vec<ValueType> {
	types.sort();
	types
}

#[test]
fn text_char_error_names_exactly_the_integer_types_it_accepts() {
	// The expected list is how a caller fixes the query, so it must name every accepted type and nothing else.
	let routine = TextChar::new();
	let accepted: Vec<ValueType> = every_integer_width(65)
		.into_iter()
		.filter(|data| call(&routine, "text::char", vec![data.clone()]).is_ok())
		.map(|data| data.get_type())
		.collect();

	let listed = expected_types(call(&routine, "text::char", vec![ColumnBuffer::utf8(["A"])]));

	assert_eq!(
		sorted(listed.clone()),
		sorted(accepted.clone()),
		"text::char accepts {accepted:?} but its type error lists {listed:?}"
	);
}

#[test]
fn time_new_nano_error_names_exactly_the_integer_types_it_accepts() {
	// Every argument is read by one integer check, so the nano error must list what hour lists and what succeeds.
	let routine = TimeNew::new();
	let one = || ColumnBuffer::int4([1]);
	let accepted: Vec<ValueType> = every_integer_width(1)
		.into_iter()
		.filter(|nano| call(&routine, "time::new", vec![one(), one(), one(), nano.clone()]).is_ok())
		.map(|nano| nano.get_type())
		.collect();

	let nano_listed =
		expected_types(call(&routine, "time::new", vec![one(), one(), one(), ColumnBuffer::utf8(["1"])]));
	let hour_listed =
		expected_types(call(&routine, "time::new", vec![ColumnBuffer::utf8(["1"]), one(), one(), one()]));

	assert_eq!(
		sorted(nano_listed.clone()),
		sorted(accepted.clone()),
		"the nano argument accepts {accepted:?} but its type error lists {nano_listed:?}"
	);
	assert_eq!(
		sorted(nano_listed.clone()),
		sorted(hour_listed.clone()),
		"hour lists {hour_listed:?} for the same integer check, but nano lists {nano_listed:?}"
	);
}
