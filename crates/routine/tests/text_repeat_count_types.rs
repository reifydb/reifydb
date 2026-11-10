// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{from_many, none_typed, utf8};
use reifydb_routine::function::text::repeat::TextRepeat;
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, column_view::ColumnView, identity::IdentityId, value_type::ValueType},
};

fn ctx(name: &str, row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn repeat(args: Vec<(FieldRef, ArrayRef)>, row_count: usize) -> Result<(FieldRef, ArrayRef), RoutineError> {
	TextRepeat::new().call(&mut ctx("text::repeat", row_count), &args)
}

#[test]
fn text_repeat_takes_every_integer_type_as_its_count() {
	// A sum over int4 answers int16; a count type left out fails every view that repeats by a sum.
	let counts = [
		Value::int1(3i8),
		Value::int2(3i16),
		Value::int4(3i32),
		Value::int8(3i64),
		Value::int16(3i128),
		Value::uint1(3u8),
		Value::uint2(3u16),
		Value::uint4(3u32),
		Value::uint8(3u64),
		Value::uint16(3u128),
	];
	for count in counts {
		let ty = count.get_type();
		let result = repeat(vec![utf8("arg0", ["x"]), from_many("arg1", count, 1)], 1)
			.unwrap_or_else(|err| panic!("a {ty:?} count is refused: {err:?}"));
		let view = ColumnView::try_from(&result).unwrap();
		assert_eq!(view.get_value(0).to_string(), "xxx", "a {ty:?} count of 3");
	}
}

#[test]
fn text_repeat_count_past_i64_is_out_of_range() {
	// A wide count narrowed by a plain cast wraps around and repeats a wrong number of times.
	let count = Value::int16(i64::MAX as i128 + 1);
	let err = repeat(vec![utf8("arg0", ["x"]), from_many("arg1", count, 1)], 1)
		.expect_err("a count past i64 must be refused");
	match err {
		RoutineError::FunctionExecutionFailed {
			reason,
			..
		} => assert!(reason.contains("out of range"), "the reason names the range: {reason}"),
		other => panic!("expected an out of range failure, got {other:?}"),
	}
}

#[test]
fn text_repeat_rejects_a_non_integer_count_even_with_no_rows() {
	// A type check reached only per row lets a view over an empty table break on its first insert.
	let err = repeat(vec![utf8("arg0", Vec::<String>::new()), utf8("arg1", Vec::<String>::new())], 0)
		.expect_err("a utf8 count must be refused before any row is read");
	match err {
		RoutineError::FunctionInvalidArgumentType {
			argument_index,
			expected,
			actual,
			..
		} => {
			assert_eq!(argument_index, 1);
			assert_eq!(actual, ValueType::Utf8);
			assert_eq!(expected.len(), 10, "every integer type is named: {expected:?}");
			assert!(expected.contains(&ValueType::Int16), "int16 is named: {expected:?}");
		}
		other => panic!("expected an argument type error, got {other:?}"),
	}
}

#[test]
fn text_repeat_negative_count_is_empty() {
	// A negative count cast to usize would try to allocate an absurd string instead of answering empty.
	let negative = repeat(vec![utf8("arg0", ["x"]), from_many("arg1", Value::int4(-1), 1)], 1).unwrap();
	assert_eq!(ColumnView::try_from(&negative).unwrap().get_value(0).to_string(), "");
}

#[test]
fn text_repeat_none_count_answers_none() {
	// A none count answered as an empty string would turn a missing value into a present one.
	let none = repeat(vec![utf8("arg0", ["x"]), none_typed("arg1", ValueType::Int4, 1)], 1).unwrap();
	assert_eq!(ColumnView::try_from(&none).unwrap().get_value(0), Value::none_of(ValueType::Utf8));
}
