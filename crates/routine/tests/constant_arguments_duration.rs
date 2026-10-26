// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{duration, from_many, int8, none_typed, utf8};
use reifydb_routine::function::duration::{
	add::DurationAdd, days::DurationDays, format::DurationFormat, get_days::DurationGetDays,
	get_months::DurationGetMonths, get_nanos::DurationGetNanos, hours::DurationHours, millis::DurationMillis,
	minutes::DurationMinutes, months::DurationMonths, negate::DurationNegate, scale::DurationScale,
	seconds::DurationSeconds, subtract::DurationSubtract, trunc::DurationTrunc, weeks::DurationWeeks,
	years::DurationYears,
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, duration::Duration, identity::IdentityId, value_type::ValueType},
};

const ROWS: usize = 4;

type Column = (FieldRef, ArrayRef);

struct Arg {
	one: Column,
	repeated: Column,
	varying: Column,
}

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("duration"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn dur(months: i32, days: i32, nanos: i64) -> Duration {
	Duration::new(months, days, nanos).unwrap()
}

fn constant(value: Value, varying: Column) -> Arg {
	Arg {
		one: from_many("arg", value.clone(), 1),
		repeated: from_many("arg", value, ROWS),
		varying,
	}
}

fn none_constant(ty: ValueType, varying: Column) -> Arg {
	Arg {
		one: none_typed("arg", ty.clone(), 1),
		repeated: none_typed("arg", ty, ROWS),
		varying,
	}
}

fn durations() -> Column {
	duration(
		"arg",
		vec![
			dur(14, 3, 3_723_000_000_123),
			dur(0, 1, 0),
			dur(-25, -2, -90_061_000_000_000),
			dur(1, 0, 59_999_999),
		],
	)
}

fn duration_arg() -> Arg {
	constant(Value::Duration(dur(27, 5, 7_384_123_456_789)), durations())
}

fn integer_arg() -> Arg {
	constant(Value::Int8(3), int8("arg", vec![1, -2, 5, 0]))
}

fn call<F: Function>(function: &F, args: &[Column]) -> Column {
	function.call(&mut ctx(ROWS), args).unwrap()
}

fn assert_same(label: &str, actual: &Column, expected: &Column) {
	assert_eq!(actual.1.len(), ROWS, "{label}: result rows");
	assert_eq!(actual.0.is_nullable(), expected.0.is_nullable(), "{label}: nullability");
	assert_eq!(actual.1.to_data(), expected.1.to_data(), "{label}: values");
}

fn check<F: Function>(name: &str, function: F, args: Vec<Arg>) {
	for position in 0..args.len() {
		let pick = |constant: fn(&Arg) -> &Column| -> Vec<Column> {
			args.iter()
				.enumerate()
				.map(|(i, arg)| {
					if i == position {
						constant(arg).clone()
					} else {
						arg.varying.clone()
					}
				})
				.collect()
		};
		let actual = call(&function, &pick(|arg| &arg.one));
		let expected = call(&function, &pick(|arg| &arg.repeated));
		assert_same(&format!("{name} constant at {position}"), &actual, &expected);
	}
	let actual = call(&function, &args.iter().map(|arg| arg.one.clone()).collect::<Vec<_>>());
	let expected = call(&function, &args.iter().map(|arg| arg.repeated.clone()).collect::<Vec<_>>());
	assert_same(&format!("{name} all constant"), &actual, &expected);
}

#[test]
fn duration_constructors_broadcast_a_one_row_integer() {
	// Without broadcasting, a constant integer answers one row instead of one per input row.
	check("days", DurationDays::new(), vec![integer_arg()]);
	check("hours", DurationHours::new(), vec![integer_arg()]);
	check("millis", DurationMillis::new(), vec![integer_arg()]);
	check("minutes", DurationMinutes::new(), vec![integer_arg()]);
	check("months", DurationMonths::new(), vec![integer_arg()]);
	check("seconds", DurationSeconds::new(), vec![integer_arg()]);
	check("weeks", DurationWeeks::new(), vec![integer_arg()]);
	check("years", DurationYears::new(), vec![integer_arg()]);
}

#[test]
fn duration_constructors_broadcast_a_one_row_none() {
	// A none constant must answer none on every row, not on one.
	let none = || none_constant(ValueType::Int8, int8("arg", vec![1, -2, 5, 0]));
	check("days", DurationDays::new(), vec![none()]);
	check("hours", DurationHours::new(), vec![none()]);
	check("millis", DurationMillis::new(), vec![none()]);
	check("minutes", DurationMinutes::new(), vec![none()]);
	check("months", DurationMonths::new(), vec![none()]);
	check("seconds", DurationSeconds::new(), vec![none()]);
	check("weeks", DurationWeeks::new(), vec![none()]);
	check("years", DurationYears::new(), vec![none()]);
}

#[test]
fn duration_accessors_and_negate_broadcast_a_one_row_duration() {
	// Iterating the argument instead of the row count answers one row for a constant duration.
	check("get_days", DurationGetDays::new(), vec![duration_arg()]);
	check("get_months", DurationGetMonths::new(), vec![duration_arg()]);
	check("get_nanos", DurationGetNanos::new(), vec![duration_arg()]);
	check("negate", DurationNegate::new(), vec![duration_arg()]);
	let none = || none_constant(ValueType::Duration, durations());
	check("get_days none", DurationGetDays::new(), vec![none()]);
	check("get_months none", DurationGetMonths::new(), vec![none()]);
	check("get_nanos none", DurationGetNanos::new(), vec![none()]);
	check("negate none", DurationNegate::new(), vec![none()]);
}

#[test]
fn duration_arithmetic_broadcasts_a_one_row_operand_on_either_side() {
	// A row past the end of a one-row operand must read row 0, never fall back to a zero duration.
	check("add", DurationAdd::new(), vec![duration_arg(), duration_arg()]);
	check("subtract", DurationSubtract::new(), vec![duration_arg(), duration_arg()]);
	check("scale", DurationScale::new(), vec![duration_arg(), integer_arg()]);
	let none = || none_constant(ValueType::Duration, durations());
	check("add none", DurationAdd::new(), vec![none(), duration_arg()]);
	check("subtract none", DurationSubtract::new(), vec![duration_arg(), none()]);
	check(
		"scale none",
		DurationScale::new(),
		vec![duration_arg(), none_constant(ValueType::Int8, int8("arg", vec![1, -2, 5, 0]))],
	);
}

#[test]
fn duration_text_functions_broadcast_a_one_row_duration_or_pattern() {
	// A one-row pattern read past its end answers an empty string or a zero duration for every later row.
	let pattern = |value: &str, varying: Vec<&str>| constant(Value::Utf8(value.to_string()), utf8("arg", varying));
	check(
		"format",
		DurationFormat::new(),
		vec![duration_arg(), pattern("%Y-%M-%D %h:%m:%s.%f", vec!["%D", "%h:%m", "%s.%f", "%Y %%"])],
	);
	check(
		"trunc",
		DurationTrunc::new(),
		vec![duration_arg(), pattern("hour", vec!["year", "day", "minute", "millis"])],
	);
	let none = || none_constant(ValueType::Utf8, utf8("arg", vec!["%D", "%h", "%m", "%s"]));
	check("format none", DurationFormat::new(), vec![duration_arg(), none()]);
	check(
		"trunc none",
		DurationTrunc::new(),
		vec![
			none_constant(ValueType::Duration, durations()),
			pattern("day", vec!["year", "day", "minute", "millis"]),
		],
	);
}
