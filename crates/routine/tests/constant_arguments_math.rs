// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	sync::LazyLock,
};

use arrow_array::{Array, ArrayRef, Float64Array};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{decimal, float8, from_many, int4};
use reifydb_routine::function::math::{
	abs::Abs, acos::Acos, approx_float8::ApproxFloat8, asin::Asin, atan::Atan, atan2::Atan2, avg::Avg, ceil::Ceil,
	clamp::Clamp, cos::Cos, count::Count, euler::Euler, exp::Exp, floor::Floor, gcd::Gcd, lcm::Lcm, log::Log,
	log2::Log2, log10::Log10, max::Max, min::Min, modulo::Modulo, pi::Pi, power::Power, round::Round, sign::Sign,
	sin::Sin, sqrt::Sqrt, sum::Sum, tan::Tan, truncate::Truncate,
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		identity::IdentityId,
		value_type::ValueType,
	},
};

const ROWS: usize = 4;

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn outcome<F: Function>(function: &F, args: &[(FieldRef, ArrayRef)]) -> Result<ArrayRef, String> {
	catch_unwind(AssertUnwindSafe(|| function.call(&mut ctx(ROWS), args)))
		.map_err(|_| "panicked".to_string())?
		.map(|(_, array)| array)
		.map_err(|error| format!("{error:?}"))
}

fn dec(text: &str) -> Decimal {
	Decimal::parse(text).unwrap()
}

fn none_of(inner: ValueType) -> Value {
	Value::None {
		inner,
	}
}

fn nones(inner: ValueType, count: usize) -> Vec<Value> {
	vec![none_of(inner); count]
}

fn ints() -> (FieldRef, ArrayRef) {
	int4("ints", [1, -2, 3, 4])
}

fn floats() -> (FieldRef, ArrayRef) {
	float8("floats", [0.5, -0.25, 0.75, 0.1])
}

fn decimals() -> (FieldRef, ArrayRef) {
	decimal("decimals", Precision::new(6), Scale::new(2), [dec("1.25"), dec("-2.50"), dec("3.75"), dec("0.05")])
}

fn mismatches<F: Function>(
	name: &str,
	function: &F,
	columns: &[(FieldRef, ArrayRef)],
	constants: &[Value],
) -> Vec<String> {
	let mut wrong = Vec::new();
	for (position, constant) in constants.iter().enumerate() {
		let with = |len: usize| {
			let mut args = columns.to_vec();
			args[position] = from_many("constant", constant.clone(), len);
			args
		};
		match (outcome(function, &with(1)), outcome(function, &with(ROWS))) {
			(Ok(one), Ok(all)) if one.len() == ROWS && one.to_data() == all.to_data() => {}
			(one, all) => wrong.push(format!(
				"{name} position {position} constant {constant:?}: one row gave {one:?}, repeated gave {all:?}"
			)),
		}
	}
	wrong
}

fn assert_none_wrong(wrong: Vec<String>) {
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn unary_constant_fills_every_row() {
	// Otherwise a 1-row constant answers 1 row, or reads past its end and turns rows 1.. into none.
	let float_constants = [Value::float8(0.5), Value::Int4(1), none_of(ValueType::Float8)];
	let mut wrong = Vec::new();
	for constant in &float_constants {
		let c = std::slice::from_ref(constant);
		let cols = [floats()];
		wrong.extend(mismatches("abs", &Abs::new(), &cols, c));
		wrong.extend(mismatches("acos", &Acos::new(), &cols, c));
		wrong.extend(mismatches("asin", &Asin::new(), &cols, c));
		wrong.extend(mismatches("atan", &Atan::new(), &cols, c));
		wrong.extend(mismatches("cos", &Cos::new(), &cols, c));
		wrong.extend(mismatches("sin", &Sin::new(), &cols, c));
		wrong.extend(mismatches("tan", &Tan::new(), &cols, c));
		wrong.extend(mismatches("exp", &Exp::new(), &cols, c));
		wrong.extend(mismatches("log", &Log::new(), &cols, c));
		wrong.extend(mismatches("log2", &Log2::new(), &cols, c));
		wrong.extend(mismatches("log10", &Log10::new(), &cols, c));
		wrong.extend(mismatches("sqrt", &Sqrt::new(), &cols, c));
		wrong.extend(mismatches("sign", &Sign::new(), &cols, c));
		wrong.extend(mismatches("approx_float8", &ApproxFloat8::new(), &cols, c));
		wrong.extend(mismatches("ceil", &Ceil::new(), &cols, c));
		wrong.extend(mismatches("floor", &Floor::new(), &cols, c));
		wrong.extend(mismatches("truncate", &Truncate::new(), &cols, c));
		wrong.extend(mismatches("round", &Round::new(), &cols, c));
	}
	assert_none_wrong(wrong);
}

#[test]
fn unary_decimal_and_integer_constant_fills_every_row() {
	// Decimal paths iterate the decoded values and integer paths pass the input through, both of constant length.
	let mut wrong = Vec::new();
	for constant in [Value::Decimal(dec("-1.75")), Value::Int4(-7)] {
		let c = std::slice::from_ref(&constant);
		let cols = [decimals()];
		wrong.extend(mismatches("abs", &Abs::new(), &cols, c));
		wrong.extend(mismatches("ceil", &Ceil::new(), &cols, c));
		wrong.extend(mismatches("floor", &Floor::new(), &cols, c));
		wrong.extend(mismatches("truncate", &Truncate::new(), &cols, c));
		wrong.extend(mismatches("round", &Round::new(), &cols, c));
	}
	assert_none_wrong(wrong);
}

#[test]
fn binary_constant_in_either_position_matches_repeated() {
	// A row loop bounded by the first argument's length, or reading the second past its end, breaks here.
	let mut wrong = Vec::new();
	let floats2 = [floats(), float8("x", [2.0, -1.5, 0.25, 3.0])];
	wrong.extend(mismatches("atan2", &Atan2::new(), &floats2, &[Value::float8(1.5), Value::float8(-0.5)]));
	wrong.extend(mismatches("atan2", &Atan2::new(), &floats2, &nones(ValueType::Float8, 2)));
	let ints2 = [ints(), int4("b", [6, 4, 9, 10])];
	let int_constants = [Value::Int4(12), Value::Int4(8)];
	wrong.extend(mismatches("gcd", &Gcd::new(), &ints2, &int_constants));
	wrong.extend(mismatches("gcd", &Gcd::new(), &ints2, &nones(ValueType::Int4, 2)));
	wrong.extend(mismatches("lcm", &Lcm::new(), &ints2, &int_constants));
	wrong.extend(mismatches("lcm", &Lcm::new(), &ints2, &nones(ValueType::Int4, 2)));
	wrong.extend(mismatches("mod", &Modulo::new(), &ints2, &int_constants));
	wrong.extend(mismatches("mod", &Modulo::new(), &ints2, &nones(ValueType::Int4, 2)));
	let pow_ints = [ints(), int4("e", [0, 1, 2, 3])];
	wrong.extend(mismatches("power", &Power::new(), &pow_ints, &[Value::Int4(3), Value::Int4(2)]));
	wrong.extend(mismatches("power", &Power::new(), &pow_ints, &nones(ValueType::Int4, 2)));
	wrong.extend(mismatches("power", &Power::new(), &floats2, &[Value::float8(2.0), Value::float8(0.5)]));
	let pow_decimals =
		[decimals(), decimal("e", Precision::new(6), Scale::new(2), [dec("1"), dec("2"), dec("0"), dec("3")])];
	wrong.extend(mismatches(
		"power",
		&Power::new(),
		&pow_decimals,
		&[Value::Decimal(dec("1.5")), Value::Decimal(dec("2"))],
	));
	assert_none_wrong(wrong);
}

#[test]
fn clamp_constant_in_any_position_matches_repeated() {
	// Otherwise clamp answers as many rows as the value argument, or none for rows past a 1-row bound.
	let mut wrong = Vec::new();
	let int_cols = [int4("v", [1, 5, 9, -3]), int4("lo", [0, 1, 2, -5]), int4("hi", [10, 8, 9, 3])];
	wrong.extend(mismatches("clamp", &Clamp::new(), &int_cols, &[Value::Int4(4), Value::Int4(2), Value::Int4(6)]));
	wrong.extend(mismatches("clamp", &Clamp::new(), &int_cols, &nones(ValueType::Int4, 3)));
	let float_cols = [floats(), float8("lo", [0.0, -1.0, 0.5, 0.0]), float8("hi", [1.0, 0.0, 0.6, 0.2])];
	wrong.extend(mismatches(
		"clamp",
		&Clamp::new(),
		&float_cols,
		&[Value::float8(0.3), Value::float8(-0.5), Value::float8(0.55)],
	));
	let none_cols = [int_cols[0].clone(), int_cols[1].clone(), int_cols[2].clone()];
	wrong.extend(mismatches("clamp", &Clamp::new(), &none_cols, &nones(ValueType::Any, 3)));
	assert_none_wrong(wrong);
}

#[test]
fn round_constant_value_or_digits_matches_repeated() {
	// The digits argument must be read per row and a constant value must still fill every row.
	let mut wrong = Vec::new();
	let digits = int4("digits", [0, 1, 2, 3]);
	wrong.extend(mismatches(
		"round",
		&Round::new(),
		&[floats(), digits.clone()],
		&[Value::float8(1.23456), Value::Int4(2)],
	));
	wrong.extend(mismatches(
		"round",
		&Round::new(),
		&[decimals(), digits.clone()],
		&[Value::Decimal(dec("1.2345")), Value::Int4(1)],
	));
	wrong.extend(mismatches("round", &Round::new(), &[ints(), digits.clone()], &[Value::Int4(7), Value::Int4(1)]));
	wrong.extend(mismatches(
		"round",
		&Round::new(),
		&[floats(), digits],
		&[none_of(ValueType::Float8), none_of(ValueType::Int4)],
	));
	assert_none_wrong(wrong);
}

#[test]
fn variadic_constant_in_any_position_matches_repeated() {
	// Row-wise min, max, avg, sum and count must see a 1-row argument in every row, not only row 0.
	let mut wrong = Vec::new();
	let ints2 = [ints(), int4("b", [6, -4, 2, 10])];
	let consts = [Value::Int4(3), Value::Int4(-1)];
	let nones = [none_of(ValueType::Int4), none_of(ValueType::Int4)];
	for constants in [&consts, &nones] {
		wrong.extend(mismatches("min", &Min::new(), &ints2, constants));
		wrong.extend(mismatches("max", &Max::new(), &ints2, constants));
		wrong.extend(mismatches("avg", &Avg::new(), &ints2, constants));
		wrong.extend(mismatches("count", &Count::new(), &ints2, constants));
		wrong.extend(mismatches("sum", &Sum::new(), &ints2[..1], &constants[..1]));
	}
	let floats2 = [floats(), float8("y", [1.0, 2.0, -3.0, 0.0])];
	wrong.extend(mismatches("avg", &Avg::new(), &floats2, &[Value::float8(0.5), Value::float8(4.0)]));
	let decimals2 =
		[decimals(), decimal("e", Precision::new(6), Scale::new(2), [dec("1"), dec("2"), dec("0"), dec("3")])];
	wrong.extend(mismatches(
		"avg",
		&Avg::new(),
		&decimals2,
		&[Value::Decimal(dec("1.5")), Value::Decimal(dec("2"))],
	));
	wrong.extend(mismatches("sum", &Sum::new(), &decimals2[..1], &[Value::Decimal(dec("1.5"))]));
	assert_none_wrong(wrong);
}

#[test]
fn no_argument_function_fills_every_row() {
	// A function with no arguments must still answer one row per input row.
	for (name, result, expected) in [
		("pi", outcome(&Pi::new(), &[]), std::f64::consts::PI),
		("euler", outcome(&Euler::new(), &[]), std::f64::consts::E),
	] {
		let array = result.unwrap();
		let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
		assert_eq!(values.len(), ROWS, "{name}");
		assert!(values.iter().all(|value| value == Some(expected)), "{name}: {values:?}");
	}
}
