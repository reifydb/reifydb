// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	sync::LazyLock,
};

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{from_many, int8, int8_with_bitvec, none, utf8_repeated},
	nulls::with_nulls,
};
use reifydb_routine::function::math::{
	add::{
		basic::Add, default::AddDefault, none::AddNone, saturate::AddSaturate, strict::AddStrict,
		wrap::AddWrap, zero::AddZero,
	},
	div::{
		basic::Div, default::DivDefault, none::DivNone, saturate::DivSaturate, strict::DivStrict,
		wrap::DivWrap, zero::DivZero,
	},
	mul::{
		basic::Mul, default::MulDefault, none::MulNone, saturate::MulSaturate, strict::MulStrict,
		wrap::MulWrap, zero::MulZero,
	},
	rem::{
		basic::Rem, default::RemDefault, none::RemNone, saturate::RemSaturate, strict::RemStrict,
		wrap::RemWrap, zero::RemZero,
	},
	sub::{
		basic::Sub, default::SubDefault, none::SubNone, saturate::SubSaturate, strict::SubStrict,
		wrap::SubWrap, zero::SubZero,
	},
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, identity::IdentityId},
};

const ROWS: usize = 5;

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

#[derive(Clone)]
enum Constant {
	Value(Value),
	Some(i64),
	None,
	Text(&'static str),
}

impl Constant {
	fn column(&self, len: usize) -> (FieldRef, ArrayRef) {
		match self {
			Constant::Value(value) => from_many("constant", value.clone(), len),
			Constant::Some(value) => {
				with_nulls(int8("constant", vec![*value; len]), NullBuffer::new_valid(len)).unwrap()
			}
			Constant::None => none("constant", len),
			Constant::Text(text) => utf8_repeated("constant", text, len),
		}
	}
}

fn left_column() -> (FieldRef, ArrayRef) {
	int8_with_bitvec("left", [i64::MAX, i64::MIN, 7, -3, 0], vec![true, true, false, true, true])
}

fn right_column() -> (FieldRef, ArrayRef) {
	int8("right", [i64::MAX, -1, 2, 0, 5])
}

fn outcome<F: Function>(function: &F, args: &[(FieldRef, ArrayRef)]) -> Result<Result<ArrayRef, String>, String> {
	catch_unwind(AssertUnwindSafe(|| {
		function.call(&mut ctx(ROWS), args).map(|(_, array)| array).map_err(|error| format!("{error:?}"))
	}))
	.map_err(|panic| {
		panic.downcast_ref::<String>()
			.cloned()
			.or_else(|| panic.downcast_ref::<&str>().map(|text| text.to_string()))
			.unwrap_or_else(|| "panic".to_string())
	})
}

fn check<F: Function>(
	name: &str,
	function: &F,
	defaults: &[(FieldRef, ArrayRef)],
	position: usize,
	constant: &Constant,
	wrong: &mut Vec<String>,
) {
	let with = |len: usize| {
		let mut args = defaults.to_vec();
		args[position] = constant.column(len);
		args
	};
	let label = format!("{name} arg {position} = {}", describe(constant));
	compare(&label, outcome(function, &with(1)), outcome(function, &with(ROWS)), wrong);
}

fn compare(
	label: &str,
	one_row: Result<Result<ArrayRef, String>, String>,
	repeated: Result<Result<ArrayRef, String>, String>,
	wrong: &mut Vec<String>,
) {
	match (one_row, repeated) {
		(Ok(Ok(one_row)), Ok(Ok(repeated))) => {
			if one_row.len() != ROWS {
				wrong.push(format!("{label}: 1-row constant gave {} rows", one_row.len()));
			} else if one_row.to_data() != repeated.to_data() {
				wrong.push(format!("{label}: 1-row {one_row:?} != repeated {repeated:?}"));
			}
		}
		(Ok(Err(one_row)), Ok(Err(repeated))) if one_row == repeated => {}
		(one_row, repeated) => wrong.push(format!(
			"{label}: 1-row {:?} != repeated {:?}",
			one_row.map(|result| result.map(|array| array.len())),
			repeated.map(|result| result.map(|array| array.len()))
		)),
	}
}

fn describe(constant: &Constant) -> String {
	match constant {
		Constant::Value(value) => format!("{value:?}"),
		Constant::Some(value) => format!("some({value})"),
		Constant::None => "none".to_string(),
		Constant::Text(text) => format!("{text:?}"),
	}
}

fn operands() -> Vec<Constant> {
	vec![
		Constant::Value(Value::Int8(i64::MAX)),
		Constant::Value(Value::Int8(i64::MIN)),
		Constant::Value(Value::Int8(-1)),
		Constant::Value(Value::Int8(2)),
		Constant::Value(Value::Int8(0)),
		Constant::Some(i64::MAX),
		Constant::Some(3),
		Constant::None,
	]
}

fn check_both(name: &str, function: &impl Function, third: Option<Constant>, wrong: &mut Vec<String>) {
	for left in operands() {
		for right in operands() {
			let with = |len: usize| {
				let mut args = vec![left.column(len), right.column(len)];
				args.extend(third.iter().map(|third| third.column(ROWS)));
				args
			};
			let label = format!("{name} both = {}, {}", describe(&left), describe(&right));
			compare(&label, outcome(function, &with(1)), outcome(function, &with(ROWS)), wrong);
		}
	}
}

fn check_two<F: Function>(name: &str, function: F, wrong: &mut Vec<String>) {
	let defaults = vec![left_column(), right_column()];
	for position in 0..2 {
		for constant in operands() {
			check(name, &function, &defaults, position, &constant, wrong);
		}
	}
	check_both(name, &function, None, wrong);
}

fn check_three<F: Function>(name: &str, function: F, third: Vec<Constant>, wrong: &mut Vec<String>) {
	let defaults = vec![left_column(), right_column(), third[0].column(ROWS)];
	for position in 0..2 {
		for constant in operands() {
			check(name, &function, &defaults, position, &constant, wrong);
		}
	}
	for constant in &third {
		check(name, &function, &defaults, 2, constant, wrong);
	}
	check_both(name, &function, Some(third[0].clone()), wrong);
}

fn fallbacks() -> Vec<Constant> {
	vec![Constant::Value(Value::Int8(5)), Constant::Some(6), Constant::None]
}

fn messages() -> Vec<Constant> {
	vec![Constant::Text("custom overflow message")]
}

macro_rules! two {
	($wrong:expr; $($name:literal => $function:ty),* $(,)?) => {
		$(check_two($name, <$function>::new(), $wrong);)*
	};
}

macro_rules! three {
	($wrong:expr, $third:expr; $($name:literal => $function:ty),* $(,)?) => {
		$(check_three($name, <$function>::new(), $third, $wrong);)*
	};
}

#[test]
fn basic_and_overflow_variants_match_a_repeated_constant() {
	// A 1-row constant operand must be read as row 0 for every row, otherwise rows past the first index out of
	// bounds or turn none.
	let mut wrong = Vec::new();
	two!(&mut wrong;
		"math::add" => Add, "math::add_none" => AddNone, "math::add_saturate" => AddSaturate,
		"math::add_wrap" => AddWrap, "math::add_zero" => AddZero,
		"math::sub" => Sub, "math::sub_none" => SubNone, "math::sub_saturate" => SubSaturate,
		"math::sub_wrap" => SubWrap, "math::sub_zero" => SubZero,
		"math::mul" => Mul, "math::mul_none" => MulNone, "math::mul_saturate" => MulSaturate,
		"math::mul_wrap" => MulWrap, "math::mul_zero" => MulZero,
		"math::div" => Div, "math::div_none" => DivNone, "math::div_saturate" => DivSaturate,
		"math::div_wrap" => DivWrap, "math::div_zero" => DivZero,
		"math::rem" => Rem, "math::rem_none" => RemNone, "math::rem_saturate" => RemSaturate,
		"math::rem_wrap" => RemWrap, "math::rem_zero" => RemZero,
	);
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn default_variants_match_a_repeated_constant() {
	// A 1-row fallback must fill every overflowed row, not only row 0, and a none constant must stay none on every
	// row.
	let mut wrong = Vec::new();
	three!(&mut wrong, fallbacks();
		"math::add_default" => AddDefault, "math::sub_default" => SubDefault,
		"math::mul_default" => MulDefault, "math::div_default" => DivDefault,
		"math::rem_default" => RemDefault,
	);
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn strict_variants_match_a_repeated_constant() {
	// A 1-row message must be the error text for an overflow on any row, never the generic fallback text.
	let mut wrong = Vec::new();
	three!(&mut wrong, messages();
		"math::add_strict" => AddStrict, "math::sub_strict" => SubStrict,
		"math::mul_strict" => MulStrict, "math::div_strict" => DivStrict,
		"math::rem_strict" => RemStrict,
	);
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
