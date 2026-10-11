// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::{batch::empty_batch, column::factory};
use reifydb_evaluate::{
	expression::{compile::compile_expression, context::CompileContext},
	lower::LoweredExpr,
};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
	registry::Routines,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::value::{
	Value,
	value_type::{ValueType, field::from_field},
};

use crate::{
	common::{Env, and, batch, between, call, cast, column, none, number, registry, rows, strings, text},
	compare::{nullable_column, samples},
};

struct WrongReturnType {
	info: RoutineInfo,
}

impl<'a> Routine<FunctionContext<'a>> for WrongReturnType {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		Ok(factory::utf8_repeated(ctx.fragment.text(), "x", ctx.row_count))
	}
}

impl Function for WrongReturnType {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}

#[test]
fn routine_error_carries_the_name_token_fragment() {
	// The error must point at the routine name, not the whole call, otherwise every routine error moves its caret.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::utf8("a", ["x"])]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = env.old(&expression, input).unwrap_err();

	assert_eq!(lowered.fragment.text(), "math::abs");
	assert_eq!(lowered, old);
}

#[test]
fn unknown_function_matches_the_old_path() {
	// An unknown name must fail with the same code and name fragment on both paths, otherwise a typo reads
	// differently.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1])]);
	let expression = call("no::such", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = env.old(&expression, input).unwrap_err();

	assert_eq!(lowered.code, "FUNCTION_001");
	assert_eq!(lowered.fragment.text(), "no::such");
	assert_eq!(lowered, old);
}

#[test]
fn arity_mismatch_matches_the_old_path() {
	// Arity must be checked before the routine runs, otherwise it indexes past its arguments.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::abs", vec![]);

	let lowered = env.lowered(&expression, rows(2)).unwrap_err();
	let old = env.old(&expression, rows(2)).unwrap_err();

	assert_eq!(lowered.code, "FUNCTION_002");
	assert_eq!(lowered, old);
}

#[test]
fn all_none_args_match_the_old_path() {
	// An all-none argument must short-circuit to a typed none column, never reach the routine body.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::none_typed("a", ValueType::Int4, 3)]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone());
	let old = env.old(&expression, input);

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn a_routine_over_a_nullable_column_without_nones_declares_the_old_field() {
	// No none in the rows means no widening at evaluate, so the declared field alone must be nullable.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let (field, array) = factory::int4("a", [1, -2]);
	let input = batch(vec![(Arc::new(field.as_ref().clone().with_nullable(true)), array)]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone());
	let old = env.old(&expression, input);

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn seeded_rng_uuid_v4_sequence_matches_the_old_path() {
	// The routine must draw from the shared rng, never a copy, otherwise a seeded run repeats uuids.
	let expression = call("uuid::v4", vec![]);
	let mixed = Env::with_routines(registry(), RuntimeContext::testing(0, 42));
	let old_only = Env::with_routines(registry(), RuntimeContext::testing(0, 42));

	let mixed_first = mixed.lowered(&expression, rows(3)).unwrap();
	let mixed_second = mixed.old(&expression, rows(3)).unwrap();
	let old_first = old_only.old(&expression, rows(3)).unwrap();
	let old_second = old_only.old(&expression, rows(3)).unwrap();

	assert_eq!(mixed_first, old_first);
	assert_eq!(mixed_second, old_second);
}

#[test]
fn mock_clock_now_reads_the_mock_time() {
	// The routine must read the context clock, never the wall clock, otherwise a deterministic run drifts.
	let env = Env::with_routines(registry(), RuntimeContext::testing(1_700_000_000_000, 0));
	let expression = call("clock::now", vec![]);

	let lowered = env.lowered(&expression, rows(2)).unwrap();
	let old = env.old(&expression, rows(2)).unwrap();

	assert_eq!(lowered, old);
}

#[test]
fn is_type_with_a_type_argument_matches_the_old_path() {
	// A type-named argument must lower to a type literal, never to a lookup of a column that does not exist.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1, 2])]);
	let expression = call("is::type", vec![column("a"), column("int4")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(strings(&lowered), ["true", "true"]);
	assert_eq!(lowered, old);
}

#[test]
fn aggregate_only_routine_in_a_filter_is_unknown() {
	// A filter must resolve scalar routines only, so an aggregate-only name is unknown there, at its name.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1, 2])]);

	let lowered = env.lowered(&call("stats::digest", vec![column("a")]), input).unwrap_err();

	assert_eq!(lowered.code, "FUNCTION_001");
	assert_eq!(lowered.fragment.text(), "stats::digest");
}

#[test]
#[should_panic(expected = "the plan declared")]
fn wrong_return_type_trips_the_output_check() {
	// Output that disagrees with the declared return type must stop the query, never flow on mistyped.
	let routines = Routines::builder()
		.register_function(Arc::new(WrongReturnType {
			info: RoutineInfo::new("test::wrong_type"),
		}))
		.configure();
	let env = Env::with_routines(routines, RuntimeContext::testing(0, 0));

	let _ = env.lowered(&call("test::wrong_type", vec![]), rows(2));
}

#[test]
fn a_row_changing_routine_falls_back_and_keeps_its_rows() {
	// DataFusion refuses a routine that changes the row count, so such a routine must run on the old path.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("gen::series", vec![number("1"), number("3")]);
	let mut ctx = env.ctx(empty_batch());
	ctx.row_count = 1;

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx).unwrap();
	let old = compile_expression(
		&CompileContext {
			symbols: &env.symbols,
		},
		&expression,
	)
	.unwrap()
	.execute(&ctx)
	.unwrap();

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["1", "2", "3"]);
}

#[test]
fn an_all_none_call_into_a_non_propagating_routine_falls_back_with_the_old_type() {
	// clamp declares float8 for untyped nones but answers an untyped none, so its field must never be declared.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::clamp", vec![none(), none(), none()]);

	let lowered = env.lowered(&expression, rows(2)).unwrap();
	let old = env.old(&expression, rows(2)).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(from_field(&lowered.0).unwrap().value_type, None);
}

#[test]
fn a_fixed_type_routine_lowers_with_the_rest_of_its_expression() {
	// An exempt routine must not force a fallback, or the lowered between beside it errors as the old path does.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1, 2, 3]), factory::int4_optional("v", [Some(1), Some(5), None])]);
	let expression = and(
		call("is::type", vec![column("a"), column("int4")]),
		between(column("v"), number("2"), number("10")),
	);

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap_err();

	assert_eq!(strings(&lowered), vec!["false", "true", "none"]);
	assert_eq!(old.code, "OPERATOR_028");
}

fn numeric_samples() -> Vec<(Value, Value)> {
	samples()
		.into_iter()
		.filter(|(value, _)| {
			matches!(
				value.get_type(),
				ValueType::Int1
					| ValueType::Int2
					| ValueType::Int4
					| ValueType::Int8
					| ValueType::Int16
					| ValueType::Uint1
					| ValueType::Uint2
					| ValueType::Uint4
					| ValueType::Uint8
					| ValueType::Uint16
					| ValueType::Float4
					| ValueType::Float8
					| ValueType::Decimal { .. }
			)
		})
		.collect()
}

#[test]
fn every_two_argument_arith_routine_declares_the_kernel_field_for_every_numeric_pair() {
	// The plan field must equal the kernel's for nullable and plain columns, otherwise an outer node types a wrong
	// column.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let mut names = vec!["math::mod".to_string()];
	for op in ["add", "sub", "mul", "div", "rem"] {
		for variant in ["", "_none", "_saturate", "_wrap", "_zero"] {
			names.push(format!("math::{op}{variant}"));
		}
	}

	for left in numeric_samples() {
		for right in numeric_samples() {
			let nullable =
				batch(vec![nullable_column("l", left.clone()), nullable_column("r", right.clone())]);
			let plain = batch(vec![
				factory::from_many("l", left.0.clone(), 2),
				factory::from_many("r", right.1.clone(), 2),
			]);
			for name in &names {
				let expression = call(name, vec![column("l"), column("r")]);
				for input in [nullable.clone(), plain.clone()] {
					let lowered = env.lowered(&expression, input.clone());
					let old = env.old(&expression, input);
					assert_eq!(
						lowered,
						old,
						"{name}({:?}, {:?})",
						left.0.get_type(),
						right.0.get_type()
					);
				}
			}
		}
	}
}

#[test]
fn every_three_argument_arith_routine_declares_the_kernel_field_for_every_numeric_pair() {
	// The fallback column feeds the decimal digits and the none rule, so its field must count in the plan too.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));

	for left in numeric_samples() {
		for right in numeric_samples() {
			let input = batch(vec![
				nullable_column("l", left.clone()),
				nullable_column("r", right.clone()),
				nullable_column("d", right.clone()),
			]);
			for op in ["add", "sub", "mul", "div", "rem"] {
				for (name, third) in [
					(format!("math::{op}_default"), column("d")),
					(format!("math::{op}_strict"), text("overflow")),
				] {
					let expression = call(&name, vec![column("l"), column("r"), third]);
					let lowered = env.lowered(&expression, input.clone());
					let old = env.old(&expression, input.clone());
					assert_eq!(
						lowered,
						old,
						"{name}({:?}, {:?})",
						left.0.get_type(),
						right.0.get_type()
					);
				}
			}
		}
	}
}

#[test]
fn a_none_argument_reaches_an_outer_routine_as_an_option() {
	// meta::type reads its argument field, so a declared int4 where the kernel answers Option(int4) shows the wrong
	// type.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("meta::type", vec![call("math::mod", vec![cast(number("7"), ValueType::Int4), none()])]);

	let lowered = env.lowered(&expression, rows(1));
	let old = env.old(&expression, rows(1));

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn an_untyped_none_first_argument_takes_the_other_arguments_type() {
	// The kernel sees an untyped none as any and promotes to the other side, so the plan must not answer any.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::power", vec![none(), cast(number("3"), ValueType::Uint8)]);

	let lowered = env.lowered(&expression, rows(1));
	let old = env.old(&expression, rows(1));

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn a_none_argument_reaches_meta_type_through_every_other_override() {
	// An outer routine reads the declared field, so each override must declare an option when an argument holds a
	// none.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4_optional("a", [Some(2024), None])]);
	let calls = [
		call("math::power", vec![column("a"), number("2")]),
		call("math::clamp", vec![column("a"), number("0"), number("10")]),
		call("date::new", vec![column("a"), number("1"), number("1")]),
		call("stats::approx_percentile", vec![none(), number("0.5")]),
	];

	for inner in calls {
		let expression = call("meta::type", vec![inner]);
		let lowered = env.lowered(&expression, input.clone());
		let old = env.old(&expression, input.clone());
		assert!(old.is_ok(), "{old:?}");
		assert_eq!(lowered, old, "{expression:?}");
	}
}
