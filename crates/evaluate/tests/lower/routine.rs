// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::{batch::empty_batch, column::factory};
use reifydb_evaluate::lower::LoweredExpr;
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
	common::{
		Env, Frozen, and, batch, between, call, cast, code_of, column, compare, none, number, registry, rows,
		strings, text, values_of,
	},
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
	let mut frozen = Frozen::inline(&[
		"FUNCTION_004 \"math::abs\" Function math::abs argument 1 has invalid type: expected one of [Int1, Int2, Int4, Int8, Int16, Uint1, Uint2, Uint4, Uint8, Uint16, Float4, Float8, Decimal], got Utf8 []",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::utf8("a", ["x"])]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = frozen.old(&env, &expression, input).unwrap_err();

	assert_eq!(lowered.fragment.text(), "math::abs");
	assert_eq!(lowered, old);
}

#[test]
fn unknown_function_matches_the_old_path() {
	// An unknown name must fail with the same code and name fragment on both paths, otherwise a typo reads
	// differently.
	let mut frozen = Frozen::inline(&["FUNCTION_001 \"no::such\" Unknown function: no::such []"]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1])]);
	let expression = call("no::such", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = frozen.old(&env, &expression, input).unwrap_err();

	assert_eq!(lowered.code, "FUNCTION_001");
	assert_eq!(lowered.fragment.text(), "no::such");
	assert_eq!(lowered, old);
}

#[test]
fn arity_mismatch_matches_the_old_path() {
	// Arity must be checked before the routine runs, otherwise it indexes past its arguments.
	let mut frozen =
		Frozen::inline(&["FUNCTION_002 \"math::abs\" Function math::abs expects 1 arguments, got 0 []"]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::abs", vec![]);

	let lowered = env.lowered(&expression, rows(2)).unwrap_err();
	let old = frozen.old(&env, &expression, rows(2)).unwrap_err();

	assert_eq!(lowered.code, "FUNCTION_002");
	assert_eq!(lowered, old);
}

#[test]
fn all_none_args_match_the_old_path() {
	// An all-none argument must short-circuit to a typed none column, never reach the routine body.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"math::abs(a)\", data_type: Int32, nullable: true } [\"none\", \"none\", \"none\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::none_typed("a", ValueType::Int4, 3)]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone());
	let old = frozen.old(&env, &expression, input);

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn a_routine_over_a_nullable_column_without_nones_declares_the_old_field() {
	// No none in the rows means no widening at evaluate, so the declared field alone must be nullable.
	let mut frozen =
		Frozen::inline(&["Field { name: \"math::abs(a)\", data_type: Int32, nullable: true } [\"1\", \"2\"]"]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let (field, array) = factory::int4("a", [1, -2]);
	let input = batch(vec![(Arc::new(field.as_ref().clone().with_nullable(true)), array)]);
	let expression = call("math::abs", vec![column("a")]);

	let lowered = env.lowered(&expression, input.clone());
	let old = frozen.old(&env, &expression, input);

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn seeded_rng_uuid_v4_sequence_matches_the_old_path() {
	// The routine must draw from the shared rng, never a copy, otherwise a seeded run repeats uuids.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"uuid::v4()\", data_type: FixedSizeBinary(16), metadata: {\"ARROW:extension:name\": \"reifydb.uuid4\"} } [\"fb780859-e8d8-47bc-b7b7-8e2f9b8d68d9\", \"5e831ca1-477e-4b21-9e3a-ba7a1f21d500\", \"37ae6bd2-3910-41ee-89ac-4e992e019381\"]",
		"Field { name: \"uuid::v4()\", data_type: FixedSizeBinary(16), metadata: {\"ARROW:extension:name\": \"reifydb.uuid4\"} } [\"a2242722-6377-4c86-bd51-ad3f130af08a\", \"d13451de-7160-4fa2-b230-76fd782de967\", \"ea9f11f8-dfb0-4a08-a881-0f9ea39c3a6a\"]",
		"Field { name: \"uuid::v4()\", data_type: FixedSizeBinary(16), metadata: {\"ARROW:extension:name\": \"reifydb.uuid4\"} } [\"fb780859-e8d8-47bc-b7b7-8e2f9b8d68d9\", \"5e831ca1-477e-4b21-9e3a-ba7a1f21d500\", \"37ae6bd2-3910-41ee-89ac-4e992e019381\"]",
	]);
	let expression = call("uuid::v4", vec![]);
	let mixed = Env::with_routines(registry(), RuntimeContext::testing(0, 42));
	let old_only = Env::with_routines(registry(), RuntimeContext::testing(0, 42));

	let mixed_first = mixed.lowered(&expression, rows(3)).unwrap();
	let mixed_second = frozen.old(&mixed, &expression, rows(3)).unwrap();
	let old_first = frozen.old(&old_only, &expression, rows(3)).unwrap();
	let old_second = frozen.old(&old_only, &expression, rows(3)).unwrap();

	assert_eq!(mixed_first, old_first);
	assert_eq!(mixed_second, old_second);
}

#[test]
fn mock_clock_now_reads_the_mock_time() {
	// The routine must read the context clock, never the wall clock, otherwise a deterministic run drifts.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"clock::now()\", data_type: Int64 } [\"1700000000000\", \"1700000000000\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(1_700_000_000_000, 0));
	let expression = call("clock::now", vec![]);

	let lowered = env.lowered(&expression, rows(2)).unwrap();
	let old = frozen.old(&env, &expression, rows(2)).unwrap();

	assert_eq!(lowered, old);
}

#[test]
fn is_type_with_a_type_argument_matches_the_old_path() {
	// A type-named argument must lower to a type literal, never to a lookup of a column that does not exist.
	let mut frozen =
		Frozen::inline(&["Field { name: \"is::type(a, int4)\", data_type: Boolean } [\"true\", \"true\"]"]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1, 2])]);
	let expression = call("is::type", vec![column("a"), column("int4")]);

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = frozen.old(&env, &expression, input).unwrap();

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
	// DataFusion refuses a routine that changes the row count, so it must run directly and keep its rows.
	let mut frozen =
		Frozen::inline(&["Field { name: \"gen::series(1, 3)\", data_type: Int32 } [\"1\", \"2\", \"3\"]"]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("gen::series", vec![number("1"), number("3")]);
	let mut ctx = env.ctx(empty_batch());
	ctx.row_count = 1;

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx).unwrap();
	let old = frozen.answer(LoweredExpr::new(expression.clone(), "test").evaluate(&ctx)).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["1", "2", "3"]);
}

#[test]
fn an_all_none_call_into_a_non_propagating_routine_declares_the_old_type() {
	// clamp declares float8 for untyped nones but answers an untyped none, so its field must never be declared.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"math::clamp(none, none, none)\", data_type: Null, nullable: true } [\"none\", \"none\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::clamp", vec![none(), none(), none()]);

	let lowered = env.lowered(&expression, rows(2)).unwrap();
	let old = frozen.old(&env, &expression, rows(2)).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(from_field(&lowered.0).unwrap().value_type, None);
}

#[test]
fn a_fixed_type_routine_lowers_with_the_rest_of_its_expression() {
	// A fixed-type routine must keep the between beside it answering per row over a nullable column.
	let mut frozen = Frozen::inline(&[
		"OPERATOR_028 \"between\" Cannot apply 'BETWEEN' operator to Option(Int4) with range of Int1 [\"Value is of type: Option(Int4)\", \"Range bounds are of type: Int1\", \"BETWEEN comparison is only supported between compatible types\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4("a", [1, 2, 3]), factory::int4_optional("v", [Some(1), Some(5), None])]);
	let expression = and(
		call("is::type", vec![column("a"), column("int4")]),
		between(column("v"), number("2"), number("10")),
	);

	let lowered = env.lowered(&expression, input).unwrap();

	assert_eq!(strings(&lowered), vec!["false", "true", "none"]);
	assert_eq!(code_of(&frozen.recorded()), "OPERATOR_028");
}

fn numeric_samples() -> Vec<(Value, Value)> {
	samples()
		.into_iter()
		.filter(|(value, _)| {
			matches!(
				value.get_type(),
				ValueType::Int1
					| ValueType::Int2 | ValueType::Int4 | ValueType::Int8
					| ValueType::Int16 | ValueType::Uint1
					| ValueType::Uint2 | ValueType::Uint4
					| ValueType::Uint8 | ValueType::Uint16
					| ValueType::Float4 | ValueType::Float8
					| ValueType::Decimal { .. }
			)
		})
		.collect()
}

#[test]
fn every_two_argument_arith_routine_declares_the_kernel_field_for_every_numeric_pair() {
	// The plan field must equal the kernel's for nullable and plain columns, otherwise an outer node types a wrong
	// column.
	let mut frozen =
		Frozen::load("every_two_argument_arith_routine_declares_the_kernel_field_for_every_numeric_pair");
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
					let old = frozen.old(&env, &expression, input);
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
	let mut frozen =
		Frozen::load("every_three_argument_arith_routine_declares_the_kernel_field_for_every_numeric_pair");
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
					let old = frozen.old(&env, &expression, input.clone());
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
	let mut frozen = Frozen::inline(&[
		"Field { name: \"meta::type(math::mod(cast(7, type), none))\", data_type: LargeUtf8 } [\"Option(Int4)\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("meta::type", vec![call("math::mod", vec![cast(number("7"), ValueType::Int4), none()])]);

	let lowered = env.lowered(&expression, rows(1));
	let old = frozen.old(&env, &expression, rows(1));

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn an_untyped_none_first_argument_takes_the_other_arguments_type() {
	// The kernel sees an untyped none as any and promotes to the other side, so the plan must not answer any.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"math::power(none, cast(3, type))\", data_type: UInt64, nullable: true } [\"none\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("math::power", vec![none(), cast(number("3"), ValueType::Uint8)]);

	let lowered = env.lowered(&expression, rows(1));
	let old = frozen.old(&env, &expression, rows(1));

	assert!(old.is_ok(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn a_none_argument_reaches_meta_type_through_every_other_override() {
	// An outer routine reads the declared field, so each override must declare an option when an argument holds a
	// none.
	let mut frozen = Frozen::load("a_none_argument_reaches_meta_type_through_every_other_override");
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
		let old = frozen.old(&env, &expression, input.clone());
		assert!(old.is_ok(), "{old:?}");
		assert_eq!(lowered, old, "{expression:?}");
	}
}

#[test]
fn a_non_propagating_routine_reports_the_same_type_with_and_without_a_none_in_the_column() {
	// The column type, not the rows, must decide the declared type; otherwise one inserted none changes meta::type.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"meta::type(math::add_none(a, a))\", data_type: LargeUtf8 } [\"Int2\", \"Int2\", \"Int2\"]",
	]);
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = call("meta::type", vec![call("math::add_none", vec![column("a"), column("a")])]);
	let (field, array) = factory::from_many("a", Value::Int1(1), 3);
	let without_none = batch(vec![(Arc::new(field.as_ref().clone().with_nullable(true)), array)]);
	let with_none = batch(vec![nullable_column("a", (Value::Int1(1), Value::Int1(2)))]);

	let without = strings(&env.lowered(&expression, without_none).unwrap());
	let with = strings(&env.lowered(&expression, with_none).unwrap());

	assert_eq!(without, with);
	assert_ne!(values_of(&frozen.recorded()), without);
}

#[test]
fn a_row_changing_routine_inside_a_compare_lines_up_with_a_batch_of_its_length() {
	// An assert over a one-value series compares one row with one row, so the nested series must not be refused.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = compare("==", call("gen::series", vec![number("1"), number("1")]), number("1"));

	let lowered = env.lowered(&expression, rows(1)).unwrap();

	assert_eq!(strings(&lowered), vec!["true"]);
}

#[test]
fn a_row_changing_routine_of_another_length_than_the_batch_errors_instead_of_misaligning() {
	// A series of three rows beside a batch of one would pair the wrong rows, so it must fail with the length.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let expression = compare("==", call("gen::series", vec![number("1"), number("3")]), number("1"));

	let err = env.lowered(&expression, rows(1)).unwrap_err();

	assert_eq!(err.code, "RUNTIME_014", "{err:?}");
}
