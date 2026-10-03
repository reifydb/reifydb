// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
	registry::Routines,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::value::value_type::ValueType;

use crate::common::{Env, batch, call, column, registry, rows, strings};

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
