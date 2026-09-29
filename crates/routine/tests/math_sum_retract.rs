// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{int1, int8, uint1, uint4},
	view::group_by::GroupId,
};
use reifydb_routine::function::math::sum::Sum;
use reifydb_routine_abi::{Accumulator, Function, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, column_view::ColumnView, identity::IdentityId},
};

fn ctx() -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count: 0,
		runtime_context: &RUNTIME,
	}
}

fn columns(column: (FieldRef, ArrayRef)) -> Vec<(FieldRef, ArrayRef)> {
	vec![column]
}

fn accumulator() -> Box<dyn Accumulator> {
	Sum::new().accumulator(&mut ctx(), &[]).unwrap().expect("sum is an aggregate")
}

fn rows(count: usize) -> Vec<(GroupId, Vec<usize>)> {
	vec![(GroupId(0), (0..count).collect())]
}

fn assert_out_of_range(result: Result<(), RoutineError>) {
	match result {
		Err(RoutineError::Wrapped(err)) => assert_eq!(err.code, "NUMBER_002"),
		other => panic!("expected NUMBER_002, got {other:?}"),
	}
}

#[test]
fn retracting_more_than_a_uint_sum_holds_is_an_error() {
	// Unchecked, 1 - 2 panics in debug and wraps to u32::MAX in release.
	let mut acc = accumulator();
	acc.update(&columns(uint4("v", [1])), &rows(1)).unwrap();
	assert_out_of_range(acc.retract(&columns(uint4("v", [2])), &rows(1)));
}

#[test]
fn retracting_below_the_signed_minimum_is_an_error() {
	// Unchecked, i8::MIN - 1 wraps to i8::MAX in release.
	let mut acc = accumulator();
	acc.update(&columns(int1("v", [i8::MIN])), &rows(1)).unwrap();
	assert_out_of_range(acc.retract(&columns(int1("v", [1])), &rows(1)));
}

#[test]
fn a_retract_batch_whose_delta_overflows_is_an_error() {
	// The rows of one retract batch are summed before the subtraction, so that sum must be checked too.
	let mut acc = accumulator();
	acc.update(&columns(uint1("v", [200])), &rows(1)).unwrap();
	assert_out_of_range(acc.retract(&columns(uint1("v", [200, 200])), &rows(2)));
}

#[test]
fn a_retract_within_range_still_subtracts() {
	// The checks must not reject a retract that leaves a valid sum, including one at the type's edge.
	let mut acc = accumulator();
	acc.update(&columns(int8("v", [i64::MAX, -5])), &rows(2)).unwrap();
	acc.retract(&columns(int8("v", [-5])), &rows(1)).unwrap();
	let (_, out) = acc.finalize().unwrap();
	assert_eq!(ColumnView::try_from(&out).unwrap().get_value(0), Value::Int8(i64::MAX));
}
