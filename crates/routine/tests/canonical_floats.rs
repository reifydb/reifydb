// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, LazyLock};

use arrow_array::{Array, ArrayRef, Float64Array, cast::AsArray, types::Float64Type};
use arrow_schema::{DataType, Field, FieldRef};
use reifydb_routine::function::math::{sin::Sin, sqrt::Sqrt};
use reifydb_routine_abi::{Function, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(function: impl Function, args: Vec<(FieldRef, ArrayRef)>) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let row_count = args.first().map_or(0, |(_, array)| array.len());
	function.call(&mut ctx(row_count), &args)
}

fn raw_float8(values: Vec<f64>) -> (FieldRef, ArrayRef) {
	// The factory would turn a -0.0 input into 0.0, so the input is built raw to reach the routine as -0.0.
	(Arc::new(Field::new("arg0", DataType::Float64, false)), Arc::new(Float64Array::from(values)))
}

fn f64_bits(array: &ArrayRef) -> Vec<u64> {
	array.as_primitive::<Float64Type>().values().iter().map(|v| v.to_bits()).collect()
}

#[test]
fn sin_of_negative_zero_gives_positive_zero() {
	// IEEE sin keeps the sign of -0.0, so the routine output must canonicalize or compare splits zero.
	let (_, out) = call(Sin::new(), vec![raw_float8(vec![-0.0])]).unwrap();
	assert_eq!(out.null_count(), 0);
	assert_eq!(f64_bits(&out), vec![0.0f64.to_bits()]);
}

#[test]
fn sqrt_of_negative_one_gives_the_canonical_nan() {
	// The CPU picks the sign of the NaN from sqrt of a negative, so the routine output must still be the one NaN.
	let (_, out) = call(Sqrt::new(), vec![raw_float8(vec![-1.0])]).unwrap();
	assert_eq!(out.null_count(), 0);
	assert_eq!(f64_bits(&out), vec![f64::NAN.to_bits()]);
}
