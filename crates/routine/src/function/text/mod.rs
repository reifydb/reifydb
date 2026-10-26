// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, BooleanArray, Datum, LargeStringArray, Scalar};
use arrow_schema::ArrowError;
use reifydb_routine_abi::{broadcast::is_constant, context::FunctionContext, error::RoutineError};

pub mod ascii;
pub mod char;
pub mod concat;
pub mod contains;
pub mod count;
pub mod ends_with;
pub mod format_bytes;
pub mod format_bytes_si;
pub mod index_of;
pub mod length;
pub mod lower;
pub mod pad_left;
pub mod pad_right;
pub mod repeat;
pub mod replace;
pub mod reverse;
pub mod starts_with;
pub mod substring;
pub mod trim;
pub mod trim_end;
pub mod trim_start;
pub mod upper;

pub(crate) fn text_predicate(
	ctx: &FunctionContext,
	left: &LargeStringArray,
	right: &LargeStringArray,
	kernel: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
) -> Result<BooleanArray, RoutineError> {
	let row_count = ctx.row_count;
	let left_constant = is_constant(left.len(), row_count);
	let right_constant = is_constant(right.len(), row_count);
	let result = match (left_constant, right_constant) {
		(true, false) => kernel(&Scalar::new(left), right),
		(false, true) => kernel(left, &Scalar::new(right)),
		_ => kernel(left, right),
	}
	.map_err(|err| RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason: err.to_string(),
	})?;
	if left_constant && right_constant {
		return Ok((0..row_count).map(|_| result.is_valid(0).then(|| result.value(0))).collect());
	}
	Ok(result)
}
