// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::{Value, container::vector::VectorContainer, value_type::ValueType};

use crate::{
	function::support::coerce::{CoercePolicy, coerce_column},
	routine::{context::FunctionContext, error::RoutineError},
};

const UNSPECIFIED_DIMS: u32 = 0;

fn list_dims(data: &ColumnBuffer) -> Option<usize> {
	let ColumnBuffer::Any(container) = data else {
		return None;
	};

	for idx in 0..container.len() {
		if !container.is_defined(idx) {
			continue;
		}

		let mut value = container.get_value(idx);
		while let Value::Any(inner) = value {
			value = *inner;
		}
		if let Value::List(items) = value {
			return Some(items.len());
		}
	}
	None
}

fn empty_vector(ctx: &FunctionContext, argument_index: usize) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason: format!("argument {} is an empty list, which has no vector dimension", argument_index + 1),
	}
}

fn ensure_arity(ctx: &FunctionContext, args: &Columns, expected: usize) -> Result<(), RoutineError> {
	if args.len() != expected {
		return Err(RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected,
			actual: args.len(),
		});
	}
	Ok(())
}

fn invalid_argument(
	ctx: &FunctionContext,
	argument_index: usize,
	expected_dims: u32,
	actual: ValueType,
) -> RoutineError {
	RoutineError::FunctionInvalidArgumentType {
		function: ctx.fragment.clone(),
		argument_index,
		expected: vec![ValueType::Vector(expected_dims)],
		actual,
	}
}

fn container(
	ctx: &FunctionContext,
	data: &ColumnBuffer,
	dims: u32,
	argument_index: usize,
) -> Result<(VectorContainer, Vec<bool>), RoutineError> {
	if let ColumnBuffer::Vector(inner) = data {
		if inner.dims() != dims {
			return Err(invalid_argument(ctx, argument_index, dims, ValueType::Vector(inner.dims())));
		}
		let rows = inner.len();
		return Ok((inner.clone(), vec![true; rows]));
	}

	let coerced = match data {
		ColumnBuffer::Any(_)
		| ColumnBuffer::Blob {
			..
		} => coerce_column(ctx, data, ValueType::Vector(dims), CoercePolicy::Error)?,
		other => return Err(invalid_argument(ctx, argument_index, dims, other.get_type())),
	};

	let (inner, bitvec) = coerced.into_unwrap_option();
	match inner {
		ColumnBuffer::Vector(inner) => {
			let rows = inner.len();
			let defined = match bitvec {
				Some(bv) => (0..rows).map(|i| bv.get(i)).collect(),
				None => vec![true; rows],
			};
			Ok((inner, defined))
		}
		other => Err(invalid_argument(ctx, argument_index, dims, other.get_type())),
	}
}

pub(crate) fn prepare_single(
	ctx: &FunctionContext,
	args: &Columns,
) -> Result<(VectorContainer, Vec<bool>), RoutineError> {
	ensure_arity(ctx, args, 1)?;

	let (data, bitvec) = args[0].unwrap_option();
	let dims = match data {
		ColumnBuffer::Vector(inner) => inner.dims(),
		other => match list_dims(other) {
			Some(0) => return Err(empty_vector(ctx, 0)),
			Some(dims) => dims as u32,
			None => return Err(invalid_argument(ctx, 0, UNSPECIFIED_DIMS, other.get_type())),
		},
	};

	let (inner, mut defined) = container(ctx, data, dims, 0)?;
	if let Some(bv) = bitvec {
		for (i, slot) in defined.iter_mut().enumerate() {
			*slot = *slot && bv.get(i);
		}
	}
	Ok((inner, defined))
}

pub(crate) fn prepare_pair(
	ctx: &FunctionContext,
	args: &Columns,
) -> Result<(VectorContainer, VectorContainer, Vec<bool>), RoutineError> {
	ensure_arity(ctx, args, 2)?;

	let (left_data, left_bitvec) = args[0].unwrap_option();
	let (right_data, right_bitvec) = args[1].unwrap_option();

	let dims = match (left_data, right_data) {
		(ColumnBuffer::Vector(left), _) => left.dims(),
		(_, ColumnBuffer::Vector(right)) => right.dims(),
		(left, right) => match list_dims(left).or_else(|| list_dims(right)) {
			Some(0) => return Err(empty_vector(ctx, 0)),
			Some(dims) => dims as u32,
			None => return Err(invalid_argument(ctx, 0, UNSPECIFIED_DIMS, left.get_type())),
		},
	};

	let (left, left_defined) = container(ctx, left_data, dims, 0)?;
	let (right, right_defined) = container(ctx, right_data, dims, 1)?;

	if left.len() != right.len() {
		return Err(RoutineError::FunctionExecutionFailed {
			function: ctx.fragment.clone(),
			reason: format!("argument row counts differ: {} and {}", left.len(), right.len()),
		});
	}

	let rows = left.len();
	let mut defined = Vec::with_capacity(rows);
	for i in 0..rows {
		defined.push(left_defined[i]
			&& right_defined[i] && left_bitvec.is_none_or(|bv| bv.get(i))
			&& right_bitvec.is_none_or(|bv| bv.get(i)));
	}

	Ok((left, right, defined))
}
