// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_arith::boolean::{and_kleene, or_kleene};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::{
	error::CoreError,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_value::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
};

use crate::Result;

pub(crate) fn try_short_circuit_and(
	l: &ColumnWithName,
	fragment: &Fragment,
	row_count: usize,
) -> Option<ColumnWithName> {
	if is_all_false_defined(l.data()) {
		Some(ColumnWithName::new(fragment.clone(), ColumnBuffer::bool(vec![false; row_count])))
	} else {
		None
	}
}

pub(crate) fn try_short_circuit_or(
	l: &ColumnWithName,
	fragment: &Fragment,
	row_count: usize,
) -> Option<ColumnWithName> {
	if is_all_true_defined(l.data()) {
		Some(ColumnWithName::new(fragment.clone(), ColumnBuffer::bool(vec![true; row_count])))
	} else {
		None
	}
}

fn is_all_false_defined(buffer: &ColumnBuffer) -> bool {
	match buffer {
		ColumnBuffer::Bool(c) => buffer.none_count() == 0 && !c.is_empty() && !c.values().has_true(),
		_ => false,
	}
}

fn is_all_true_defined(buffer: &ColumnBuffer) -> bool {
	match buffer {
		ColumnBuffer::Bool(c) => buffer.none_count() == 0 && !c.is_empty() && !c.values().has_false(),
		_ => false,
	}
}

fn is_all_none(bv: Option<&BooleanBuffer>) -> bool {
	match bv {
		Some(bv) => !bv.has_true(),
		None => false,
	}
}

pub fn execute_logical_op(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: &Fragment,
	logical_op: LogicalOp,
) -> Result<ColumnWithName> {
	let (left_data, left_nulls) = left.data().clone().split_nulls();
	let (right_data, right_nulls) = right.data().clone().split_nulls();
	let left_bv = left_nulls.as_ref().map(NullBuffer::inner);
	let right_bv = right_nulls.as_ref().map(NullBuffer::inner);
	let len = left_data.len();

	let synthetic = BooleanBuffer::new_unset(len);

	let (l_v_bits, l_valid_bv) = match &left_data {
		ColumnBuffer::Bool(c) => (c.values(), left_bv),
		_ if is_all_none(left_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, &left_data, &right_data),
	};
	let (r_v_bits, r_valid_bv) = match &right_data {
		ColumnBuffer::Bool(c) => (c.values(), right_bv),
		_ if is_all_none(right_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, &left_data, &right_data),
	};

	let l = BooleanArray::new(l_v_bits.clone(), l_valid_bv.map(|bv| NullBuffer::new(bv.clone())));
	let r = BooleanArray::new(r_v_bits.clone(), r_valid_bv.map(|bv| NullBuffer::new(bv.clone())));

	let result = match logical_op {
		LogicalOp::And => and_kleene(&l, &r).map_err(|err| CoreError::FrameError {
			message: err.to_string(),
		})?,
		LogicalOp::Or => or_kleene(&l, &r).map_err(|err| CoreError::FrameError {
			message: err.to_string(),
		})?,
		LogicalOp::Xor => {
			if l.len() != r.len() {
				return Err(CoreError::FrameError {
					message: format!(
						"Cannot perform bitwise operation on arrays of different length: {} != {}",
						l.len(),
						r.len()
					),
				}
				.into());
			}
			let nulls = match (l.nulls(), r.nulls()) {
				(None, None) => None,
				(Some(n), None) | (None, Some(n)) => Some(n.clone()),
				(Some(a), Some(b)) => Some(NullBuffer::new(a.inner() & b.inner())),
			};
			BooleanArray::new(l.values() ^ r.values(), nulls)
		}
		LogicalOp::Not => unreachable!("NOT is unary; not handled by execute_logical_op"),
	};

	Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::Bool(result)))
}

fn type_error(
	logical_op: &LogicalOp,
	fragment: &Fragment,
	left: &ColumnBuffer,
	right: &ColumnBuffer,
) -> Result<ColumnWithName> {
	let category = if left.is_number() || right.is_number() {
		OperandCategory::Number
	} else if left.is_text() || right.is_text() {
		OperandCategory::Text
	} else if left.is_temporal() || right.is_temporal() {
		OperandCategory::Temporal
	} else if left.is_uuid() || right.is_uuid() {
		OperandCategory::Uuid
	} else {
		unimplemented!("{} {:?} {}", left.get_type(), logical_op, right.get_type());
	};
	Err(TypeError::LogicalOperatorNotApplicable {
		operator: logical_op.clone(),
		operand_category: category,
		fragment: fragment.clone(),
	}
	.into())
}
