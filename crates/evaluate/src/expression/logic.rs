// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, or_kleene};
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::CoreError,
	value::column::{factory, nulls::split_nulls},
};
use reifydb_value::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		value_type::{
			ValueType,
			field::{FieldType, named},
		},
	},
};

use crate::Result;

pub(crate) fn bool_column(name: &str, values: BooleanArray, nullable: bool) -> (FieldRef, ArrayRef) {
	let ty = match nullable || values.null_count() > 0 {
		true => ValueType::Option(Box::new(ValueType::Boolean)),
		false => ValueType::Boolean,
	};
	named(name, FieldType::from(ty), Arc::new(values))
}

pub(crate) fn try_short_circuit_and(
	l: &(FieldRef, ArrayRef),
	fragment: &Fragment,
	row_count: usize,
) -> Result<Option<(FieldRef, ArrayRef)>> {
	if is_all_false_defined(&ColumnView::try_from(l)?) {
		Ok(Some(factory::bool(fragment.text(), vec![false; row_count])))
	} else {
		Ok(None)
	}
}

pub(crate) fn try_short_circuit_or(
	l: &(FieldRef, ArrayRef),
	fragment: &Fragment,
	row_count: usize,
) -> Result<Option<(FieldRef, ArrayRef)>> {
	if is_all_true_defined(&ColumnView::try_from(l)?) {
		Ok(Some(factory::bool(fragment.text(), vec![true; row_count])))
	} else {
		Ok(None)
	}
}

fn is_all_false_defined(view: &ColumnView) -> bool {
	match &view.data {
		ViewData::Bool(c) => view.none_count() == 0 && !c.is_empty() && !c.values().has_true(),
		_ => false,
	}
}

fn is_all_true_defined(view: &ColumnView) -> bool {
	match &view.data {
		ViewData::Bool(c) => view.none_count() == 0 && !c.is_empty() && !c.values().has_false(),
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
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: &Fragment,
	logical_op: LogicalOp,
) -> Result<(FieldRef, ArrayRef)> {
	let (left_data, left_nulls) = split_nulls(left.clone())?;
	let (right_data, right_nulls) = split_nulls(right.clone())?;
	let left_bv = left_nulls.as_ref().map(NullBuffer::inner);
	let right_bv = right_nulls.as_ref().map(NullBuffer::inner);
	let len = left_data.1.len();

	let synthetic = BooleanBuffer::new_unset(len);

	let (left_view, right_view) = (ColumnView::try_from(&left_data)?, ColumnView::try_from(&right_data)?);
	let (l_v_bits, l_valid_bv) = match &left_view.data {
		ViewData::Bool(c) => (c.values(), left_bv),
		_ if is_all_none(left_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, &left_view, &right_view),
	};
	let (r_v_bits, r_valid_bv) = match &right_view.data {
		ViewData::Bool(c) => (c.values(), right_bv),
		_ if is_all_none(right_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, &left_view, &right_view),
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
			let nulls = match (l.logical_nulls(), r.logical_nulls()) {
				(None, None) => None,
				(Some(n), None) | (None, Some(n)) => Some(n),
				(Some(a), Some(b)) => Some(NullBuffer::new(a.inner() & b.inner())),
			};
			BooleanArray::new(l.values() ^ r.values(), nulls)
		}
		LogicalOp::Not => unreachable!("NOT is unary; not handled by execute_logical_op"),
	};

	Ok(bool_column(fragment.text(), result, left.0.is_nullable() || right.0.is_nullable()))
}

fn type_error(
	logical_op: &LogicalOp,
	fragment: &Fragment,
	left: &ColumnView,
	right: &ColumnView,
) -> Result<(FieldRef, ArrayRef)> {
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
