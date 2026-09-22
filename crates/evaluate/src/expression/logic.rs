// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_value::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
};

use super::option::apply_option_bitvec;
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
		ColumnBuffer::Bool(c) => !c.is_empty() && !c.values().has_true(),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => {
			if !!bitvec.has_false() {
				return false;
			}
			matches!(inner.as_ref(), ColumnBuffer::Bool(c) if !c.is_empty() && !c.values().has_true())
		}
		_ => false,
	}
}

fn is_all_true_defined(buffer: &ColumnBuffer) -> bool {
	match buffer {
		ColumnBuffer::Bool(c) => !c.is_empty() && !c.values().has_false(),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => {
			if !!bitvec.has_false() {
				return false;
			}
			matches!(inner.as_ref(), ColumnBuffer::Bool(c) if !c.is_empty() && !c.values().has_false())
		}
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
	bool_fn: fn(bool, bool) -> bool,
) -> Result<ColumnWithName> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();
	let len = left_data.len();

	let synthetic = BooleanBuffer::new_unset(len);

	let (l_v_bits, l_valid_bv) = match left_data {
		ColumnBuffer::Bool(c) => (c.values(), left_bv),
		_ if is_all_none(left_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, left_data, right_data),
	};
	let (r_v_bits, r_valid_bv) = match right_data {
		ColumnBuffer::Bool(c) => (c.values(), right_bv),
		_ if is_all_none(right_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, left_data, right_data),
	};

	let value_data: Vec<bool> =
		l_v_bits.iter().zip(r_v_bits.iter()).map(|(l_val, r_val)| bool_fn(l_val, r_val)).collect();
	let value_buffer = ColumnBuffer::bool(value_data);

	let result_bv = compute_kleene_validity(&logical_op, l_valid_bv, r_valid_bv, l_v_bits, r_v_bits, len);

	let final_buffer = match result_bv {
		Some(bv) => apply_option_bitvec(value_buffer, bv),
		None => value_buffer,
	};

	Ok(ColumnWithName::new(fragment.clone(), final_buffer))
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

fn compute_kleene_validity(
	logical_op: &LogicalOp,
	left_bv: Option<&BooleanBuffer>,
	right_bv: Option<&BooleanBuffer>,
	l_data: &BooleanBuffer,
	r_data: &BooleanBuffer,
	len: usize,
) -> Option<BooleanBuffer> {
	if left_bv.is_none() && right_bv.is_none() {
		return None;
	}
	let bv = BooleanBuffer::collect_bool(len, |i| {
		let l_valid = left_bv.is_none_or(|bv| bv.value(i));
		let r_valid = right_bv.is_none_or(|bv| bv.value(i));
		let l_v = l_data.value(i);
		let r_v = r_data.value(i);
		let both_valid = l_valid && r_valid;
		let l_false = l_valid && !l_v;
		let r_false = r_valid && !r_v;
		let l_true = l_valid && l_v;
		let r_true = r_valid && r_v;
		match logical_op {
			LogicalOp::And => both_valid || l_false || r_false,
			LogicalOp::Or => both_valid || l_true || r_true,
			LogicalOp::Xor => both_valid,
			LogicalOp::Not => unreachable!("NOT is unary; not handled by execute_logical_op"),
		}
	});
	Some(bv)
}
