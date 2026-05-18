// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_type::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	util::bitvec::BitVec,
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
		ColumnBuffer::Bool(c) => !c.is_empty() && c.data().none(),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => {
			if !bitvec.all_ones() {
				return false;
			}
			matches!(inner.as_ref(), ColumnBuffer::Bool(c) if !c.is_empty() && c.data().none())
		}
		_ => false,
	}
}

fn is_all_true_defined(buffer: &ColumnBuffer) -> bool {
	match buffer {
		ColumnBuffer::Bool(c) => !c.is_empty() && c.data().all_ones(),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => {
			if !bitvec.all_ones() {
				return false;
			}
			matches!(inner.as_ref(), ColumnBuffer::Bool(c) if !c.is_empty() && c.data().all_ones())
		}
		_ => false,
	}
}

fn is_all_none(bv: Option<&BitVec>) -> bool {
	match bv {
		Some(bv) => bv.count_ones() == 0,
		None => false,
	}
}

pub(crate) fn execute_logical_op(
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: &Fragment,
	logical_op: LogicalOp,
	bool_fn: fn(bool, bool) -> bool,
) -> Result<ColumnWithName> {
	let (left_data, left_bv) = left.data().unwrap_option();
	let (right_data, right_bv) = right.data().unwrap_option();
	let len = left_data.len();

	let synthetic = BitVec::repeat(len, false);

	let (l_v_bits, l_valid_bv) = match left_data {
		ColumnBuffer::Bool(c) => (c.data(), left_bv),
		_ if is_all_none(left_bv) => (&synthetic, Some(&synthetic)),
		_ => return type_error(&logical_op, fragment, left_data, right_data),
	};
	let (r_v_bits, r_valid_bv) = match right_data {
		ColumnBuffer::Bool(c) => (c.data(), right_bv),
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
	left_bv: Option<&BitVec>,
	right_bv: Option<&BitVec>,
	l_data: &BitVec,
	r_data: &BitVec,
	len: usize,
) -> Option<BitVec> {
	if left_bv.is_none() && right_bv.is_none() {
		return None;
	}
	let bv = BitVec::from_fn(len, |i| {
		let l_valid = left_bv.is_none_or(|bv| bv.get(i));
		let r_valid = right_bv.is_none_or(|bv| bv.get(i));
		let l_v = l_data.get(i);
		let r_v = r_data.get(i);
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
