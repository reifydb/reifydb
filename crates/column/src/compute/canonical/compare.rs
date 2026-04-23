// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::cmp::{Ordering, Ordering::*};

use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer};
use reifydb_type::{
	Result,
	value::{Value, r#type::Type},
};

use crate::compute::CompareOp;

// Produce a boolean canonical array where each row is `true` iff
// `array[row] <op> rhs`. None values in the input propagate to None in the
// output (RQL three-valued logic). The output buffer is a `ColumnBuffer::Bool`
// and the `ty` is `Type::Boolean`.
pub fn compare(array: &Canonical, rhs: &Value, op: CompareOp) -> Result<Canonical> {
	let len = array.len();
	let mut out = Vec::with_capacity(len);
	for i in 0..len {
		let lhs = array.buffer.get_value(i);
		let ord = cmp_values(&lhs, rhs);
		out.push(apply_cmp_order(op, ord));
	}
	let new_buffer = ColumnBuffer::bool(out);
	let new_nones = array.nones.clone();
	Ok(Canonical::new(Type::Boolean, array.nullable, new_nones, new_buffer))
}

fn cmp_values(lhs: &Value, rhs: &Value) -> Ordering {
	lhs.partial_cmp(rhs).unwrap_or(Less)
}

fn apply_cmp_order(op: CompareOp, order: Ordering) -> bool {
	match op {
		CompareOp::Eq => matches!(order, Equal),
		CompareOp::Ne => !matches!(order, Equal),
		CompareOp::Lt => matches!(order, Less),
		CompareOp::LtEq => matches!(order, Less | Equal),
		CompareOp::Gt => matches!(order, Greater),
		CompareOp::GtEq => matches!(order, Greater | Equal),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn compare_int4_equality() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 20, 40]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Eq).unwrap();
		assert_eq!(out.buffer.get_value(0), Value::Boolean(false));
		assert_eq!(out.buffer.get_value(1), Value::Boolean(true));
		assert_eq!(out.buffer.get_value(2), Value::Boolean(false));
		assert_eq!(out.buffer.get_value(3), Value::Boolean(true));
		assert_eq!(out.buffer.get_value(4), Value::Boolean(false));
	}

	#[test]
	fn compare_int4_greater_than() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Gt).unwrap();
		assert_eq!(out.buffer.get_value(0), Value::Boolean(false));
		assert_eq!(out.buffer.get_value(1), Value::Boolean(false));
		assert_eq!(out.buffer.get_value(2), Value::Boolean(true));
		assert_eq!(out.buffer.get_value(3), Value::Boolean(true));
	}
}
