// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::{Ordering, Ordering::*},
	sync::Arc,
};

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{data::canonical::Canonical, nulls::split_nulls};
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::ColumnView,
		value_type::{ValueType, field::FieldType},
	},
};

use crate::compute::CompareOp;

pub fn compare(array: &Canonical, rhs: &Value, op: CompareOp) -> Result<Canonical> {
	let len = array.len();
	let nullable = array.view().is_nullable();
	let (values, nulls) = split_nulls(array.to_column(""))?;
	let values = ColumnView::try_from(&values)?;
	let mut out = Vec::with_capacity(len);
	for i in 0..len {
		let lhs = values.get_value(i);
		let ord = cmp_values(&lhs, rhs);
		out.push(apply_cmp_order(op, ord));
	}
	let value_type = match nullable {
		true => ValueType::Option(Box::new(ValueType::Boolean)),
		false => ValueType::Boolean,
	};
	Canonical::new(FieldType::from(value_type), Arc::new(BooleanArray::new(BooleanBuffer::from(out), nulls)))
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
	use reifydb_core::value::column::factory;

	use super::*;

	#[test]
	fn compare_int4_equality() {
		let ca = Canonical::from_column(&factory::int4("c", [10i32, 20, 30, 20, 40])).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Eq).unwrap();
		assert_eq!(out.view().get_value(0), Value::Boolean(false));
		assert_eq!(out.view().get_value(1), Value::Boolean(true));
		assert_eq!(out.view().get_value(2), Value::Boolean(false));
		assert_eq!(out.view().get_value(3), Value::Boolean(true));
		assert_eq!(out.view().get_value(4), Value::Boolean(false));
	}

	#[test]
	fn compare_int4_greater_than() {
		let ca = Canonical::from_column(&factory::int4("c", [10i32, 20, 30, 40])).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Gt).unwrap();
		assert_eq!(out.view().get_value(0), Value::Boolean(false));
		assert_eq!(out.view().get_value(1), Value::Boolean(false));
		assert_eq!(out.view().get_value(2), Value::Boolean(true));
		assert_eq!(out.view().get_value(3), Value::Boolean(true));
	}
}
