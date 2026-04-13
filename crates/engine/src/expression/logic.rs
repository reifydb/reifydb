// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
};

use super::option::binary_op_unwrap_option;
use crate::Result;

pub(crate) fn execute_logical_op(
	left: &Column,
	right: &Column,
	fragment: &Fragment,
	logical_op: LogicalOp,
	bool_fn: fn(bool, bool) -> bool,
) -> Result<Column> {
	binary_op_unwrap_option(left, right, fragment.clone(), |left, right| match (&left.data(), &right.data()) {
		(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
			let data: Vec<bool> = l_container
				.data()
				.iter()
				.zip(r_container.data().iter())
				.map(|(l_val, r_val)| bool_fn(l_val, r_val))
				.collect();

			Ok(Column {
				name: fragment.clone(),
				data: ColumnData::bool(data),
			})
		}
		(l, r) => {
			let category = if l.is_number() || r.is_number() {
				OperandCategory::Number
			} else if l.is_text() || r.is_text() {
				OperandCategory::Text
			} else if l.is_temporal() || r.is_temporal() {
				OperandCategory::Temporal
			} else if l.is_uuid() || r.is_uuid() {
				OperandCategory::Uuid
			} else {
				unimplemented!("{} {:?} {}", l.get_type(), logical_op, r.get_type());
			};
			Err(TypeError::LogicalOperatorNotApplicable {
				operator: logical_op.clone(),
				operand_category: category,
				fragment: fragment.clone(),
			}
			.into())
		}
	})
}
