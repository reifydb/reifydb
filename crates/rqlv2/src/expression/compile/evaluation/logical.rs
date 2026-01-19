// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Logical operations: AND, OR, XOR

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use crate::expression::types::{EvalError, EvalResult};

pub(crate) fn eval_logical_and(left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				let l_val = l.get(i);
				let r_val = r.get(i);

				match (l_val, r_val) {
					(Some(false), _) | (_, Some(false)) => {
						result_data.push(false);
						result_bitvec.push(true);
					}
					(Some(true), Some(true)) => {
						result_data.push(true);
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_and"),
				ColumnData::bool_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", left.data().get_type()),
			context: "AND operands".to_string(),
		}),
	}
}

pub(crate) fn eval_logical_or(left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				let l_val = l.get(i);
				let r_val = r.get(i);

				match (l_val, r_val) {
					(Some(true), _) | (_, Some(true)) => {
						result_data.push(true);
						result_bitvec.push(true);
					}
					(Some(false), Some(false)) => {
						result_data.push(false);
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_or"),
				ColumnData::bool_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", left.data().get_type()),
			context: "OR operands".to_string(),
		}),
	}
}

pub(crate) fn eval_logical_xor(left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				let l_val = l.get(i);
				let r_val = r.get(i);

				match (l_val, r_val) {
					(Some(lv), Some(rv)) => {
						// XOR is true when values are different
						result_data.push(lv != rv);
						result_bitvec.push(true);
					}
					_ => {
						// If either operand is null, result is null
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_xor"),
				ColumnData::bool_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", left.data().get_type()),
			context: "XOR operands".to_string(),
		}),
	}
}
