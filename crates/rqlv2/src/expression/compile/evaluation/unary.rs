// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Unary operations: NOT, NEG, PLUS

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use crate::{
	expression::types::{EvalError, EvalResult},
	plan::node::expr::UnaryPlanOp,
};

pub(crate) fn eval_unary(op: UnaryPlanOp, col: &Column) -> EvalResult<Column> {
	match op {
		UnaryPlanOp::Not => eval_unary_not(col),
		UnaryPlanOp::Neg => eval_unary_neg(col),
		UnaryPlanOp::Plus => Ok(col.clone()), // Plus is a no-op
	}
}

fn eval_unary_not(col: &Column) -> EvalResult<Column> {
	match col.data() {
		ColumnData::Bool(container) => {
			let row_count = container.len();
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match container.get(i) {
					Some(v) => {
						result_data.push(!v);
						result_bitvec.push(true);
					}
					None => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_not"),
				ColumnData::bool_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", col.data().get_type()),
			context: "NOT operand".to_string(),
		}),
	}
}

fn eval_unary_neg(col: &Column) -> EvalResult<Column> {
	match col.data() {
		ColumnData::Int8(container) => {
			let row_count = container.len();
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match container.get(i) {
					Some(&v) => {
						result_data.push(-v);
						result_bitvec.push(true);
					}
					None => {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_neg"),
				ColumnData::int8_with_bitvec(result_data, result_bitvec),
			))
		}
		ColumnData::Float8(container) => {
			let row_count = container.len();
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match container.get(i) {
					Some(&v) => {
						result_data.push(-v);
						result_bitvec.push(true);
					}
					None => {
						result_data.push(0.0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_neg"),
				ColumnData::float8_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "numeric".to_string(),
			found: format!("{:?}", col.data().get_type()),
			context: "NEG operand".to_string(),
		}),
	}
}
