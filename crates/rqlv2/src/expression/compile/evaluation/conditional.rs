// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Conditional expression evaluation: IF/THEN/ELSE

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use crate::expression::types::{EvalError, EvalResult};

pub(crate) fn eval_conditional(condition: &Column, then_col: &Column, else_col: &Column) -> EvalResult<Column> {
	let row_count = condition.data().len();

	match condition.data() {
		ColumnData::Bool(cond) => {
			// For now, require same type for then/else
			match (then_col.data(), else_col.data()) {
				(ColumnData::Int8(t), ColumnData::Int8(e)) => {
					let mut result_data = Vec::with_capacity(row_count);
					let mut result_bitvec = Vec::with_capacity(row_count);

					for i in 0..row_count {
						match cond.get(i) {
							Some(true) => match t.get(i) {
								Some(&v) => {
									result_data.push(v);
									result_bitvec.push(true);
								}
								None => {
									result_data.push(0);
									result_bitvec.push(false);
								}
							},
							Some(false) => match e.get(i) {
								Some(&v) => {
									result_data.push(v);
									result_bitvec.push(true);
								}
								None => {
									result_data.push(0);
									result_bitvec.push(false);
								}
							},
							None => {
								result_data.push(0);
								result_bitvec.push(false);
							}
						}
					}

					Ok(Column::new(
						Fragment::internal("_if"),
						ColumnData::int8_with_bitvec(result_data, result_bitvec),
					))
				}
				_ => Err(EvalError::UnsupportedOperation {
					operation: "conditional with non-int types".to_string(),
				}),
			}
		}
		_ => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", condition.data().get_type()),
			context: "conditional condition".to_string(),
		}),
	}
}
