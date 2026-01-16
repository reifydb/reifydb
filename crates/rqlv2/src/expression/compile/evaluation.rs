// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Evaluation functions for binary and unary operations on columns.

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use crate::{
	expression::types::{EvalError, EvalResult},
	plan::node::expr::{BinaryPlanOp, UnaryPlanOp},
};

pub(super) fn eval_binary(op: BinaryPlanOp, left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();
	if right.data().len() != row_count {
		return Err(EvalError::RowCountMismatch {
			expected: row_count,
			actual: right.data().len(),
		});
	}

	match op {
		// Comparison operators
		BinaryPlanOp::Gt => eval_compare(left, right, "_gt", |a, b| a > b, |a, b| a > b),
		BinaryPlanOp::Ge => eval_compare(left, right, "_ge", |a, b| a >= b, |a, b| a >= b),
		BinaryPlanOp::Lt => eval_compare(left, right, "_lt", |a, b| a < b, |a, b| a < b),
		BinaryPlanOp::Le => eval_compare(left, right, "_le", |a, b| a <= b, |a, b| a <= b),
		BinaryPlanOp::Eq => eval_equality(left, right, "_eq", false),
		BinaryPlanOp::Ne => eval_equality(left, right, "_ne", true),

		// Logical operators
		BinaryPlanOp::And => eval_logical_and(left, right),
		BinaryPlanOp::Or => eval_logical_or(left, right),
		BinaryPlanOp::Xor => Err(EvalError::UnsupportedOperation {
			operation: "XOR".to_string(),
		}),

		// Arithmetic operators
		BinaryPlanOp::Add => eval_arithmetic(left, right, "_add", |a, b| a + b, |a, b| a + b),
		BinaryPlanOp::Sub => eval_arithmetic(left, right, "_sub", |a, b| a - b, |a, b| a - b),
		BinaryPlanOp::Mul => eval_arithmetic(left, right, "_mul", |a, b| a * b, |a, b| a * b),
		BinaryPlanOp::Div => eval_arithmetic_div(left, right),
		BinaryPlanOp::Rem => eval_arithmetic_rem(left, right),

		// String
		BinaryPlanOp::Concat => Err(EvalError::UnsupportedOperation {
			operation: "CONCAT".to_string(),
		}),
	}
}

fn eval_compare<FI, FF>(left: &Column, right: &Column, name: &str, cmp_int: FI, cmp_float: FF) -> EvalResult<Column>
where
	FI: Fn(i64, i64) -> bool,
	FF: Fn(f64, f64) -> bool,
{
	let row_count = left.data().len();
	let mut result_data = Vec::with_capacity(row_count);
	let mut result_bitvec = Vec::with_capacity(row_count);

	match (left.data(), right.data()) {
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(cmp_int(lv, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(ColumnData::Float8(l), ColumnData::Float8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(cmp_float(lv, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(ColumnData::Int8(l), ColumnData::Float8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(cmp_float(lv as f64, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(ColumnData::Float8(l), ColumnData::Int8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(cmp_float(lv, rv as f64));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		// Handle comparisons with Undefined
		(ColumnData::Undefined(_), _) | (_, ColumnData::Undefined(_)) => {
			for _ in 0..row_count {
				result_data.push(false);
				result_bitvec.push(false);
			}
		}
		_ => {
			return Err(EvalError::TypeMismatch {
				expected: format!("{:?}", left.data().get_type()),
				found: format!("{:?}", right.data().get_type()),
				context: "comparison operands".to_string(),
			});
		}
	}

	Ok(Column::new(Fragment::internal(name), ColumnData::bool_with_bitvec(result_data, result_bitvec)))
}

fn eval_equality(left: &Column, right: &Column, name: &str, negate: bool) -> EvalResult<Column> {
	let row_count = left.data().len();
	let mut result_data = Vec::with_capacity(row_count);
	let mut result_bitvec = Vec::with_capacity(row_count);

	match (left.data(), right.data()) {
		(ColumnData::Bool(l), ColumnData::Bool(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(lv), Some(rv)) => {
						let eq = lv == rv;
						result_data.push(if negate {
							!eq
						} else {
							eq
						});
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						let eq = lv == rv;
						result_data.push(if negate {
							!eq
						} else {
							eq
						});
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(ColumnData::Float8(l), ColumnData::Float8(r)) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						let eq = lv == rv;
						result_data.push(if negate {
							!eq
						} else {
							eq
						});
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		(
			ColumnData::Utf8 {
				container: l,
				..
			},
			ColumnData::Utf8 {
				container: r,
				..
			},
		) => {
			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(lv), Some(rv)) => {
						let eq = lv == rv;
						result_data.push(if negate {
							!eq
						} else {
							eq
						});
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}
			}
		}
		_ => {
			return Err(EvalError::TypeMismatch {
				expected: format!("{:?}", left.data().get_type()),
				found: format!("{:?}", right.data().get_type()),
				context: "equality operands".to_string(),
			});
		}
	}

	Ok(Column::new(Fragment::internal(name), ColumnData::bool_with_bitvec(result_data, result_bitvec)))
}

fn eval_logical_and(left: &Column, right: &Column) -> EvalResult<Column> {
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

fn eval_logical_or(left: &Column, right: &Column) -> EvalResult<Column> {
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

fn eval_arithmetic<FI, FF>(left: &Column, right: &Column, name: &str, op_int: FI, op_float: FF) -> EvalResult<Column>
where
	FI: Fn(i64, i64) -> i64,
	FF: Fn(f64, f64) -> f64,
{
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(op_int(lv, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal(name),
				ColumnData::int8_with_bitvec(result_data, result_bitvec),
			))
		}
		(ColumnData::Float8(l), ColumnData::Float8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(op_float(lv, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0.0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal(name),
				ColumnData::float8_with_bitvec(result_data, result_bitvec),
			))
		}
		// Mixed types: coerce to Float8
		(ColumnData::Float8(l), ColumnData::Int8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(op_float(lv, rv as f64));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0.0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal(name),
				ColumnData::float8_with_bitvec(result_data, result_bitvec),
			))
		}
		(ColumnData::Int8(l), ColumnData::Float8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(op_float(lv as f64, rv));
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0.0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal(name),
				ColumnData::float8_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.data().get_type()),
			found: format!("{:?}", right.data().get_type()),
			context: format!("{} operands", name),
		}),
	}
}

fn eval_arithmetic_div(left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) if rv != 0 => {
						result_data.push(lv / rv);
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_div"),
				ColumnData::int8_with_bitvec(result_data, result_bitvec),
			))
		}
		(ColumnData::Float8(l), ColumnData::Float8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) => {
						result_data.push(lv / rv);
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0.0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_div"),
				ColumnData::float8_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.data().get_type()),
			found: format!("{:?}", right.data().get_type()),
			context: "DIV operands".to_string(),
		}),
	}
}

fn eval_arithmetic_rem(left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();

	match (left.data(), right.data()) {
		(ColumnData::Int8(l), ColumnData::Int8(r)) => {
			let mut result_data = Vec::with_capacity(row_count);
			let mut result_bitvec = Vec::with_capacity(row_count);

			for i in 0..row_count {
				match (l.get(i), r.get(i)) {
					(Some(&lv), Some(&rv)) if rv != 0 => {
						result_data.push(lv % rv);
						result_bitvec.push(true);
					}
					_ => {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}
			}

			Ok(Column::new(
				Fragment::internal("_rem"),
				ColumnData::int8_with_bitvec(result_data, result_bitvec),
			))
		}
		_ => Err(EvalError::TypeMismatch {
			expected: format!("{:?}", left.data().get_type()),
			found: format!("{:?}", right.data().get_type()),
			context: "REM operands".to_string(),
		}),
	}
}

pub(super) fn eval_unary(op: UnaryPlanOp, col: &Column) -> EvalResult<Column> {
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

pub(super) fn eval_conditional(condition: &Column, then_col: &Column, else_col: &Column) -> EvalResult<Column> {
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
