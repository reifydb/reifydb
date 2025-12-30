// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of Expr AST to CompiledExpr closures.
//!
//! This module converts the Expr enum (an AST) into nested closures that
//! can be executed directly without enum dispatch.

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_type::{BitVec, Fragment, Type, Value};

use super::{
	compiled::{CompiledExpr, CompiledFilter},
	eval::EvalValue,
	types::{BinaryOp, ColumnRef, Expr, Literal, UnaryOp},
};
use crate::error::{Result, VmError};

/// Compile an Expr AST into a CompiledExpr closure.
///
/// The resulting closure captures all static information (column names,
/// literals, operators) and only needs columns and context at evaluation time.
pub fn compile_expr(expr: Expr) -> CompiledExpr {
	match expr {
		Expr::ColumnRef(col_ref) => compile_column_ref(col_ref),
		Expr::Literal(lit) => compile_literal(lit),
		Expr::BinaryOp {
			op,
			left,
			right,
		} => compile_binary(op, *left, *right),
		Expr::UnaryOp {
			op,
			operand,
		} => compile_unary(op, *operand),
		Expr::VarRef(name) => compile_var_ref(name),
		Expr::FieldAccess {
			object,
			field,
		} => compile_field_access(*object, field),
	}
}

/// Compile an Expr AST into a CompiledFilter that returns BitVec directly.
///
/// This is more efficient for filter predicates as it avoids creating
/// an intermediate Column for the boolean result.
pub fn compile_filter(expr: Expr) -> CompiledFilter {
	let compiled = compile_expr(expr);
	CompiledFilter::new(move |columns, ctx| {
		let column = compiled.eval(columns, ctx)?;
		column_to_mask(&column)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Individual compilation functions
// ─────────────────────────────────────────────────────────────────────────────

fn compile_column_ref(col_ref: ColumnRef) -> CompiledExpr {
	let name = col_ref.name.clone();
	let index = col_ref.index;

	CompiledExpr::new(move |columns, _ctx| {
		// Try name-based lookup first
		if !name.is_empty() {
			if let Some(col) = columns.iter().find(|c| c.name().text() == name) {
				return Ok(col.clone());
			}
			return Err(VmError::ColumnNotFound {
				name: name.clone(),
			});
		}

		// Fall back to index
		if index >= columns.len() {
			return Err(VmError::ColumnIndexOutOfBounds {
				index,
				count: columns.len(),
			});
		}
		Ok(columns[index].clone())
	})
}

fn compile_literal(lit: Literal) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| broadcast_literal(&lit, columns.row_count()))
}

fn compile_var_ref(name: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		let value = ctx.get_var(&name).ok_or_else(|| VmError::UndefinedVariable {
			name: name.clone(),
		})?;

		match value {
			EvalValue::Scalar(v) => broadcast_value(v, columns.row_count()),
			EvalValue::Record(_) => Err(VmError::TypeMismatch {
				expected: Type::Int8,
				found: Type::Undefined,
				context: format!("variable '{}' is a record, not a scalar", name).into(),
			}),
		}
	})
}

fn compile_field_access(object: Expr, field: String) -> CompiledExpr {
	// Optimize the common case: VarRef.field
	if let Expr::VarRef(var_name) = object {
		CompiledExpr::new(move |columns, ctx| {
			let value = ctx.get_var(&var_name).ok_or_else(|| VmError::UndefinedVariable {
				name: var_name.clone(),
			})?;

			match value {
				EvalValue::Record(record) => {
					let field_value = record.get(&field).ok_or_else(|| VmError::FieldNotFound {
						field: field.clone(),
						record: var_name.clone(),
					})?;
					broadcast_value(field_value, columns.row_count())
				}
				EvalValue::Scalar(_) => Err(VmError::TypeMismatch {
					expected: Type::Undefined,
					found: Type::Int8,
					context: format!(
						"cannot access field '{}' on scalar variable '{}'",
						field, var_name
					)
					.into(),
				}),
			}
		})
	} else {
		// General case: compile object expression, then access field
		// For now, only VarRef is supported as the object
		let _obj_fn = compile_expr(object);
		CompiledExpr::new(move |_columns, _ctx| {
			Err(VmError::UnsupportedOperation {
				operation: format!("field access '{}' on non-variable expression", field),
			})
		})
	}
}

fn compile_binary(op: BinaryOp, left: Expr, right: Expr) -> CompiledExpr {
	let left_fn = compile_expr(left);
	let right_fn = compile_expr(right);

	CompiledExpr::new(move |columns, ctx| {
		let left_col = left_fn.eval(columns, ctx)?;
		let right_col = right_fn.eval(columns, ctx)?;
		eval_binary(op, &left_col, &right_col)
	})
}

fn compile_unary(op: UnaryOp, operand: Expr) -> CompiledExpr {
	let operand_fn = compile_expr(operand);

	CompiledExpr::new(move |columns, ctx| {
		let col = operand_fn.eval(columns, ctx)?;
		eval_unary(op, &col)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Evaluation helpers (same logic as eval.rs, but standalone functions)
// ─────────────────────────────────────────────────────────────────────────────

fn broadcast_literal(lit: &Literal, row_count: usize) -> Result<Column> {
	let data = match lit {
		Literal::Null => ColumnData::undefined(row_count),
		Literal::Bool(v) => ColumnData::bool(vec![*v; row_count]),
		Literal::Int8(v) => ColumnData::int8(vec![*v; row_count]),
		Literal::Float8(v) => ColumnData::float8(vec![*v; row_count]),
		Literal::Utf8(s) => ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>()),
	};

	Ok(Column::new(Fragment::internal("_literal"), data))
}

fn broadcast_value(value: &Value, row_count: usize) -> Result<Column> {
	let data = match value {
		Value::Undefined => ColumnData::undefined(row_count),
		Value::Boolean(v) => ColumnData::bool(vec![*v; row_count]),
		Value::Int8(v) => ColumnData::int8(vec![*v; row_count]),
		Value::Float8(v) => ColumnData::float8(std::iter::repeat(f64::from(*v)).take(row_count)),
		Value::Utf8(s) => ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>()),
		_ => {
			return Err(VmError::UnsupportedOperation {
				operation: format!("broadcast of value type {:?}", value),
			});
		}
	};

	Ok(Column::new(Fragment::internal("_var"), data))
}

fn column_to_mask(column: &Column) -> Result<BitVec> {
	match column.data() {
		ColumnData::Bool(container) => {
			let mask = BitVec::from_fn(container.len(), |i| container.get(i).unwrap_or(false));
			Ok(mask)
		}
		other => Err(VmError::TypeMismatch {
			expected: Type::Boolean,
			found: other.get_type(),
			context: "filter predicate must be boolean".into(),
		}),
	}
}

fn eval_binary(op: BinaryOp, left: &Column, right: &Column) -> Result<Column> {
	let row_count = left.data().len();
	if right.data().len() != row_count {
		return Err(VmError::RowCountMismatch {
			expected: row_count,
			actual: right.data().len(),
		});
	}

	match op {
		// Comparison operators
		BinaryOp::Gt => eval_compare(left, right, "_gt", |a, b| a > b, |a, b| a > b),
		BinaryOp::Ge => eval_compare(left, right, "_ge", |a, b| a >= b, |a, b| a >= b),
		BinaryOp::Lt => eval_compare(left, right, "_lt", |a, b| a < b, |a, b| a < b),
		BinaryOp::Le => eval_compare(left, right, "_le", |a, b| a <= b, |a, b| a <= b),
		BinaryOp::Eq => eval_equality(left, right, "_eq", false),
		BinaryOp::Ne => eval_equality(left, right, "_ne", true),

		// Logical operators
		BinaryOp::And => eval_logical_and(left, right),
		BinaryOp::Or => eval_logical_or(left, right),

		// Arithmetic operators
		BinaryOp::Add => eval_arithmetic(left, right, "_add", |a, b| a + b, |a, b| a + b),
		BinaryOp::Sub => eval_arithmetic(left, right, "_sub", |a, b| a - b, |a, b| a - b),
		BinaryOp::Mul => eval_arithmetic(left, right, "_mul", |a, b| a * b, |a, b| a * b),
		BinaryOp::Div => eval_arithmetic_div(left, right),
	}
}

fn eval_compare<FI, FF>(left: &Column, right: &Column, name: &str, cmp_int: FI, cmp_float: FF) -> Result<Column>
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
		_ => {
			return Err(VmError::TypeMismatch {
				expected: left.data().get_type(),
				found: right.data().get_type(),
				context: "comparison operands".into(),
			});
		}
	}

	Ok(Column::new(Fragment::internal(name), ColumnData::bool_with_bitvec(result_data, result_bitvec)))
}

fn eval_equality(left: &Column, right: &Column, name: &str, negate: bool) -> Result<Column> {
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
			return Err(VmError::TypeMismatch {
				expected: left.data().get_type(),
				found: right.data().get_type(),
				context: "equality operands".into(),
			});
		}
	}

	Ok(Column::new(Fragment::internal(name), ColumnData::bool_with_bitvec(result_data, result_bitvec)))
}

fn eval_logical_and(left: &Column, right: &Column) -> Result<Column> {
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
		_ => Err(VmError::TypeMismatch {
			expected: Type::Boolean,
			found: left.data().get_type(),
			context: "AND operands must be boolean".into(),
		}),
	}
}

fn eval_logical_or(left: &Column, right: &Column) -> Result<Column> {
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
		_ => Err(VmError::TypeMismatch {
			expected: Type::Boolean,
			found: left.data().get_type(),
			context: "OR operands must be boolean".into(),
		}),
	}
}

fn eval_arithmetic<FI, FF>(left: &Column, right: &Column, name: &str, op_int: FI, op_float: FF) -> Result<Column>
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
		_ => Err(VmError::TypeMismatch {
			expected: left.data().get_type(),
			found: right.data().get_type(),
			context: format!("{} operands", name).into(),
		}),
	}
}

fn eval_arithmetic_div(left: &Column, right: &Column) -> Result<Column> {
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
		_ => Err(VmError::TypeMismatch {
			expected: left.data().get_type(),
			found: right.data().get_type(),
			context: "DIV operands".into(),
		}),
	}
}

fn eval_unary(op: UnaryOp, col: &Column) -> Result<Column> {
	match op {
		UnaryOp::Not => eval_unary_not(col),
		UnaryOp::Neg => eval_unary_neg(col),
		UnaryOp::IsNull => eval_is_null(col, false),
		UnaryOp::IsNotNull => eval_is_null(col, true),
	}
}

fn eval_unary_not(col: &Column) -> Result<Column> {
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
		_ => Err(VmError::TypeMismatch {
			expected: Type::Boolean,
			found: col.data().get_type(),
			context: "NOT operand".into(),
		}),
	}
}

fn eval_unary_neg(col: &Column) -> Result<Column> {
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
		_ => Err(VmError::TypeMismatch {
			expected: Type::Int8,
			found: col.data().get_type(),
			context: "NEG operand".into(),
		}),
	}
}

fn eval_is_null(col: &Column, negated: bool) -> Result<Column> {
	let row_count = col.data().len();

	let result: Vec<bool> = (0..row_count)
		.map(|i| {
			let is_null = !col.data().is_defined(i);
			if negated {
				!is_null
			} else {
				is_null
			}
		})
		.collect();

	Ok(Column::new(
		Fragment::internal(if negated {
			"_is_not_null"
		} else {
			"_is_null"
		}),
		ColumnData::bool(result),
	))
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::Columns;

	use super::*;
	use crate::expr::{EvalContext, EvalValue};

	#[test]
	fn test_compile_column_ref() {
		let expr = Expr::ColumnRef(ColumnRef {
			name: "age".to_string(),
			index: 0,
		});
		let compiled = compile_expr(expr);

		let columns =
			Columns::new(vec![Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 30, 35]))]);

		let result = compiled.eval(&columns, &EvalContext::new()).unwrap();
		match result.data() {
			ColumnData::Int8(c) => {
				assert_eq!(c.len(), 3);
				assert_eq!(c.get(0), Some(&25));
			}
			_ => panic!("Expected Int8 column"),
		}
	}

	#[test]
	fn test_compile_literal() {
		let expr = Expr::Literal(Literal::Int8(42));
		let compiled = compile_expr(expr);

		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let result = compiled.eval(&columns, &EvalContext::new()).unwrap();
		match result.data() {
			ColumnData::Int8(c) => {
				assert_eq!(c.len(), 3);
				assert_eq!(c.get(0), Some(&42));
				assert_eq!(c.get(1), Some(&42));
				assert_eq!(c.get(2), Some(&42));
			}
			_ => panic!("Expected Int8 column"),
		}
	}

	#[test]
	fn test_compile_binary_gt() {
		// age > 30
		let expr = Expr::BinaryOp {
			op: BinaryOp::Gt,
			left: Box::new(Expr::ColumnRef(ColumnRef {
				name: "age".to_string(),
				index: 0,
			})),
			right: Box::new(Expr::Literal(Literal::Int8(30))),
		};
		let compiled = compile_filter(expr);

		let columns =
			Columns::new(vec![Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 30, 35]))]);

		let mask = compiled.eval(&columns, &EvalContext::new()).unwrap();
		assert_eq!(mask.len(), 3);
		let bits: Vec<bool> = mask.iter().collect();
		assert!(!bits[0]); // 25 > 30 = false
		assert!(!bits[1]); // 30 > 30 = false
		assert!(bits[2]); // 35 > 30 = true
	}

	#[test]
	fn test_compile_var_ref() {
		use std::collections::HashMap;

		use reifydb_type::Value;

		let expr = Expr::VarRef("x".to_string());
		let compiled = compile_expr(expr);

		let mut variables = HashMap::new();
		variables.insert("x".to_string(), EvalValue::Scalar(Value::Int8(100)));
		let ctx = EvalContext::with_variables(variables);

		let columns = Columns::new(vec![Column::new(Fragment::from("y"), ColumnData::int8(vec![1, 2, 3]))]);

		let result = compiled.eval(&columns, &ctx).unwrap();
		match result.data() {
			ColumnData::Int8(c) => {
				assert_eq!(c.len(), 3);
				// All rows should have value 100 (broadcast)
				assert_eq!(c.get(0), Some(&100));
				assert_eq!(c.get(1), Some(&100));
				assert_eq!(c.get(2), Some(&100));
			}
			_ => panic!("Expected Int8 column"),
		}
	}
}
