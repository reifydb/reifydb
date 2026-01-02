// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of PlanExpr to CompiledExpr async closures.
//!
//! This module converts PlanExpr (a resolved AST) into nested closures that
//! return futures and can be executed directly without enum dispatch.
//! The async design supports subquery execution within expressions.
//!
//! Reference: https://blog.cloudflare.com/building-fast-interpreters-in-rust/

use std::collections::HashMap;

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_type::{BitVec, Fragment, Value};

use super::{
	eval::EvalValue,
	types::{CompiledExpr, CompiledFilter, EvalError, EvalResult},
};
use crate::plan::node::expr::{BinaryPlanOp, PlanExpr, UnaryPlanOp};

/// Compile a PlanExpr into a CompiledExpr closure.
///
/// The resulting closure captures all static information (column names,
/// literals, operators) and only needs columns and context at evaluation time.
pub fn compile_plan_expr<'bump>(expr: &PlanExpr<'bump>) -> CompiledExpr {
	match expr {
		PlanExpr::LiteralNull(_) => compile_literal_null(),
		PlanExpr::LiteralBool(v, _) => compile_literal_bool(*v),
		PlanExpr::LiteralInt(v, _) => compile_literal_int(*v),
		PlanExpr::LiteralFloat(v, _) => compile_literal_float(*v),
		PlanExpr::LiteralString(v, _) => compile_literal_string(v.to_string()),
		PlanExpr::LiteralBytes(v, _) => compile_literal_bytes(v.to_vec()),
		PlanExpr::Column(col) => compile_column_ref(col.name().to_string()),
		PlanExpr::Variable(var) => compile_variable_ref(var.variable_id, var.name.to_string()),
		PlanExpr::Rownum(_) => compile_rownum(),
		PlanExpr::Wildcard(_) => compile_wildcard(),
		PlanExpr::Binary {
			op,
			left,
			right,
			..
		} => compile_binary(*op, left, right),
		PlanExpr::Unary {
			op,
			operand,
			..
		} => compile_unary(*op, operand),
		PlanExpr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => compile_between(expr, low, high, *negated),
		PlanExpr::In {
			expr,
			list,
			negated,
			..
		} => compile_in(expr, list, *negated),
		PlanExpr::Cast {
			expr,
			target_type,
			..
		} => compile_cast(expr, *target_type),
		PlanExpr::Call {
			function,
			arguments,
			..
		} => compile_call(function.name.to_string(), arguments),
		PlanExpr::Aggregate {
			function,
			arguments,
			distinct,
			..
		} => compile_aggregate(function.name.to_string(), arguments, *distinct),
		PlanExpr::Conditional {
			condition,
			then_expr,
			else_expr,
			..
		} => compile_conditional(condition, then_expr, else_expr),
		PlanExpr::Subquery(_plan) => {
			// Subqueries require executor support
			CompiledExpr::new(|_, _| {
				Box::pin(async {
					Err(EvalError::UnsupportedOperation {
						operation: "subquery".to_string(),
					})
				})
			})
		}
		PlanExpr::List(items, _) => compile_list(items),
		PlanExpr::Tuple(items, _) => compile_tuple(items),
		PlanExpr::Record(fields, _) => compile_record(fields),
		PlanExpr::Alias {
			expr,
			..
		} => compile_plan_expr(expr), // Alias is metadata only
	}
}

/// Compile a PlanExpr into a CompiledFilter that returns BitVec directly.
///
/// This is more efficient for filter predicates as it avoids creating
/// an intermediate Column for the boolean result.
pub fn compile_plan_filter<'bump>(expr: &PlanExpr<'bump>) -> CompiledFilter {
	let compiled = compile_plan_expr(expr);
	CompiledFilter::new(move |columns, ctx| {
		let compiled = compiled.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let column = compiled.eval(&columns, &ctx).await?;
			column_to_mask(&column)
		})
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Literal compilation
// ─────────────────────────────────────────────────────────────────────────────

fn compile_literal_null() -> CompiledExpr {
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move { Ok(Column::new(Fragment::internal("_null"), ColumnData::undefined(row_count))) })
	})
}

fn compile_literal_bool(value: bool) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move {
			Ok(Column::new(Fragment::internal("_bool"), ColumnData::bool(vec![value; row_count])))
		})
	})
}

fn compile_literal_int(value: i64) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move {
			Ok(Column::new(Fragment::internal("_int"), ColumnData::int8(vec![value; row_count])))
		})
	})
}

fn compile_literal_float(value: f64) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move {
			Ok(Column::new(
				Fragment::internal("_float"),
				ColumnData::float8(std::iter::repeat(value).take(row_count)),
			))
		})
	})
}

fn compile_literal_string(value: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let value = value.clone();
		let row_count = columns.row_count();
		Box::pin(async move {
			Ok(Column::new(
				Fragment::internal("_string"),
				ColumnData::utf8(std::iter::repeat(value).take(row_count).collect::<Vec<_>>()),
			))
		})
	})
}

fn compile_literal_bytes(_value: Vec<u8>) -> CompiledExpr {
	// TODO: Implement proper bytes column support
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move { Ok(Column::new(Fragment::internal("_bytes"), ColumnData::undefined(row_count))) })
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Reference compilation
// ─────────────────────────────────────────────────────────────────────────────

fn compile_column_ref(name: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		let name = name.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			// Try name-based lookup in columns
			if let Some(col) = columns.iter().find(|c| c.name().text() == name) {
				return Ok(col.clone());
			}

			// Check outer row values for correlated subqueries
			if let Some(outer_values) = &ctx.current_row_values {
				if let Some(value) = outer_values.get(&name) {
					return broadcast_value(value, columns.row_count());
				}
			}

			Err(EvalError::ColumnNotFound {
				name,
			})
		})
	})
}

fn compile_variable_ref(id: u32, name: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		let name = name.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let value = ctx.get_var(id).ok_or(EvalError::VariableNotFound {
				id,
			})?;

			match value {
				EvalValue::Scalar(v) => broadcast_value(v, columns.row_count()),
				EvalValue::Record(_) => Err(EvalError::TypeMismatch {
					expected: "scalar".to_string(),
					found: "record".to_string(),
					context: format!("variable '{}'", name),
				}),
			}
		})
	})
}

fn compile_rownum() -> CompiledExpr {
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		Box::pin(async move {
			let values: Vec<i64> = (0..row_count as i64).collect();
			Ok(Column::new(Fragment::internal("_rownum"), ColumnData::int8(values)))
		})
	})
}

fn compile_wildcard() -> CompiledExpr {
	// Wildcard should be expanded during planning
	CompiledExpr::new(|_, _| {
		Box::pin(async {
			Err(EvalError::UnsupportedOperation {
				operation: "wildcard should be expanded during planning".to_string(),
			})
		})
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operator compilation
// ─────────────────────────────────────────────────────────────────────────────

fn compile_binary<'bump>(op: BinaryPlanOp, left: &PlanExpr<'bump>, right: &PlanExpr<'bump>) -> CompiledExpr {
	let left_fn = compile_plan_expr(left);
	let right_fn = compile_plan_expr(right);

	CompiledExpr::new(move |columns, ctx| {
		let left_fn = left_fn.clone();
		let right_fn = right_fn.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let left_col = left_fn.eval(&columns, &ctx).await?;
			let right_col = right_fn.eval(&columns, &ctx).await?;
			eval_binary(op, &left_col, &right_col)
		})
	})
}

fn compile_unary<'bump>(op: UnaryPlanOp, operand: &PlanExpr<'bump>) -> CompiledExpr {
	let operand_fn = compile_plan_expr(operand);

	CompiledExpr::new(move |columns, ctx| {
		let operand_fn = operand_fn.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let col = operand_fn.eval(&columns, &ctx).await?;
			eval_unary(op, &col)
		})
	})
}

fn compile_between<'bump>(
	expr: &PlanExpr<'bump>,
	low: &PlanExpr<'bump>,
	high: &PlanExpr<'bump>,
	negated: bool,
) -> CompiledExpr {
	let expr_fn = compile_plan_expr(expr);
	let low_fn = compile_plan_expr(low);
	let high_fn = compile_plan_expr(high);

	CompiledExpr::new(move |columns, ctx| {
		let expr_fn = expr_fn.clone();
		let low_fn = low_fn.clone();
		let high_fn = high_fn.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let expr_col = expr_fn.eval(&columns, &ctx).await?;
			let low_col = low_fn.eval(&columns, &ctx).await?;
			let high_col = high_fn.eval(&columns, &ctx).await?;

			// expr >= low AND expr <= high
			let ge_result = eval_binary(BinaryPlanOp::Ge, &expr_col, &low_col)?;
			let le_result = eval_binary(BinaryPlanOp::Le, &expr_col, &high_col)?;
			let result = eval_binary(BinaryPlanOp::And, &ge_result, &le_result)?;

			if negated {
				eval_unary(UnaryPlanOp::Not, &result)
			} else {
				Ok(result)
			}
		})
	})
}

fn compile_in<'bump>(expr: &PlanExpr<'bump>, list: &[&PlanExpr<'bump>], negated: bool) -> CompiledExpr {
	let expr_fn = compile_plan_expr(expr);
	let list_fns: Vec<_> = list.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		let expr_fn = expr_fn.clone();
		let list_fns = list_fns.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			if list_fns.is_empty() {
				// Empty list: result is false (or true if negated)
				return Ok(Column::new(
					Fragment::internal("_in"),
					ColumnData::bool(vec![negated; columns.row_count()]),
				));
			}

			let expr_col = expr_fn.eval(&columns, &ctx).await?;

			// Build result: expr = v1 OR expr = v2 OR ...
			let first = list_fns[0].eval(&columns, &ctx).await?;
			let mut result = eval_binary(BinaryPlanOp::Eq, &expr_col, &first)?;

			for item_fn in &list_fns[1..] {
				let item_col = item_fn.eval(&columns, &ctx).await?;
				let eq_result = eval_binary(BinaryPlanOp::Eq, &expr_col, &item_col)?;
				result = eval_binary(BinaryPlanOp::Or, &result, &eq_result)?;
			}

			if negated {
				eval_unary(UnaryPlanOp::Not, &result)
			} else {
				Ok(result)
			}
		})
	})
}

fn compile_cast<'bump>(expr: &PlanExpr<'bump>, _target_type: crate::plan::Type) -> CompiledExpr {
	// TODO: Implement proper type casting
	compile_plan_expr(expr)
}

fn compile_call<'bump>(function_name: String, arguments: &[&PlanExpr<'bump>]) -> CompiledExpr {
	let arg_fns: Vec<_> = arguments.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		let function_name = function_name.clone();
		let arg_fns = arg_fns.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			// Evaluate all arguments
			let mut arg_cols = Vec::with_capacity(arg_fns.len());
			for arg_fn in &arg_fns {
				arg_cols.push(arg_fn.eval(&columns, &ctx).await?);
			}

			// TODO: Call function registry
			let _ = function_name;
			Err(EvalError::UnsupportedOperation {
				operation: "function calls".to_string(),
			})
		})
	})
}

fn compile_aggregate<'bump>(_function_name: String, _arguments: &[&PlanExpr<'bump>], _distinct: bool) -> CompiledExpr {
	// Aggregates are handled by the Apply(Aggregate) plan node
	CompiledExpr::new(|_, _| {
		Box::pin(async {
			Err(EvalError::UnsupportedOperation {
				operation: "aggregate in expression context".to_string(),
			})
		})
	})
}

fn compile_conditional<'bump>(
	condition: &PlanExpr<'bump>,
	then_expr: &PlanExpr<'bump>,
	else_expr: &PlanExpr<'bump>,
) -> CompiledExpr {
	let cond_fn = compile_plan_expr(condition);
	let then_fn = compile_plan_expr(then_expr);
	let else_fn = compile_plan_expr(else_expr);

	CompiledExpr::new(move |columns, ctx| {
		let cond_fn = cond_fn.clone();
		let then_fn = then_fn.clone();
		let else_fn = else_fn.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			let cond_col = cond_fn.eval(&columns, &ctx).await?;
			let then_col = then_fn.eval(&columns, &ctx).await?;
			let else_col = else_fn.eval(&columns, &ctx).await?;

			eval_conditional(&cond_col, &then_col, &else_col)
		})
	})
}

fn compile_list<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
	let item_fns: Vec<_> = items.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		let item_fns = item_fns.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			// Evaluate all items
			let mut _items = Vec::with_capacity(item_fns.len());
			for item_fn in &item_fns {
				_items.push(item_fn.eval(&columns, &ctx).await?);
			}
			// TODO: Build list value
			Err(EvalError::UnsupportedOperation {
				operation: "list expressions".to_string(),
			})
		})
	})
}

fn compile_tuple<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
	let item_fns: Vec<_> = items.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		let item_fns = item_fns.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			// Evaluate all items
			let mut _items = Vec::with_capacity(item_fns.len());
			for item_fn in &item_fns {
				_items.push(item_fn.eval(&columns, &ctx).await?);
			}
			// TODO: Build tuple value
			Err(EvalError::UnsupportedOperation {
				operation: "tuple expressions".to_string(),
			})
		})
	})
}

fn compile_record<'bump>(fields: &[(&str, &PlanExpr<'bump>)]) -> CompiledExpr {
	let field_fns: Vec<_> = fields.iter().map(|(name, expr)| (name.to_string(), compile_plan_expr(expr))).collect();

	CompiledExpr::new(move |columns, ctx| {
		let field_fns = field_fns.clone();
		let columns = columns.clone();
		let ctx = ctx.clone();
		Box::pin(async move {
			// Evaluate all fields
			let mut _fields = HashMap::new();
			for (name, expr_fn) in &field_fns {
				let col = expr_fn.eval(&columns, &ctx).await?;
				_fields.insert(name.clone(), col);
			}
			// TODO: Build record value
			Err(EvalError::UnsupportedOperation {
				operation: "record expressions".to_string(),
			})
		})
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Evaluation helpers
// ─────────────────────────────────────────────────────────────────────────────

fn broadcast_value(value: &Value, row_count: usize) -> EvalResult<Column> {
	let data = match value {
		Value::Undefined => ColumnData::undefined(row_count),
		Value::Boolean(v) => ColumnData::bool(vec![*v; row_count]),
		Value::Int8(v) => ColumnData::int8(vec![*v; row_count]),
		Value::Float8(v) => ColumnData::float8(std::iter::repeat(f64::from(*v)).take(row_count)),
		Value::Utf8(s) => ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>()),
		_ => {
			return Err(EvalError::UnsupportedOperation {
				operation: format!("broadcast of value type {:?}", value),
			});
		}
	};

	Ok(Column::new(Fragment::internal("_var"), data))
}

fn column_to_mask(column: &Column) -> EvalResult<BitVec> {
	match column.data() {
		ColumnData::Bool(container) => {
			let mask = BitVec::from_fn(container.len(), |i| container.get(i).unwrap_or(false));
			Ok(mask)
		}
		other => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", other.get_type()),
			context: "filter predicate".to_string(),
		}),
	}
}

fn eval_binary(op: BinaryPlanOp, left: &Column, right: &Column) -> EvalResult<Column> {
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

fn eval_unary(op: UnaryPlanOp, col: &Column) -> EvalResult<Column> {
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

fn eval_conditional(condition: &Column, then_col: &Column, else_col: &Column) -> EvalResult<Column> {
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

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::Columns;
	use reifydb_type::Value;

	use super::*;
	use crate::compiled_expr::EvalContext;

	#[tokio::test]
	async fn test_compile_literal_int() {
		let expr = compile_literal_int(42);
		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let result = expr.eval(&columns, &EvalContext::new()).await.unwrap();
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

	#[tokio::test]
	async fn test_compile_column_ref() {
		let expr = compile_column_ref("age".to_string());
		let columns =
			Columns::new(vec![Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 30, 35]))]);

		let result = expr.eval(&columns, &EvalContext::new()).await.unwrap();
		match result.data() {
			ColumnData::Int8(c) => {
				assert_eq!(c.len(), 3);
				assert_eq!(c.get(0), Some(&25));
			}
			_ => panic!("Expected Int8 column"),
		}
	}

	#[tokio::test]
	async fn test_compile_comparison() {
		// Build: 25 > 30 = false, 30 > 30 = false, 35 > 30 = true
		let age_col = compile_column_ref("age".to_string());
		let threshold = compile_literal_int(30);

		let gt_expr = CompiledExpr::new(move |columns, ctx| {
			let age_col = age_col.clone();
			let threshold = threshold.clone();
			let columns = columns.clone();
			let ctx = ctx.clone();
			Box::pin(async move {
				let left = age_col.eval(&columns, &ctx).await?;
				let right = threshold.eval(&columns, &ctx).await?;
				eval_binary(BinaryPlanOp::Gt, &left, &right)
			})
		});

		let columns =
			Columns::new(vec![Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 30, 35]))]);

		let result = gt_expr.eval(&columns, &EvalContext::new()).await.unwrap();
		match result.data() {
			ColumnData::Bool(c) => {
				assert_eq!(c.len(), 3);
				assert!(!c.get(0).unwrap()); // 25 > 30 = false
				assert!(!c.get(1).unwrap()); // 30 > 30 = false
				assert!(c.get(2).unwrap()); // 35 > 30 = true
			}
			_ => panic!("Expected Bool column"),
		}
	}

	#[tokio::test]
	async fn test_compile_variable_ref() {
		let expr = compile_variable_ref(1, "x".to_string());

		let mut ctx = EvalContext::new();
		ctx.set_var(1, EvalValue::Scalar(Value::Int8(100)));

		let columns = Columns::new(vec![Column::new(Fragment::from("y"), ColumnData::int8(vec![1, 2, 3]))]);

		let result = expr.eval(&columns, &ctx).await.unwrap();
		match result.data() {
			ColumnData::Int8(c) => {
				assert_eq!(c.len(), 3);
				assert_eq!(c.get(0), Some(&100));
				assert_eq!(c.get(1), Some(&100));
				assert_eq!(c.get(2), Some(&100));
			}
			_ => panic!("Expected Int8 column"),
		}
	}
}
