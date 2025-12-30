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
	types::{BinaryOp, ColumnRef, Expr, Literal, SubqueryKind, UnaryOp},
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
		Expr::Subquery {
			index,
			kind,
		} => compile_subquery(index, kind),
		Expr::InSubquery {
			expr,
			subquery_index,
			negated,
		} => compile_in_subquery(*expr, subquery_index, negated),
		Expr::InList {
			expr,
			values,
			negated,
		} => compile_in_list(*expr, values, negated),
		Expr::Call {
			function_name,
			arguments,
		} => compile_call(function_name, arguments),
	}
}

fn compile_call(function_name: String, arguments: Vec<Expr>) -> CompiledExpr {
	// Compile each argument expression
	let compiled_args: Vec<CompiledExpr> = arguments.into_iter().map(compile_expr).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate all argument expressions to columns
		let arg_columns: Vec<Column> =
			compiled_args.iter().map(|arg| arg.eval(columns, ctx)).collect::<Result<Vec<_>>>()?;

		let row_count = columns.row_count();
		let args = reifydb_core::value::column::Columns::new(arg_columns);

		// Call function with columnar args
		let result_data = ctx.call_function(&function_name, &args, row_count)?;

		Ok(Column::new(Fragment::internal("_call"), result_data))
	})
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

	CompiledExpr::new(move |columns, ctx| {
		// Try name-based lookup first
		if !name.is_empty() {
			if let Some(col) = columns.iter().find(|c| c.name().text() == name) {
				return Ok(col.clone());
			}

			// Check outer row values for correlated subqueries
			if let Some(outer_values) = &ctx.current_row_values {
				if let Some(value) = outer_values.get(&name) {
					return broadcast_value(value, columns.row_count());
				}
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

fn compile_subquery(index: u16, kind: SubqueryKind) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		// Execute subquery using the executor from context
		let executor = ctx.subquery_executor.as_ref().ok_or_else(|| VmError::SubqueryExecutorNotAvailable)?;

		// Check if this is a correlated subquery
		let is_correlated = executor.is_correlated(index)?;

		if is_correlated {
			// Correlated subquery - must execute per-row
			let row_count = columns.row_count();
			let mut results = Vec::with_capacity(row_count);

			for row_idx in 0..row_count {
				// Build outer row values for this row
				let mut outer_values = std::collections::HashMap::new();
				for col in columns.iter() {
					let value = col.data().get_value(row_idx);
					outer_values.insert(col.name().text().to_string(), value);
				}

				// Create context with outer row values
				let row_ctx = ctx.with_outer_row(outer_values);
				let result = executor.execute(index, &row_ctx)?;

				match kind {
					SubqueryKind::Scalar => {
						// Handle empty result first - if no rows, value is undefined
						let value = if result.row_count() == 0 || result.is_empty() {
							Value::Undefined
						} else {
							result[0].data().get_value(0)
						};
						results.push(value);
					}
					SubqueryKind::Exists => {
						results.push(Value::Boolean(result.row_count() > 0));
					}
					SubqueryKind::NotExists => {
						results.push(Value::Boolean(result.row_count() == 0));
					}
				}
			}

			// Build result column from collected values
			match kind {
				SubqueryKind::Scalar => {
					// Determine type from first non-undefined value
					values_to_column("_scalar", results)
				}
				SubqueryKind::Exists | SubqueryKind::NotExists => {
					let bools: Vec<bool> = results
						.into_iter()
						.map(|v| matches!(v, Value::Boolean(true)))
						.collect();
					Ok(Column::new(
						Fragment::internal(if matches!(kind, SubqueryKind::Exists) {
							"_exists"
						} else {
							"_not_exists"
						}),
						ColumnData::bool(bools),
					))
				}
			}
		} else {
			// Uncorrelated subquery - execute once and broadcast
			let result = executor.execute(index, ctx)?;

			match kind {
				SubqueryKind::Scalar => {
					// Handle empty result - if no rows or no columns, return NULL
					if result.row_count() == 0 || result.is_empty() {
						return Ok(Column::new(
							Fragment::internal("_scalar"),
							ColumnData::undefined(columns.row_count()),
						));
					}
					// Get first value and broadcast
					let col = &result[0];
					let value = col.data().get_value(0);
					broadcast_value(&value, columns.row_count())
				}
				SubqueryKind::Exists => {
					// Return true if subquery has any rows
					let exists = result.row_count() > 0;
					Ok(Column::new(
						Fragment::internal("_exists"),
						ColumnData::bool(vec![exists; columns.row_count()]),
					))
				}
				SubqueryKind::NotExists => {
					// Return true if subquery has no rows
					let not_exists = result.row_count() == 0;
					Ok(Column::new(
						Fragment::internal("_not_exists"),
						ColumnData::bool(vec![not_exists; columns.row_count()]),
					))
				}
			}
		}
	})
}

fn compile_in_subquery(expr: Expr, subquery_index: u16, negated: bool) -> CompiledExpr {
	let expr_fn = compile_expr(expr);

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate left expression
		let left_col = expr_fn.eval(columns, ctx)?;

		// Execute subquery
		let executor = ctx.subquery_executor.as_ref().ok_or_else(|| VmError::SubqueryExecutorNotAvailable)?;

		let result = executor.execute(subquery_index, ctx)?;

		// Build membership set from first column of result
		let set = build_membership_set(&result)?;

		// Check membership for each value in left column
		check_membership(&left_col, &set, negated)
	})
}

fn compile_in_list(expr: Expr, values: Vec<Expr>, negated: bool) -> CompiledExpr {
	let expr_fn = compile_expr(expr);
	let value_fns: Vec<_> = values.into_iter().map(compile_expr).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate left expression
		let left_col = expr_fn.eval(columns, ctx)?;

		// Build membership set from evaluated values
		let mut set = std::collections::HashSet::new();
		for vfn in &value_fns {
			let val_col = vfn.eval(columns, ctx)?;
			// Add first value (assumed to be a scalar broadcast)
			if val_col.data().len() > 0 {
				let value = val_col.data().get_value(0);
				set.insert(HashableValue(value));
			}
		}

		// Check membership
		check_membership_set(&left_col, &set, negated)
	})
}

/// Wrapper to make Value hashable for set membership
#[derive(Clone, Debug)]
struct HashableValue(Value);

impl PartialEq for HashableValue {
	fn eq(&self, other: &Self) -> bool {
		match (&self.0, &other.0) {
			(Value::Int8(a), Value::Int8(b)) => a == b,
			(Value::Float8(a), Value::Float8(b)) => a == b,
			(Value::Boolean(a), Value::Boolean(b)) => a == b,
			(Value::Utf8(a), Value::Utf8(b)) => a == b,
			(Value::Undefined, Value::Undefined) => true,
			_ => false,
		}
	}
}

impl Eq for HashableValue {}

impl std::hash::Hash for HashableValue {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		match &self.0 {
			Value::Int8(v) => {
				0u8.hash(state);
				v.hash(state);
			}
			Value::Float8(v) => {
				1u8.hash(state);
				v.value().to_bits().hash(state);
			}
			Value::Boolean(v) => {
				2u8.hash(state);
				v.hash(state);
			}
			Value::Utf8(v) => {
				3u8.hash(state);
				v.hash(state);
			}
			Value::Undefined => {
				4u8.hash(state);
			}
			_ => {
				255u8.hash(state);
			}
		}
	}
}

/// Build a membership set from the first column of a Columns result.
fn build_membership_set(
	columns: &reifydb_core::value::column::Columns,
) -> Result<std::collections::HashSet<HashableValue>> {
	let mut set = std::collections::HashSet::new();
	if columns.is_empty() {
		return Ok(set);
	}
	let col = &columns[0];
	for i in 0..col.data().len() {
		let value = col.data().get_value(i);
		set.insert(HashableValue(value));
	}
	Ok(set)
}

/// Check membership of left column values in set.
fn check_membership(left: &Column, set: &std::collections::HashSet<HashableValue>, negated: bool) -> Result<Column> {
	let row_count = left.data().len();
	let mut result_data = Vec::with_capacity(row_count);

	for i in 0..row_count {
		let value = left.data().get_value(i);
		let is_member = set.contains(&HashableValue(value));
		let result = if negated {
			!is_member
		} else {
			is_member
		};
		result_data.push(result);
	}

	Ok(Column::new(
		Fragment::internal(if negated {
			"_not_in"
		} else {
			"_in"
		}),
		ColumnData::bool(result_data),
	))
}

/// Check membership using pre-built HashableValue set.
fn check_membership_set(
	left: &Column,
	set: &std::collections::HashSet<HashableValue>,
	negated: bool,
) -> Result<Column> {
	check_membership(left, set, negated)
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

/// Convert a vector of values to a column, inferring type from the first non-undefined value.
fn values_to_column(name: &str, values: Vec<Value>) -> Result<Column> {
	if values.is_empty() {
		return Ok(Column::new(Fragment::internal(name), ColumnData::undefined(0)));
	}

	// Find the first non-undefined value to determine type
	let first_defined = values.iter().find(|v| !matches!(v, Value::Undefined));

	let data = match first_defined {
		Some(Value::Int8(_)) => {
			let ints: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			let bitvec: Vec<bool> = values.iter().map(|v| !matches!(v, Value::Undefined)).collect();
			ColumnData::int8_with_bitvec(ints, bitvec)
		}
		Some(Value::Float8(f)) => {
			let floats: Vec<f64> = values
				.iter()
				.map(|v| match v {
					Value::Float8(f) => f64::from(*f),
					Value::Int8(n) => *n as f64,
					_ => 0.0,
				})
				.collect();
			let bitvec: Vec<bool> = values.iter().map(|v| !matches!(v, Value::Undefined)).collect();
			ColumnData::float8_with_bitvec(floats, bitvec)
		}
		Some(Value::Utf8(_)) => {
			let strings: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => String::new(),
				})
				.collect();
			ColumnData::utf8(strings)
		}
		Some(Value::Boolean(_)) => {
			let bools: Vec<bool> = values.iter().map(|v| matches!(v, Value::Boolean(true))).collect();
			ColumnData::bool(bools)
		}
		_ => ColumnData::undefined(values.len()),
	};

	Ok(Column::new(Fragment::internal(name), data))
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
		// Handle comparisons with Undefined - result is always Undefined (false in filter)
		(ColumnData::Int8(_), ColumnData::Undefined(_))
		| (ColumnData::Undefined(_), ColumnData::Int8(_))
		| (ColumnData::Float8(_), ColumnData::Undefined(_))
		| (ColumnData::Undefined(_), ColumnData::Float8(_))
		| (ColumnData::Undefined(_), ColumnData::Undefined(_)) => {
			// Comparison with NULL yields NULL - in filter context, this means false
			for _ in 0..row_count {
				result_data.push(false);
				result_bitvec.push(false); // Mark as undefined
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
