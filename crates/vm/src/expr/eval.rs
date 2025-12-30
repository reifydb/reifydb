// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{Column, ColumnData, Columns};
use reifydb_type::{BitVec, Fragment, Type, Value};

use super::types::{BinaryOp, ColumnRef, Expr, Literal, UnaryOp};
use crate::{
	error::{Result, VmError},
	vmcore::state::Record,
};

/// Trait for executing subqueries during expression evaluation.
pub trait SubqueryExecutor: Send + Sync {
	/// Execute a subquery by its index and return the result columns.
	fn execute(&self, index: u16, ctx: &EvalContext) -> Result<Columns>;

	/// Check if a subquery is correlated (references outer columns).
	fn is_correlated(&self, index: u16) -> Result<bool>;
}

/// Context for expression evaluation with captured scope variables.
#[derive(Default, Clone)]
pub struct EvalContext {
	/// Captured variable values from scope at filter creation time.
	pub variables: HashMap<String, EvalValue>,

	/// Optional subquery executor for evaluating subquery expressions.
	pub subquery_executor: Option<Arc<dyn SubqueryExecutor>>,

	/// Current row values for correlated subquery execution.
	/// Maps column names to their values for the current outer row.
	pub current_row_values: Option<HashMap<String, Value>>,
}

/// Value types that can be used in expression evaluation.
#[derive(Debug, Clone)]
pub enum EvalValue {
	Scalar(Value),
	Record(Record),
}

impl EvalContext {
	/// Create an empty evaluation context.
	pub fn new() -> Self {
		Self {
			variables: HashMap::new(),
			subquery_executor: None,
			current_row_values: None,
		}
	}

	/// Create a context with the given variables.
	pub fn with_variables(variables: HashMap<String, EvalValue>) -> Self {
		Self {
			variables,
			subquery_executor: None,
			current_row_values: None,
		}
	}

	/// Create a context with a subquery executor.
	pub fn with_subquery_executor(executor: Arc<dyn SubqueryExecutor>) -> Self {
		Self {
			variables: HashMap::new(),
			subquery_executor: Some(executor),
			current_row_values: None,
		}
	}

	/// Get a variable value.
	pub fn get_var(&self, name: &str) -> Option<&EvalValue> {
		self.variables.get(name)
	}

	/// Get a value from current_row_values (for correlated subquery column lookup).
	pub fn get_outer_column(&self, name: &str) -> Option<&Value> {
		self.current_row_values.as_ref()?.get(name)
	}

	/// Create a new context with outer row values for correlated subquery execution.
	pub fn with_outer_row(&self, outer_values: HashMap<String, Value>) -> Self {
		Self {
			variables: self.variables.clone(),
			subquery_executor: self.subquery_executor.clone(),
			current_row_values: Some(outer_values),
		}
	}
}

impl Expr {
	/// Evaluate expression to a boolean mask (for filter predicates).
	/// Returns a BitVec where true = row passes the filter.
	pub fn eval_to_mask(&self, columns: &Columns, ctx: &EvalContext) -> Result<BitVec> {
		let column = self.eval_to_column(columns, ctx)?;

		match column.data() {
			ColumnData::Bool(container) => {
				// Row passes if: is_defined AND value == true
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

	/// Evaluate expression to a Column (for projections/computed columns).
	pub fn eval_to_column(&self, columns: &Columns, ctx: &EvalContext) -> Result<Column> {
		match self {
			Expr::ColumnRef(col_ref) => self.eval_column_ref(col_ref, columns),
			Expr::Literal(lit) => self.broadcast_literal(lit, columns.row_count()),
			Expr::BinaryOp {
				op,
				left,
				right,
			} => {
				let left_col = left.eval_to_column(columns, ctx)?;
				let right_col = right.eval_to_column(columns, ctx)?;
				self.eval_binary(*op, &left_col, &right_col)
			}
			Expr::UnaryOp {
				op,
				operand,
			} => {
				let col = operand.eval_to_column(columns, ctx)?;
				self.eval_unary(*op, &col)
			}
			Expr::VarRef(name) => self.eval_var_ref(name, columns.row_count(), ctx),
			Expr::FieldAccess {
				object,
				field,
			} => self.eval_field_access(object, field, columns, ctx),
			Expr::Subquery {
				..
			}
			| Expr::InSubquery {
				..
			}
			| Expr::InList {
				..
			} => {
				// Subquery expressions are handled by the compiled expression path
				// (compile.rs) which has access to the SubqueryExecutor.
				// Direct eval_to_column on Expr AST for subqueries is not supported.
				Err(VmError::UnsupportedOperation {
					operation: "subquery expressions must be compiled before evaluation".into(),
				})
			}
		}
	}

	/// Evaluate a variable reference.
	fn eval_var_ref(&self, name: &str, row_count: usize, ctx: &EvalContext) -> Result<Column> {
		let value = ctx.get_var(name).ok_or_else(|| VmError::UndefinedVariable {
			name: name.to_string(),
		})?;

		match value {
			EvalValue::Scalar(v) => self.broadcast_value(v, row_count),
			EvalValue::Record(_) => Err(VmError::TypeMismatch {
				expected: Type::Int8, // placeholder
				found: Type::Undefined,
				context: format!("variable '{}' is a record, not a scalar", name).into(),
			}),
		}
	}

	/// Evaluate field access on an expression.
	fn eval_field_access(
		&self,
		object: &Expr,
		field: &str,
		columns: &Columns,
		ctx: &EvalContext,
	) -> Result<Column> {
		// For now, only support VarRef as the object (e.g., $user.id)
		match object {
			Expr::VarRef(var_name) => {
				let value = ctx.get_var(var_name).ok_or_else(|| VmError::UndefinedVariable {
					name: var_name.to_string(),
				})?;

				match value {
					EvalValue::Record(record) => {
						let field_value =
							record.get(field).ok_or_else(|| VmError::FieldNotFound {
								field: field.to_string(),
								record: var_name.clone(),
							})?;
						self.broadcast_value(field_value, columns.row_count())
					}
					EvalValue::Scalar(_) => Err(VmError::TypeMismatch {
						expected: Type::Undefined, // placeholder for record type
						found: Type::Int8,
						context: format!(
							"cannot access field '{}' on scalar variable '{}'",
							field, var_name
						)
						.into(),
					}),
				}
			}
			_ => Err(VmError::UnsupportedOperation {
				operation: "field access on non-variable expression".into(),
			}),
		}
	}

	/// Broadcast a single value to a column with row_count rows.
	fn broadcast_value(&self, value: &Value, row_count: usize) -> Result<Column> {
		let data = match value {
			Value::Undefined => ColumnData::undefined(row_count),
			Value::Boolean(v) => ColumnData::bool(vec![*v; row_count]),
			Value::Int8(v) => ColumnData::int8(vec![*v; row_count]),
			Value::Float8(v) => ColumnData::float8(std::iter::repeat(f64::from(*v)).take(row_count)),
			Value::Utf8(s) => {
				ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>())
			}
			_ => {
				return Err(VmError::UnsupportedOperation {
					operation: format!("broadcast of value type {:?}", value),
				});
			}
		};

		Ok(Column::new(Fragment::internal("_var"), data))
	}

	fn eval_column_ref(&self, col_ref: &ColumnRef, columns: &Columns) -> Result<Column> {
		// First try to find by name (preferred for bytecode execution)
		if !col_ref.name.is_empty() {
			if let Some(col) = columns.iter().find(|c| c.name().text() == col_ref.name) {
				return Ok(col.clone());
			}
			return Err(VmError::ColumnNotFound {
				name: col_ref.name.clone(),
			});
		}

		// Fall back to index lookup (for pre-resolved expressions)
		if col_ref.index >= columns.len() {
			return Err(VmError::ColumnIndexOutOfBounds {
				index: col_ref.index,
				count: columns.len(),
			});
		}
		Ok(columns[col_ref.index].clone())
	}

	fn broadcast_literal(&self, lit: &Literal, row_count: usize) -> Result<Column> {
		let data = match lit {
			Literal::Null => ColumnData::undefined(row_count),
			Literal::Bool(v) => ColumnData::bool(vec![*v; row_count]),
			Literal::Int8(v) => ColumnData::int8(vec![*v; row_count]),
			Literal::Float8(v) => ColumnData::float8(vec![*v; row_count]),
			Literal::Utf8(s) => {
				ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>())
			}
		};

		Ok(Column::new(Fragment::internal("_literal"), data))
	}

	fn eval_binary(&self, op: BinaryOp, left: &Column, right: &Column) -> Result<Column> {
		let row_count = left.data().len();
		if right.data().len() != row_count {
			return Err(VmError::RowCountMismatch {
				expected: row_count,
				actual: right.data().len(),
			});
		}

		match op {
			// Comparison operators
			BinaryOp::Gt => self.eval_compare(left, right, "_gt", |a, b| a > b, |a, b| a > b),
			BinaryOp::Ge => self.eval_compare(left, right, "_ge", |a, b| a >= b, |a, b| a >= b),
			BinaryOp::Lt => self.eval_compare(left, right, "_lt", |a, b| a < b, |a, b| a < b),
			BinaryOp::Le => self.eval_compare(left, right, "_le", |a, b| a <= b, |a, b| a <= b),
			BinaryOp::Eq => self.eval_equality(left, right, "_eq", false),
			BinaryOp::Ne => self.eval_equality(left, right, "_ne", true),

			// Logical operators
			BinaryOp::And => self.eval_logical_and(left, right),
			BinaryOp::Or => self.eval_logical_or(left, right),

			// Arithmetic operators
			BinaryOp::Add => self.eval_arithmetic(left, right, "_add", |a, b| a + b, |a, b| a + b),
			BinaryOp::Sub => self.eval_arithmetic(left, right, "_sub", |a, b| a - b, |a, b| a - b),
			BinaryOp::Mul => self.eval_arithmetic(left, right, "_mul", |a, b| a * b, |a, b| a * b),
			BinaryOp::Div => self.eval_arithmetic_div(left, right),
		}
	}

	fn eval_compare<FI, FF>(
		&self,
		left: &Column,
		right: &Column,
		name: &str,
		cmp_int: FI,
		cmp_float: FF,
	) -> Result<Column>
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

	fn eval_equality(&self, left: &Column, right: &Column, name: &str, negate: bool) -> Result<Column> {
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

	fn eval_logical_and(&self, left: &Column, right: &Column) -> Result<Column> {
		let row_count = left.data().len();

		match (left.data(), right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				// SQL three-valued logic:
				// FALSE AND x = FALSE
				// TRUE AND TRUE = TRUE
				// TRUE AND NULL = NULL
				// NULL AND NULL = NULL
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
							// NULL involved
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

	fn eval_logical_or(&self, left: &Column, right: &Column) -> Result<Column> {
		let row_count = left.data().len();

		match (left.data(), right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				// SQL three-valued logic:
				// TRUE OR x = TRUE
				// FALSE OR FALSE = FALSE
				// FALSE OR NULL = NULL
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

	fn eval_arithmetic<FI, FF>(
		&self,
		left: &Column,
		right: &Column,
		name: &str,
		op_int: FI,
		op_float: FF,
	) -> Result<Column>
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

	fn eval_arithmetic_div(&self, left: &Column, right: &Column) -> Result<Column> {
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
							// Division by zero or null
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

	fn eval_unary(&self, op: UnaryOp, col: &Column) -> Result<Column> {
		match op {
			UnaryOp::Not => self.eval_unary_not(col),
			UnaryOp::Neg => self.eval_unary_neg(col),
			UnaryOp::IsNull => self.eval_is_null(col, false),
			UnaryOp::IsNotNull => self.eval_is_null(col, true),
		}
	}

	fn eval_unary_not(&self, col: &Column) -> Result<Column> {
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

	fn eval_unary_neg(&self, col: &Column) -> Result<Column> {
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

	fn eval_is_null(&self, col: &Column, negated: bool) -> Result<Column> {
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
}
