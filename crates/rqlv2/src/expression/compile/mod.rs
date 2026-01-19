// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of PlanExpr to CompiledExpr async closures.
//!
//! This module converts PlanExpr (a resolved AST) into nested closures that
//! return futures and can be executed directly without enum dispatch.
//! The async design supports subquery execution within expressions.
//!
//! Reference: https://blog.cloudflare.com/building-fast-interpreters-in-rust/

pub mod evaluation;
pub mod helpers;
pub mod literal;
pub mod operator;
pub mod reference;

use helpers::column_to_mask;
use literal::{
	compile_literal_bool, compile_literal_bytes, compile_literal_float, compile_literal_int,
	compile_literal_string, compile_literal_undefined,
};
use operator::{
	compile_aggregate_call, compile_between, compile_binary, compile_cast, compile_conditional,
	compile_in, compile_list, compile_record, compile_scalar_call, compile_tuple, compile_unary,
};
use reference::{compile_column_ref, compile_field_access, compile_rownum, compile_variable_ref, compile_wildcard};
use reifydb_core::value::column::{Column, columns::Columns};
use reifydb_type::fragment::Fragment;

use crate::{
	expression::types::{CompiledExpr, CompiledFilter, EvalError},
	plan::node::expr::PlanExpr,
};

/// Compile a PlanExpr into a CompiledExpr closure.
///
/// The resulting closure captures all static information (column names,
/// literals, operators) and only needs columns and context at evaluation time.
pub fn compile_plan_expr<'bump>(expr: &PlanExpr<'bump>) -> CompiledExpr {
	match expr {
		PlanExpr::LiteralUndefined(_) => compile_literal_undefined(),
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
		} => compile_scalar_call(function.name.to_string(), arguments),
		PlanExpr::Aggregate {
			function,
			arguments,
			distinct,
			..
		} => compile_aggregate_call(function.name.to_string(), arguments, *distinct),
		PlanExpr::Conditional {
			condition,
			then_expr,
			else_expr,
			..
		} => compile_conditional(condition, then_expr, else_expr),
		PlanExpr::Subquery(_plan) => {
			// Subqueries require executor support
			CompiledExpr::new(|_, _| {
				Err(EvalError::UnsupportedOperation {
					operation: "subquery".to_string(),
				})
			})
		}
		PlanExpr::Exists {
			..
		} => {
			// EXISTS subqueries require executor support
			CompiledExpr::new(|_, _| {
				Err(EvalError::UnsupportedOperation {
					operation: "EXISTS subquery".to_string(),
				})
			})
		}
		PlanExpr::InSubquery {
			..
		} => {
			// IN subqueries require executor support
			CompiledExpr::new(|_, _| {
				Err(EvalError::UnsupportedOperation {
					operation: "IN subquery".to_string(),
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
		PlanExpr::FieldAccess {
			base,
			field,
			..
		} => compile_field_access(compile_plan_expr(base), field.to_string()),
		PlanExpr::CallScriptFunction {
			name,
			arguments,
			..
		} => {
			// Compile arguments
			let name = name.to_string();
			let compiled_args: Vec<CompiledExpr> = arguments.iter().map(|a| compile_plan_expr(a)).collect();

			CompiledExpr::new(move |columns, ctx| {
				// Evaluate arguments to columns
				let mut arg_cols = Vec::with_capacity(compiled_args.len());
				for arg in &compiled_args {
					arg_cols.push(arg.eval(columns, ctx)?);
				}
				let args = Columns::new(arg_cols);

				// Call script function through trait
				let result = ctx.call_script_function(&name, &args, columns.row_count())?;

				Ok(Column::new(Fragment::internal("_call"), result))
			})
		}
	}
}

/// Compile a PlanExpr into a CompiledFilter that returns BitVec directly.
///
/// This is more efficient for filter predicates as it avoids creating
/// an intermediate Column for the boolean result.
pub fn compile_plan_filter<'bump>(expr: &PlanExpr<'bump>) -> CompiledFilter {
	let compiled = compile_plan_expr(expr);
	CompiledFilter::new(move |columns, ctx| {
		let column = compiled.eval(columns, ctx)?;
		column_to_mask(&column)
	})
}

#[cfg(test)]
pub mod tests {
	use reifydb::vendor::tokio;
	use reifydb_core::value::column::{Column, data::ColumnData};
	use reifydb_type::value::Value;

	use super::{evaluation::eval_binary, *};
	use crate::{
		expression::eval::{context::EvalContext, value::EvalValue},
		plan::node::expr::BinaryPlanOp,
	};

	#[tokio::test]
	async fn test_compile_literal_int() {
		let expr = compile_literal_int(42);
		let columns = Columns::new(vec![Column::new(Fragment::from("x"), ColumnData::int8(vec![1, 2, 3]))]);

		let result = expr.eval(&columns, &EvalContext::new()).unwrap();
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

		let result = expr.eval(&columns, &EvalContext::new()).unwrap();
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
			let left = age_col.eval(columns, ctx)?;
			let right = threshold.eval(columns, ctx)?;
			eval_binary(BinaryPlanOp::Gt, &left, &right)
		});

		let columns =
			Columns::new(vec![Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 30, 35]))]);

		let result = gt_expr.eval(&columns, &EvalContext::new()).unwrap();
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

		let result = expr.eval(&columns, &ctx).unwrap();
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
