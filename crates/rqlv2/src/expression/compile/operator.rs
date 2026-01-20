// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator compilation (binary, unary, between, in, cast, call, etc.).

use std::collections::HashMap;

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use super::{
	compile_plan_expr,
	evaluation::{eval_binary, eval_conditional, eval_unary},
};
use crate::{
	expression::types::{CompiledExpr, EvalError},
	plan::node::expr::{BinaryPlanOp, PlanExpr, UnaryPlanOp},
};

pub(super) fn compile_binary<'bump>(op: BinaryPlanOp, left: &PlanExpr<'bump>, right: &PlanExpr<'bump>) -> CompiledExpr {
	let left_fn = compile_plan_expr(left);
	let right_fn = compile_plan_expr(right);

	CompiledExpr::new(move |columns, ctx| {
		let left_col = left_fn.eval(columns, ctx)?;
		let right_col = right_fn.eval(columns, ctx)?;
		eval_binary(op, &left_col, &right_col)
	})
}

pub(super) fn compile_unary<'bump>(op: UnaryPlanOp, operand: &PlanExpr<'bump>) -> CompiledExpr {
	let operand_fn = compile_plan_expr(operand);

	CompiledExpr::new(move |columns, ctx| {
		let col = operand_fn.eval(columns, ctx)?;
		eval_unary(op, &col)
	})
}

pub(super) fn compile_between<'bump>(
	expr: &PlanExpr<'bump>,
	low: &PlanExpr<'bump>,
	high: &PlanExpr<'bump>,
	negated: bool,
) -> CompiledExpr {
	let expr_fn = compile_plan_expr(expr);
	let low_fn = compile_plan_expr(low);
	let high_fn = compile_plan_expr(high);

	CompiledExpr::new(move |columns, ctx| {
		let expr_col = expr_fn.eval(columns, ctx)?;
		let low_col = low_fn.eval(columns, ctx)?;
		let high_col = high_fn.eval(columns, ctx)?;

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
}

pub(super) fn compile_in<'bump>(expr: &PlanExpr<'bump>, list: &[&PlanExpr<'bump>], negated: bool) -> CompiledExpr {
	let expr_fn = compile_plan_expr(expr);
	let list_fns: Vec<_> = list.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		if list_fns.is_empty() {
			// Empty list: result is false (or true if negated)
			return Ok(Column::new(
				Fragment::internal("_in"),
				ColumnData::bool(vec![negated; columns.row_count()]),
			));
		}

		let expr_col = expr_fn.eval(columns, ctx)?;

		// Build result: expr = v1 OR expr = v2 OR ...
		let first = list_fns[0].eval(columns, ctx)?;
		let mut result = eval_binary(BinaryPlanOp::Eq, &expr_col, &first)?;

		for item_fn in &list_fns[1..] {
			let item_col = item_fn.eval(columns, ctx)?;
			let eq_result = eval_binary(BinaryPlanOp::Eq, &expr_col, &item_col)?;
			result = eval_binary(BinaryPlanOp::Or, &result, &eq_result)?;
		}

		if negated {
			eval_unary(UnaryPlanOp::Not, &result)
		} else {
			Ok(result)
		}
	})
}

pub(super) fn compile_cast<'bump>(expr: &PlanExpr<'bump>, target_type: Type) -> CompiledExpr {
	use super::evaluation::cast_column_data;

	let expr_fn = compile_plan_expr(expr);

	CompiledExpr::new(move |columns, ctx| {
		let col = expr_fn.eval(columns, ctx)?;
		let casted_data = cast_column_data(&col.data(), target_type)?;

		Ok(Column::new(col.name.clone(), casted_data))
	})
}

pub fn compile_scalar_call<'bump>(function_name: String, arguments: &[&PlanExpr<'bump>]) -> CompiledExpr {
	use reifydb_core::value::column::columns::Columns;
	use reifydb_function::ScalarFunctionContext;

	let arg_fns: Vec<_> = arguments.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate all arguments to columns
		let mut arg_cols = Vec::with_capacity(arg_fns.len());
		for arg_fn in &arg_fns {
			arg_cols.push(arg_fn.eval(columns, ctx)?);
		}
		let args = Columns::new(arg_cols);

		// Get function registry from context
		let functions = ctx.functions.as_ref().ok_or_else(|| EvalError::UnsupportedOperation {
			operation: format!("scalar function '{}' (no registry available)", function_name),
		})?;

		// Look up scalar function
		let scalar_func =
			functions.get_scalar(&function_name).ok_or_else(|| EvalError::UnsupportedOperation {
				operation: format!("unknown scalar function '{}'", function_name),
			})?;

		// Call scalar function
		let result_data = scalar_func
			.scalar(ScalarFunctionContext {
				columns: &args,
				row_count: columns.row_count(),
			})
			.map_err(|e| EvalError::SubqueryError {
				message: format!("Scalar function '{}' error: {}", function_name, e),
			})?;

		Ok(Column::new(Fragment::internal(&function_name), result_data))
	})
}

pub fn compile_aggregate_call<'bump>(
	function_name: String,
	arguments: &[&PlanExpr<'bump>],
	_distinct: bool, // TODO: handle distinct in future
) -> CompiledExpr {
	use reifydb_core::value::column::view::group_by::GroupByView;
	use reifydb_function::AggregateFunctionContext;

	let arg_fns: Vec<_> = arguments.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate arguments
		let mut arg_cols = Vec::with_capacity(arg_fns.len());
		for arg_fn in &arg_fns {
			arg_cols.push(arg_fn.eval(columns, ctx)?);
		}

		// Get function registry
		let functions = ctx.functions.as_ref().ok_or_else(|| EvalError::UnsupportedOperation {
			operation: format!("aggregate function '{}' (no registry available)", function_name),
		})?;

		// Look up aggregate function
		let mut agg_func =
			functions.get_aggregate(&function_name).ok_or_else(|| EvalError::UnsupportedOperation {
				operation: format!("unknown aggregate function '{}'", function_name),
			})?;

		// Create single-group view for expression context
		// (In plan operators like GroupBy, this would be multiple groups)
		let mut group_view = GroupByView::new();
		let all_indices: Vec<usize> = (0..columns.row_count()).collect();
		group_view.insert(vec![], all_indices);

		// Get first argument or create dummy for count()
		let column = if arg_cols.is_empty() {
			Column::new(Fragment::internal("_count"), ColumnData::undefined(columns.row_count()))
		} else {
			arg_cols[0].clone()
		};

		// Execute aggregate
		agg_func.aggregate(AggregateFunctionContext {
			column: &column,
			groups: &group_view,
		})
		.map_err(|e| EvalError::SubqueryError {
			message: format!("Aggregate '{}' error: {}", function_name, e),
		})?;

		let (_keys, result_data) = agg_func.finalize().map_err(|e| EvalError::SubqueryError {
			message: format!("Aggregate '{}' finalize error: {}", function_name, e),
		})?;

		Ok(Column::new(Fragment::internal(&function_name), result_data))
	})
}

pub(super) fn compile_conditional<'bump>(
	condition: &PlanExpr<'bump>,
	then_expr: &PlanExpr<'bump>,
	else_expr: &PlanExpr<'bump>,
) -> CompiledExpr {
	let cond_fn = compile_plan_expr(condition);
	let then_fn = compile_plan_expr(then_expr);
	let else_fn = compile_plan_expr(else_expr);

	CompiledExpr::new(move |columns, ctx| {
		let cond_col = cond_fn.eval(columns, ctx)?;
		let then_col = then_fn.eval(columns, ctx)?;
		let else_col = else_fn.eval(columns, ctx)?;

		eval_conditional(&cond_col, &then_col, &else_col)
	})
}

pub(super) fn compile_list<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
	let item_fns: Vec<_> = items.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate all items
		let mut _items = Vec::with_capacity(item_fns.len());
		for item_fn in &item_fns {
			_items.push(item_fn.eval(columns, ctx)?);
		}
		// TODO: Build list value
		Err(EvalError::UnsupportedOperation {
			operation: "list expressions".to_string(),
		})
	})
}

pub(super) fn compile_tuple<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
	let item_fns: Vec<_> = items.iter().map(|e| compile_plan_expr(e)).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate all items
		let mut _items = Vec::with_capacity(item_fns.len());
		for item_fn in &item_fns {
			_items.push(item_fn.eval(columns, ctx)?);
		}
		// TODO: Build tuple value
		Err(EvalError::UnsupportedOperation {
			operation: "tuple expressions".to_string(),
		})
	})
}

pub(super) fn compile_record<'bump>(fields: &[(&str, &PlanExpr<'bump>)]) -> CompiledExpr {
	let field_fns: Vec<_> = fields.iter().map(|(name, expr)| (name.to_string(), compile_plan_expr(expr))).collect();

	CompiledExpr::new(move |columns, ctx| {
		// Evaluate all fields
		let mut _fields = HashMap::new();
		for (name, expr_fn) in &field_fns {
			let col = expr_fn.eval(columns, ctx)?;
			_fields.insert(name.clone(), col);
		}
		// TODO: Build record value
		Err(EvalError::UnsupportedOperation {
			operation: "record expressions".to_string(),
		})
	})
}
