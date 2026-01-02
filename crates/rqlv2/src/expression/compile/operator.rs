// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator compilation (binary, unary, between, in, cast, call, etc.).

use std::collections::HashMap;

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_type::Fragment;

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

pub(super) fn compile_unary<'bump>(op: UnaryPlanOp, operand: &PlanExpr<'bump>) -> CompiledExpr {
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

pub(super) fn compile_in<'bump>(expr: &PlanExpr<'bump>, list: &[&PlanExpr<'bump>], negated: bool) -> CompiledExpr {
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

pub(super) fn compile_cast<'bump>(expr: &PlanExpr<'bump>, _target_type: crate::plan::Type) -> CompiledExpr {
	// TODO: Implement proper type casting
	compile_plan_expr(expr)
}

pub(super) fn compile_call<'bump>(function_name: String, arguments: &[&PlanExpr<'bump>]) -> CompiledExpr {
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

pub(super) fn compile_aggregate<'bump>(
	_function_name: String,
	_arguments: &[&PlanExpr<'bump>],
	_distinct: bool,
) -> CompiledExpr {
	// Aggregates are handled by the Apply(Aggregate) plan node
	CompiledExpr::new(|_, _| {
		Box::pin(async {
			Err(EvalError::UnsupportedOperation {
				operation: "aggregate in expression context".to_string(),
			})
		})
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

pub(super) fn compile_list<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
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

pub(super) fn compile_tuple<'bump>(items: &[&PlanExpr<'bump>]) -> CompiledExpr {
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

pub(super) fn compile_record<'bump>(fields: &[(&str, &PlanExpr<'bump>)]) -> CompiledExpr {
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
