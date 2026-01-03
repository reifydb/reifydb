// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Statement and pipeline compilation orchestration.

use bumpalo::collections::Vec as BumpVec;
use reifydb_transaction::IntoStandardTransaction;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::{
		Expr, Pipeline, Program, Statement,
		expr::{BinaryExpr, BinaryOp, FromExpr, SubQueryExpr},
	},
	plan::{
		OutputSchema, Plan,
		node::{
			control::{BreakNode, CallScriptFunctionNode, ContinueNode, ExprNode, ReturnNode},
			query::{ScanNode, VariableSourceNode},
		},
	},
};

impl<'bump, 'cat, T: IntoStandardTransaction> Planner<'bump, 'cat, T> {
	/// Compile a program to plans.
	pub(super) async fn compile_program(&mut self, program: Program<'bump>) -> Result<&'bump [Plan<'bump>]> {
		let mut plans = BumpVec::new_in(self.bump);
		for stmt in program.statements {
			let plan = self.compile_statement(stmt).await?;
			plans.push(plan);
		}
		Ok(plans.into_bump_slice())
	}

	/// Compile a statement to a plan.
	pub(super) async fn compile_statement(&mut self, stmt: &Statement<'bump>) -> Result<Plan<'bump>> {
		match stmt {
			Statement::Pipeline(pipeline) => self.compile_pipeline(pipeline).await,
			Statement::Expression(expr_stmt) => {
				// Handle based on expression type:
				// - Pipeline operators go through compile_pipeline_stage
				// - Other expressions compile directly to Plan::Expr
				match expr_stmt.expr {
					// Pipeline-producing expressions
					Expr::From(_)
					| Expr::Filter(_)
					| Expr::Map(_)
					| Expr::Extend(_)
					| Expr::Aggregate(_)
					| Expr::Sort(_)
					| Expr::Take(_)
					| Expr::Distinct(_)
					| Expr::Join(_)
					| Expr::Merge(_)
					| Expr::Window(_)
					| Expr::SubQuery(_) => self.compile_pipeline_stage(expr_stmt.expr, None).await,

					// Binary(As) is used for aliasing (e.g., "from table as t")
					Expr::Binary(bin) if bin.op == BinaryOp::As => {
						self.compile_pipeline_stage(expr_stmt.expr, None).await
					}

					// Function calls need special handling for script functions
					Expr::Call(call) => {
						if let Expr::Identifier(ident) = call.function {
							if self.script_functions.iter().any(|&name| name == ident.name)
							{
								return Ok(Plan::CallScriptFunction(
									CallScriptFunctionNode {
										name: self.bump.alloc_str(ident.name),
										span: call.span,
									},
								));
							}
						}
						// Builtin functions compile as expressions
						let plan_expr = self.compile_expr(expr_stmt.expr, None)?;
						Ok(Plan::Expr(ExprNode {
							expr: self.bump.alloc(plan_expr),
							span: expr_stmt.span,
						}))
					}

					// All other expressions compile directly
					_ => {
						let plan_expr = self.compile_expr(expr_stmt.expr, None)?;
						Ok(Plan::Expr(ExprNode {
							expr: self.bump.alloc(plan_expr),
							span: expr_stmt.span,
						}))
					}
				}
			}
			Statement::Let(let_stmt) => self.compile_let(let_stmt).await,
			Statement::Assign(assign_stmt) => self.compile_assign(assign_stmt).await,
			Statement::If(if_stmt) => self.compile_if(if_stmt).await,
			Statement::Loop(loop_stmt) => self.compile_loop(loop_stmt).await,
			Statement::For(for_stmt) => self.compile_for(for_stmt).await,
			Statement::Break(break_stmt) => Ok(Plan::Break(BreakNode {
				span: break_stmt.span,
			})),
			Statement::Continue(continue_stmt) => Ok(Plan::Continue(ContinueNode {
				span: continue_stmt.span,
			})),
			Statement::Return(return_stmt) => self.compile_return(return_stmt).await,
			Statement::Create(create_stmt) => self.compile_create(create_stmt).await,
			Statement::Insert(insert_stmt) => self.compile_insert(insert_stmt).await,
			Statement::Update(update_stmt) => self.compile_update(update_stmt).await,
			Statement::Delete(delete_stmt) => self.compile_delete(delete_stmt).await,
			Statement::Drop(drop_stmt) => self.compile_drop(drop_stmt).await,
			Statement::Alter(alter_stmt) => self.compile_alter(alter_stmt).await,
			Statement::Def(def_stmt) => self.compile_def(def_stmt).await,
			_ => Err(PlanError {
				kind: PlanErrorKind::Unsupported(format!("statement type: {:?}", stmt)),
				span: stmt.span(),
			}),
		}
	}

	/// Compile a pipeline to a plan.
	pub(super) async fn compile_pipeline(&mut self, pipeline: &Pipeline<'bump>) -> Result<Plan<'bump>> {
		if pipeline.stages.is_empty() {
			return Err(PlanError {
				kind: PlanErrorKind::EmptyPipeline,
				span: pipeline.span,
			});
		}

		let mut current: Option<&'bump Plan<'bump>> = None;
		let mut schema: Option<OutputSchema<'bump>> = None;

		for stage in pipeline.stages {
			let plan = self.compile_pipeline_stage_with_schema(stage, current, schema.as_ref()).await?;
			// Build schema from the compiled plan for next stage
			schema = Some(self.build_schema_from_plan(&plan));
			current = Some(self.bump.alloc(plan));
		}

		// unwrap is safe because we checked for empty pipeline above
		Ok(*current.unwrap())
	}

	/// Compile a pipeline stage to a plan.
	pub(super) async fn compile_pipeline_stage(
		&mut self,
		expr: &Expr<'bump>,
		input: Option<&'bump Plan<'bump>>,
	) -> Result<Plan<'bump>> {
		match expr {
			Expr::From(from) => self.compile_from(from).await,
			Expr::Binary(bin) if bin.op == BinaryOp::As => self.compile_as_alias(bin, input).await,
			Expr::Filter(filter) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("filter"),
					span: filter.span,
				})?;
				self.compile_filter(filter, input).await
			}
			Expr::Map(map) => self.compile_map(map, input).await,
			Expr::Extend(extend) => self.compile_extend(extend, input).await,
			Expr::Aggregate(agg) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("aggregate"),
					span: agg.span,
				})?;
				self.compile_aggregate(agg, input).await
			}
			Expr::Sort(sort) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("sort"),
					span: sort.span,
				})?;
				self.compile_sort(sort, input).await
			}
			Expr::Take(take) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("take"),
					span: take.span,
				})?;
				self.compile_take(take, input).await
			}
			Expr::Distinct(distinct) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("distinct"),
					span: distinct.span,
				})?;
				self.compile_distinct(distinct, input).await
			}
			Expr::Join(join) => {
				let left = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("join"),
					span: join.span(),
				})?;
				self.compile_join(join, left).await
			}
			Expr::Merge(merge) => {
				let left = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("merge"),
					span: merge.span,
				})?;
				self.compile_merge(merge, left).await
			}
			Expr::Window(window) => self.compile_window(window, input).await,
			// Handle subqueries as pipeline stages
			Expr::SubQuery(subquery) => self.compile_subquery(subquery, input).await,
			// Handle literals as return statements (for if body)
			Expr::Literal(lit) => {
				let expr = self.bump.alloc(self.compile_literal(lit));
				Ok(Plan::Return(ReturnNode {
					value: Some(expr),
					span: lit.span(),
				}))
			}
			// Handle variables as pipeline sources
			Expr::Variable(var) => {
				let resolved = self.resolve_variable(var.name, var.span)?;
				Ok(Plan::VariableSource(VariableSourceNode {
					variable: resolved,
					span: var.span,
				}))
			}
			// Handle function calls
			Expr::Call(call) => {
				// Get the function name
				if let Expr::Identifier(ident) = call.function {
					// Check if this is a script function
					if self.script_functions.iter().any(|&name| name == ident.name) {
						return Ok(Plan::CallScriptFunction(CallScriptFunctionNode {
							name: self.bump.alloc_str(ident.name),
							span: call.span,
						}));
					}
				}
				// For other calls (like builtin functions), compile as expression
				let plan_expr = self.compile_expr(expr, None)?;
				Ok(Plan::Expr(ExprNode {
					expr: self.bump.alloc(plan_expr),
					span: expr.span(),
				}))
			}
			_ => Err(PlanError {
				kind: PlanErrorKind::Unsupported(format!("pipeline stage: {:?}", expr)),
				span: expr.span(),
			}),
		}
	}

	/// Compile a pipeline stage to a plan with schema context for column resolution.
	pub(super) async fn compile_pipeline_stage_with_schema(
		&mut self,
		expr: &Expr<'bump>,
		input: Option<&'bump Plan<'bump>>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		match expr {
			Expr::From(from) => self.compile_from(from).await,
			Expr::Binary(bin) if bin.op == BinaryOp::As => self.compile_as_alias(bin, input).await,
			Expr::Filter(filter) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("filter"),
					span: filter.span,
				})?;
				self.compile_filter_with_schema(filter, input, schema).await
			}
			Expr::Map(map) => self.compile_map_with_schema(map, input, schema).await,
			Expr::Extend(extend) => self.compile_extend_with_schema(extend, input, schema).await,
			Expr::Aggregate(agg) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("aggregate"),
					span: agg.span,
				})?;
				self.compile_aggregate_with_schema(agg, input, schema).await
			}
			Expr::Sort(sort) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("sort"),
					span: sort.span,
				})?;
				self.compile_sort_with_schema(sort, input, schema).await
			}
			Expr::Take(take) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("take"),
					span: take.span,
				})?;
				self.compile_take(take, input).await
			}
			Expr::Distinct(distinct) => {
				let input = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("distinct"),
					span: distinct.span,
				})?;
				self.compile_distinct(distinct, input).await
			}
			Expr::Join(join) => {
				let left = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("join"),
					span: join.span(),
				})?;
				self.compile_join(join, left).await
			}
			Expr::Merge(merge) => {
				let left = input.ok_or_else(|| PlanError {
					kind: PlanErrorKind::MissingInput("merge"),
					span: merge.span,
				})?;
				self.compile_merge(merge, left).await
			}
			Expr::Window(window) => self.compile_window(window, input).await,
			// Handle subqueries as pipeline stages
			Expr::SubQuery(subquery) => self.compile_subquery(subquery, input).await,
			// Handle literals as return statements (for if body)
			Expr::Literal(lit) => {
				let expr = self.bump.alloc(self.compile_literal(lit));
				Ok(Plan::Return(ReturnNode {
					value: Some(expr),
					span: lit.span(),
				}))
			}
			// Handle variables as pipeline sources
			Expr::Variable(var) => {
				let resolved = self.resolve_variable(var.name, var.span)?;
				Ok(Plan::VariableSource(VariableSourceNode {
					variable: resolved,
					span: var.span,
				}))
			}
			// Handle function calls
			Expr::Call(call) => {
				// Get the function name
				if let Expr::Identifier(ident) = call.function {
					// Check if this is a script function
					if self.script_functions.iter().any(|&name| name == ident.name) {
						return Ok(Plan::CallScriptFunction(CallScriptFunctionNode {
							name: self.bump.alloc_str(ident.name),
							span: call.span,
						}));
					}
				}
				// For other calls (like builtin functions), compile as expression
				let plan_expr = self.compile_expr(expr, schema)?;
				Ok(Plan::Expr(ExprNode {
					expr: self.bump.alloc(plan_expr),
					span: expr.span(),
				}))
			}
			_ => Err(PlanError {
				kind: PlanErrorKind::Unsupported(format!("pipeline stage: {:?}", expr)),
				span: expr.span(),
			}),
		}
	}

	/// Compile a Binary(As) expression to handle aliases on FROM/other expressions.
	fn compile_as_alias<'a>(
		&'a mut self,
		bin: &'a BinaryExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Plan<'bump>>> + 'a>>
	where
		'bump: 'a,
	{
		Box::pin(async move {
			let alias = match bin.right {
				Expr::Identifier(ident) => Some(self.bump.alloc_str(ident.name) as &'bump str),
				_ => None,
			};

			match bin.left {
				Expr::From(FromExpr::Source(source)) => {
					let primitive = self
						.resolve_primitive(source.namespace, source.name, source.span)
						.await?;
					Ok(Plan::Scan(ScanNode {
						primitive,
						alias,
						span: bin.span,
					}))
				}
				// For other expressions with AS, just compile the left side
				_ => self.compile_pipeline_stage(bin.left, input).await,
			}
		})
	}

	/// Compile a subquery expression as a pipeline.
	pub(crate) fn compile_subquery<'a>(
		&'a mut self,
		subquery: &'a SubQueryExpr<'bump>,
		_input: Option<&'bump Plan<'bump>>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Plan<'bump>>> + 'a>>
	where
		'bump: 'a,
	{
		Box::pin(async move {
			if subquery.pipeline.is_empty() {
				return Err(PlanError {
					kind: PlanErrorKind::EmptyPipeline,
					span: subquery.span,
				});
			}

			let mut current: Option<&'bump Plan<'bump>> = None;
			for stage in subquery.pipeline {
				let plan = self.compile_pipeline_stage(stage, current).await?;
				current = Some(self.bump.alloc(plan));
			}

			Ok(*current.unwrap())
		})
	}
	/// Compile a sequence of pipeline stages to a plan slice.
	pub(super) async fn compile_statement_body_as_pipeline(
		&mut self,
		stages: &[Expr<'bump>],
	) -> Result<&'bump [&'bump Plan<'bump>]> {
		if stages.is_empty() {
			return Ok(&[]);
		}

		let mut current: Option<&'bump Plan<'bump>> = None;
		for stage in stages {
			let plan = self.compile_pipeline_stage(stage, current).await?;
			current = Some(self.bump.alloc(plan));
		}

		if let Some(plan) = current {
			Ok(self.bump.alloc_slice_copy(&[plan]))
		} else {
			Ok(&[])
		}
	}
	/// Compile a statement body to a plan slice.
	pub(super) fn compile_statement_body<'a>(
		&'a mut self,
		stmts: &'a [Statement<'bump>],
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<&'bump [&'bump Plan<'bump>]>> + 'a>>
	where
		'bump: 'a,
	{
		Box::pin(async move {
			let mut plans = BumpVec::with_capacity_in(stmts.len(), self.bump);
			for stmt in stmts {
				let plan = self.compile_statement(stmt).await?;
				plans.push(self.bump.alloc(plan) as &'bump Plan<'bump>);
			}
			Ok(plans.into_bump_slice())
		})
	}
}
